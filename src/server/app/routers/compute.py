from pydantic import BaseModel, Field
from typing import Optional, List, Dict
import rq
import re
import base64
import time
import uuid
import json
from fastapi import APIRouter, Request, HTTPException, status
from nacl.signing import VerifyKey
from pydantic import BaseModel, Field

from .. import helpers
from .. import auth
from .. import maintenance

app = APIRouter()


def verify_challenge(encrypted_challenge):
    try:
        decrypted = auth.encryption_key.decrypt(
            base64.b64decode(encrypted_challenge)
        ).decode("ascii")
    except:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid compute key for challenge",
        )
    if time.time() - float(decrypted) > 60:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid challenge"
        )


class FunctionRegister(BaseModel):
    function: str = Field(..., description="Base64-encoded and encrypted function")
    major: int = Field(..., description="Major version number")
    minor: int = Field(..., description="Minor version number")
    digest: str = Field(..., description="SHA256 digest of function")
    signature: str = Field(..., description="Base64 Signature of the function digest")
    signing_key: str = Field(..., description="Public key of the signing key")
    packages: List[str] = Field(
        ..., description="List of encrypted packages to install"
    )
    packages_hashes: List[str] = Field(
        ..., description="Hashes of the packages to install in same order as packages."
    )
    challenge: str = Field(..., description="Encrypted challenge from /auth/challenge")


@app.post(
    "/function/register",
    tags=["compute"],
)
def task_register(body: FunctionRegister):
    verify_challenge(body.challenge)

    # verify signature
    owner = auth.db.users.find_one({"sign": body.signing_key})
    if not owner:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Unknown signing key"
        )
    try:
        VerifyKey(base64.b64decode(owner["sign"])).verify(
            body.digest.encode("ascii"), base64.b64decode(body.signature)
        )
    except:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid signature"
        )

    # store function
    if not auth.db.functions.find_one({"digest": body.digest}):
        auth.db.functions.insert_one(
            {
                "digest": body.digest,
                "function": body.function,
                "signature": body.signature,
                "signing_key": body.signing_key,
                "major": body.major,
                "minor": body.minor,
                "packages": dict(zip(body.packages_hashes, body.packages)),
            }
        )
        auth.db.logs.insert_one(
            {"event": "function/register", "id": body.digest, "ts": time.time()}
        )
    return {"status": "ok"}


@app.get(
    "/function/fetch/{digest}",
    tags=["compute"],
)
def function_fetch(digest: str):
    code = auth.db.functions.find_one({"digest": digest})
    if code:
        authorization = auth.db.users.find_one({"sign": code["signing_key"]})
        return {
            "function": code["function"],
            "packages": code["packages"],
            "signature": code["signature"],
            "signing_key": code["signing_key"],
            "authorization": authorization["signature"],
        }
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Invalid digest")


class TaskSubmit(BaseModel):
    tag: str = Field(..., description="Tag for the task")
    function: str = Field(..., description="SHA256 digest of function")
    calls: str = Field(..., description="JSON-encoded list of calls")
    ncores: int = Field(..., description="Number of cores to use")
    datacenters: List[str] = Field(..., description="Acceptable datacenters")
    challenge: str = Field(..., description="Encrypted challenge from /auth/challenge")
    digest: str = Field(..., description="SHA256 digest of calls")


@app.post("/tasks/submit", tags=["compute"])
def tasks_submit(body: TaskSubmit):
    verify_challenge(body.challenge)
    # todo: verify digest

    calls = json.loads(body.calls)
    uuids = []
    logentries = []
    taskentries = []

    function = auth.db.functions.find_one({"digest": body.function})
    queues = [
        f'py-{function["major"]}.{function["minor"]},nc-{body.ncores},dc-{_}'
        for _ in body.datacenters
    ]
    if queues == []:
        queues = [f'py-{function["major"]}.{function["minor"]},nc-{body.ncores},dc-any']

    for queue in queues:
        auth.db.active_queues.update_one(
            {"queue": queue}, {"$inc": {"submits": 1}}, upsert=True
        )

    for call in calls:
        taskid = str(uuid.uuid4())
        logentries.append({"event": "task/submit", "id": taskid, "ts": time.time()})
        taskentries.append(
            {
                "id": taskid,
                "call": call,
                "tag": body.tag,
                "function": body.function,
                "ncores": body.ncores,
                "datacenters": body.datacenters,
                "queues": queues,
                "status": "pending",
            }
        )
        uuids.append(taskid)

    with auth.client.start_session() as session:
        with session.start_transaction():
            auth.db.logs.insert_many(logentries)
            auth.db.tasks.insert_many(taskentries)

    return uuids


class TasksDelete(BaseModel):
    challenge: str = Field(..., description="Encrypted challenge from /auth/challenge")
    delete: List[str] = Field(..., description="List of task ids to cancel and delete.")


@app.post("/tasks/delete", tags=["compute"])
def tasks_delete(body: TasksDelete):
    verify_challenge(body.challenge)

    # mongodb
    logentries = []
    existing = auth.db.tasks.find({"id": {"$in": body.delete}})
    for task in existing:
        logentries.append({"event": "task/delete", "id": task["id"], "ts": time.time()})
    existing_ids = [_["id"] for _ in existing]
    auth.db.tasks.delete_many({"id": {"$in": existing_ids}})
    if len(logentries) > 0:
        auth.db.logs.insert_many(logentries)

    # rq
    for rqid in maintenance.redis_conn.hmget("id2id", existing_ids):
        if rqid is None:
            continue

        job = rq.job.Job.fetch(rqid, connection=maintenance.redis_conn)
        job.cancel()
        job.delete()
    maintenance.redis_conn.hdel("id2id", existing_ids)

    return existing_ids


class TasksInspect(BaseModel):
    challenge: str = Field(..., description="Encrypted challenge from /auth/challenge")
    tasks: List[str] = Field(
        ..., description="List of task ids of which to query the status."
    )


@app.post("/tasks/inspect", tags=["compute"])
def task_inspect(body: TasksInspect):
    verify_challenge(body.challenge)

    result = {_: None for _ in body.tasks}
    for task in auth.db.tasks.find({"id": {"$in": body.tasks}}):
        result[task["id"]] = task["status"]

    return result


class TaskResult(BaseModel):
    task: str = Field(..., description="Task ID")
    result: str = Field(description="Base64-encoded and encrypted result", default=None)
    error: str = Field(
        description="Base64-encoded and encrypted error message", default=None
    )
    duration: float = Field(..., description="Duration of task execution")


class ResultsStore(BaseModel):
    results: List[TaskResult] = Field(..., description="List of results to store")
    challenge: str = Field(..., description="Encrypted challenge from /auth/challenge")


@app.post("/results/store", tags=["compute"])
def results_store(body: ResultsStore):
    verify_challenge(body.challenge)

    for result in body.results:
        is_error = False
        if result.error is not None:
            is_error = True

        if is_error:
            status = "error"
        else:
            status = "completed"

        update = {
            "status": status,
            "result": result.result,
            "error": result.error,
            "duration": result.duration,
        }
        auth.db.tasks.update_one({"id": result.task}, {"$set": update})


class ResultsRetrieve(BaseModel):
    tasks: List[str] = Field(..., description="Task IDs")
    challenge: str = Field(..., description="Encrypted challenge from /auth/challenge")


@app.post("/results/retrieve", tags=["compute"])
def results_retreive(body: ResultsRetrieve):
    verify_challenge(body.challenge)

    results = {_: None for _ in body.tasks}
    for task in auth.db.tasks.find({"id": {"$in": body.tasks}}):
        entry = {"result": None, "error": None}
        has_info = False
        for key in entry.keys():
            if key in task:
                entry[key] = task[key]
                has_info = True
        if has_info:
            results[task["id"]] = entry

    return results


@app.get("/queue/inspect", tags=["statistics"])
def inspect_usage():
    ret = {}
    unixminute_now = int(time.time() / 60)
    unixminute_first = unixminute_now - 60
    for stats in auth.db.stats.find({"ts": {"$gte": unixminute_first}}):
        refts = stats["ts"]
        stats = {k: v for k, v in stats.items() if k != "_id" and k != "ts"}
        ret[unixminute_now - refts] = stats
    return ret


@app.get("/datacenters/inspect", tags=["statistics"])
def inspect_usage():
    now = time.time()
    ret = {}
    for dc in auth.db.heartbeat.find():
        ret[dc["datacenter"]] = max(0, int(now - dc["ts"]))
    return ret


class QueueHasWork(BaseModel):
    challenge: str = Field(..., description="Encrypted challenge from /auth/challenge")
    datacenter: str = Field(..., description="Datacenter checking in.")
    packages: Dict[str, List[str]] = Field(..., description="Installed packages.")


@app.post("/queue/haswork", tags=["statistics"])
def task_inspect(body: QueueHasWork):
    verify_challenge(body.challenge)

    # update heartbeat
    auth.db.heartbeat.update_one(
        {"datacenter": body.datacenter}, {"$set": {"ts": time.time()}}, upsert=True
    )

    # check if there is work
    qs = rq.Queue.all(connection=auth.redis)
    regex = r"py-(?P<pythonversion>.+),nc-(?P<numcores>.+),dc-(?P<datacenter>.+)"

    queues = []
    for queue in qs:
        m = re.match(regex, queue.name)
        if m is None:
            continue
        if m.group("datacenter") != body.datacenter and m.group("datacenter") != "any":
            continue

        if queue.count > 0:
            queues.append(
                {
                    "version": m.group("pythonversion"),
                    "numcores": m.group("numcores"),
                    "name": queue.name,
                }
            )

    return queues


@app.get("/system/status", tags=["statistics"])
def task_inspect():
    return "ok"
