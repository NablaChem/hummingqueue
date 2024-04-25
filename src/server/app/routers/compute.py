from pydantic import BaseModel, Field
from typing import List, Dict
import re
import base64
import time
import uuid
import random
import json
from fastapi import APIRouter, HTTPException, status
from nacl.signing import VerifyKey
import nacl
from pydantic import BaseModel, Field

from .. import auth

app = APIRouter()


def verify_challenge(signed_challenge):
    for user in auth.db.users.find():
        verify_key = VerifyKey(base64.b64decode(user["sign"]))
        try:
            signed = verify_key.verify(
                base64.b64decode(signed_challenge.encode("ascii"))
            ).decode("ascii")
        except nacl.exceptions.BadSignatureError:
            continue
        try:
            signed_time = float(signed)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Challenge is not a signed timestamp.",
            )
        break
    else:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid compute key for challenge",
        )
    if time.time() - signed_time > 60:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Old challenge"
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
    prefix = f'py-{function["major"]}.{function["minor"]}-nc-{body.ncores}-dc-'
    queues = [f"{prefix}{_}" for _ in body.datacenters]
    if queues == []:
        queues = [f"{prefix}any"]

    now = time.time()
    for call in calls:
        taskid = str(uuid.uuid4())
        logentries.append({"event": "task/submit", "id": taskid, "ts": now})
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
                "received": now,
            }
        )
        uuids.append(taskid)

    with auth.client.start_session() as session:
        with session.start_transaction():
            if len(logentries) > 0:
                auth.db.logs.insert_many(logentries)
                auth.db.tasks.insert_many(taskentries)

    for queue in queues:
        auth.db.active_queues.update_one(
            {"queue": queue}, {"$set": {"queue": queue}}, upsert=True
        )

    return uuids


class TasksDelete(BaseModel):
    challenge: str = Field(..., description="Encrypted challenge from /auth/challenge")
    delete: List[str] = Field(..., description="List of task ids to cancel and delete.")


@app.post("/tasks/delete", tags=["compute"])
def tasks_delete(body: TasksDelete):
    verify_challenge(body.challenge)

    # mongodb
    logentries = []
    to_be_deleted = {
        "id": {"$in": body.delete},
        "status": {"$in": ["pending", "queued"]},
    }

    existing = auth.db.tasks.find(to_be_deleted)
    now = time.time()
    existing_ids = []
    for task in existing:
        logentries.append({"event": "task/delete", "id": task["id"], "ts": now})
        existing_ids.append(task["id"])
    auth.db.tasks.update_many(to_be_deleted, {"$set": {"status": "deleted"}})
    auth.db.tasks.update_many(
        to_be_deleted, {"$unset": {"error": "", "result": "", "call": ""}}
    )
    if len(logentries) > 0:
        auth.db.logs.insert_many(logentries)

    return existing_ids


class TasksInspect(BaseModel):
    challenge: str = Field(..., description="Encrypted challenge from /auth/challenge")
    tasks: List[str] = Field(
        ..., description="List of task ids of which to query the status."
    )


class TasksSync(BaseModel):
    challenge: str = Field(..., description="Encrypted challenge from /auth/challenge")
    datacenter: str = Field(..., description="Datacenter checking in.")
    known: List[str] = Field(
        ..., description="List of task ids of which to query the status."
    )


@app.post("/tasks/sync", tags=["compute"])
def task_sync(body: TasksSync):
    verify_challenge(body.challenge)

    lost = []
    stale = set(body.known)
    for task in auth.db.tasks.find(
        {"on_datacenter": body.datacenter, "status": "queued"}
    ):
        if task["id"] not in body.known:
            lost.append(task["id"])
        else:
            stale.remove(task["id"])

    # for all in lost: set status to pending and remove on_datacenter
    auth.db.tasks.update_many(
        {"id": {"$in": lost}},
        {"$set": {"status": "pending"}, "$unset": {"on_datacenter": 1}},
    )

    return list(stale)


@app.post("/tasks/inspect", tags=["compute"])
def task_inspect(body: TasksInspect):
    verify_challenge(body.challenge)

    result = {_: None for _ in body.tasks}
    for task in auth.db.tasks.find({"id": {"$in": body.tasks}}):
        result[task["id"]] = task["status"]

    return result


@app.post("/tasks/inspect", tags=["compute"])
def task_inspect(body: TasksInspect):
    verify_challenge(body.challenge)

    result = {_: None for _ in body.tasks}
    for task in auth.db.tasks.find({"id": {"$in": body.tasks}}):
        result[task["id"]] = task["status"]

    return result


class TasksFind(BaseModel):
    challenge: str = Field(..., description="Encrypted challenge from /auth/challenge")
    tag: str = Field(..., description="Tag of which to list all tasks.")


@app.post("/tasks/find", tags=["compute"])
def task_find(body: TasksFind):
    verify_challenge(body.challenge)

    result = []
    for task in auth.db.tasks.find({"tag": body.tag}):
        result.append(task["id"])

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

    now = time.time()
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
            "done": now,
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
        if task["status"] == "deleted":
            entry["error"] = "Hummingqueue: Task deleted"
            has_info = True
        if has_info:
            results[task["id"]] = entry

    return results


class QueueRequirements(BaseModel):
    datacenter: str = Field(..., description="Datacenter name")
    challenge: str = Field(..., description="Encrypted challenge from /auth/challenge")
    version: str = Field(..., description="Python version.")


@app.post("/queue/requirements", tags=["compute"])
def queue_requirements(body: QueueRequirements):
    verify_challenge(body.challenge)

    major, minor = body.version.split(".")
    major = int(major)
    minor = int(minor)

    pipeline = [
        {"$match": {"major": major, "minor": minor}},
        {"$project": {"packages": 1, "_id": 0}},
        {"$project": {"packages": {"$objectToArray": "$packages"}}},
        {"$unwind": "$packages"},
        {"$group": {"_id": "$packages.k", "name": {"$first": "$packages.v"}}},
    ]

    return [_["name"] for _ in auth.db.functions.aggregate(pipeline)]


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


@app.get("/tags/inspect", tags=["statistics"])
def inspect_tags():
    unixminute_now = int(time.time() / 60)
    unixminute_first = unixminute_now - 1
    tags = {}
    for stats in auth.db.stats_tags.find({"ts": {"$gte": unixminute_first}}):
        del stats["_id"]
        if stats["tag"] not in tags or tags[stats["tag"]]["ts"] < stats["ts"]:
            tags[stats["tag"]] = stats

    results = []
    for stats in tags.values():
        # incomplete or recent
        try:
            if (
                stats["pending"] > 0
                or stats["queued"] > 0
                or unixminute_now * 60 - stats["updated"] < 7 * 24 * 60 * 60
            ):
                results.append(stats)
        except:
            continue
    return results


@app.get("/datacenters/inspect", tags=["statistics"])
def inspect_usage():
    now = time.time()
    ret = {}
    for dc in auth.db.heartbeat.find():
        ret[dc["datacenter"]] = max(0, int(now - dc["ts"]))
    return ret


class TasksDequeue(BaseModel):
    challenge: str = Field(..., description="Encrypted challenge from /auth/challenge")
    datacenter: str = Field(..., description="Datacenter checking in.")
    packages: Dict[str, List[str]] = Field(..., description="Installed packages.")
    maxtasks: int = Field(..., description="Maximum number of tasks to return.")
    available: int = Field(
        ..., description="Estimated number of available compute units."
    )
    allocated: int = Field(..., description="Number of allocated compute units.")
    running: int = Field(..., description="Number of running tasks.")
    used: int = Field(..., description="Number of used compute units.")


@app.post("/tasks/dequeue", tags=["compute"])
def tasks_dequeue(body: TasksDequeue):
    verify_challenge(body.challenge)

    # update heartbeat
    auth.db.heartbeat.update_one(
        {"datacenter": body.datacenter},
        {
            "$set": {
                "ts": time.time(),
                "packages": body.packages,
                "available": body.available,
                "allocated": body.allocated,
                "running": body.running,
                "used": body.used,
            }
        },
        upsert=True,
    )

    # check if there is work
    regex = r"py-(?P<pythonversion>.+)-nc-(?P<numcores>.+)-dc-(?P<datacenter>.+)"

    tasks = {}
    remaining = body.maxtasks
    logentries = []
    active_queues = [_["queue"] for _ in auth.db.active_queues.find()]
    random.shuffle(active_queues)
    runid = random.randint(0, 10e6)
    for queue in active_queues:
        m = re.match(regex, queue)
        if m is None:
            continue
        if m.group("datacenter") != body.datacenter and m.group("datacenter") != "any":
            continue

        while remaining > 0:
            candidates = auth.db.tasks.find(
                {"status": "pending", "queues": queue}, {"_id": 1}, limit=remaining
            )
            candidate_ids = [_["_id"] for _ in candidates]
            if len(candidate_ids) == 0:
                break

            auth.db.tasks.update_many(
                {
                    "status": "pending",
                    "queues": queue,
                    "inflight": None,
                    "_id": {"$in": candidate_ids},
                },
                {
                    "$set": {
                        "status": "queued",
                        "inflight": runid,
                        "on_datacenter": body.datacenter,
                    }
                },
            )
            inflight = auth.db.tasks.find({"status": "queued", "inflight": runid})
            inflights = 0
            for task in inflight:
                if queue not in tasks:
                    tasks[queue] = []
                tasks[queue].append(
                    {
                        "call": task["call"],
                        "function": task["function"],
                        "hmqid": task["id"],
                        "job_timeout": "6h",
                    }
                )
                remaining -= 1
                inflights += 1
                logentries.append(
                    {"event": "task/dispatch", "id": task["id"], "ts": time.time()}
                )
            if inflights == 0:
                break

    # remove empty queues
    for queue in active_queues:
        if queue not in tasks:
            auth.db.active_queues.delete_one({"queue": queue})

    if len(logentries) > 0:
        auth.db.logs.insert_many(logentries)

    return tasks


@app.get("/system/status", tags=["statistics"])
def task_inspect():
    return "ok"
