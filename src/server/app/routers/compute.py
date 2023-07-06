from pydantic import BaseModel, Field
from typing import Optional, List
import rq
import base64
import time
import uuid
import json
from fastapi import APIRouter, Request, HTTPException, status
from pydantic import BaseModel, Field

from .. import helpers
from .. import auth
from .. import maintenance

app = APIRouter()


def verify_challenge(encrypted_challenge):
    if (
        time.time()
        - float(
            auth.encryption_key.decrypt(base64.b64decode(encrypted_challenge)).decode(
                "ascii"
            )
        )
        > 60
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid challenge"
        )


class FunctionRegister(BaseModel):
    function: str = Field(..., description="Base64-encoded and encrypted function")
    digest: str = Field(..., description="SHA256 digest of function")
    challenge: str = Field(..., description="Encrypted challenge from /auth/challenge")


@app.post(
    "/function/register",
    tags=["compute"],
)
def task_register(body: FunctionRegister):
    verify_challenge(body.challenge)

    if not auth.db.functions.find_one({"digest": body.digest}):
        auth.db.functions.insert_one({"digest": body.digest, "function": body.function})
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
        return {"function": code["function"]}
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Invalid digest")


class TaskSubmit(BaseModel):
    tag: str = Field(..., description="Tag for the task")
    function: str = Field(..., description="SHA256 digest of function")
    calls: str = Field(..., description="JSON-encoded list of calls")
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
    for call in calls:
        taskid = str(uuid.uuid4())
        logentries.append({"event": "task/submit", "id": taskid, "ts": time.time()})
        taskentries.append(
            {
                "id": taskid,
                "call": call,
                "tag": body.tag,
                "function": body.function,
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
        entry = {"status": None, "result": None, "error": None, "duration": None}
        for key in entry.keys():
            if key in entry:
                entry[key] = task[key]
        results[task["id"]] = entry

    return results
