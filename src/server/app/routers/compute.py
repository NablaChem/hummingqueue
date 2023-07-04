from pydantic import BaseModel, Field
from typing import Optional, List
import hashlib
import base64
import time
import uuid
import json
from fastapi import APIRouter, Request, HTTPException, status
from pydantic import BaseModel, Field

from .. import helpers
from .. import auth

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


@app.post("/task/submit", tags=["compute"])
def task_submit(body: TaskSubmit):
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


class TaskFetch(BaseModel):
    count: int = Field(..., description="Number of tasks to fetch")
    challenge: str = Field(..., description="Encrypted challenge from /auth/challenge")


# @app.post("/task/fetch", tags=["compute"])
# def task_fetch(body: TaskFetch):
#     verify_challenge(body.challenge)

#     tasks = []
#     logentries = []
#     for i in range(body.count):
#         task = auth.db.tasks.find_one_and_update(
#             {"status": "pending"},
#             {"$set": {"status": "queued"}},
#             return_document=True,
#         )
#         if task is None:
#             break
#         tasks.append(
#             {"call": task["call"], "function": task["function"], "id": task["id"]}
#         )
#         logentries.append({"event": "task/fetch", "id": task["id"], "ts": time.time()})

#     if len(logentries) > 0:
#         auth.db.logs.insert_many(logentries)
#     return tasks


class TaskResult(BaseModel):
    id: str = Field(..., description="Task ID")
    result: str = Field(description="Base64-encoded and encrypted result", default=None)
    error: str = Field(
        description="Base64-encoded and encrypted error message", default=None
    )
    duration: float = Field(..., description="Duration of task execution")
    challenge: str = Field(..., description="Encrypted challenge from /auth/challenge")


@app.post("/task/result", tags=["compute"])
def task_result(body: TaskResult):
    verify_challenge(body.challenge)

    is_error = False
    if body.error is not None:
        is_error = True

    if is_error:
        status = "error"
    else:
        status = "completed"

    update = {
        "status": status,
        "result": body.result,
        "error": body.error,
        "duration": body.duration,
    }
    auth.db.tasks.update_one({"id": body.id}, {"$set": update})


# task is getting old on compute node
# @app.post("/task/return", tags=["compute"])
# def task_resturn
