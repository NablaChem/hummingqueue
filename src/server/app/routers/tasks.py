from pydantic import BaseModel, Field
from typing import List
import time
import uuid
import json
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from .. import auth

app = APIRouter()


class TaskSubmit(BaseModel):
    tag: str = Field(..., description="Tag for the task")
    function: str = Field(..., description="SHA256 digest of function")
    calls: str = Field(..., description="JSON-encoded list of calls")
    ncores: int = Field(..., description="Number of cores to use")
    datacenters: List[str] = Field(..., description="Acceptable datacenters")
    challenge: str = Field(..., description="Encrypted challenge from /auth/challenge")
    digest: str = Field(..., description="SHA256 digest of calls")


@app.post("/tasks/submit", tags=["compute"])
async def tasks_submit(body: TaskSubmit):
    await auth.verify_challenge(body.challenge)
    # todo: verify digest

    calls = json.loads(body.calls)
    uuids = []
    logentries = []
    taskentries = []

    function = await auth.db.functions.find_one({"digest": body.function})
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

    async with await auth.client.start_session() as session:
        async with session.start_transaction():
            if len(logentries) > 0:
                await auth.db.logs.insert_many(logentries)
                await auth.db.tasks.insert_many(taskentries)

    for queue in queues:
        await auth.db.active_queues.update_one(
            {"queue": queue}, {"$set": {"queue": queue}}, upsert=True
        )

    return uuids


class TasksDelete(BaseModel):
    challenge: str = Field(..., description="Encrypted challenge from /auth/challenge")
    tasks: List[str] = Field(None, description="List of task ids to cancel and delete.")
    tag: str = Field(None, description="Tag of which to cancel and delete all tasks.")


class TasksCancel(BaseModel):
    challenge: str = Field(..., description="Encrypted challenge from /auth/challenge")
    tasks: List[str] = Field(None, description="List of task ids to cancel.")
    tag: str = Field(None, description="Tag of which to cancel all tasks.")


class TasksDeleteCancelResponse(BaseModel):
    count: int = Field(..., description="Number of tasks affected.")


@app.post("/tasks/cancel", tags=["compute"], response_model=TasksDeleteCancelResponse)
async def tasks_delete(body: TasksCancel):
    await auth.verify_challenge(body.challenge)

    return {"count": await cancel_and_delete(body, delete=False)}


@app.post("/tasks/delete", tags=["compute"], response_model=TasksDeleteCancelResponse)
async def tasks_delete(body: TasksDelete):
    await auth.verify_challenge(body.challenge)

    return {"count": await cancel_and_delete(body, delete=True)}


async def cancel_and_delete(body, delete):
    if body.tag:
        if delete:
            criterion = {"tag": body.tag}
        else:
            criterion = {"tag": body.tag, "status": {"$in": ["pending", "queued"]}}
    elif body.tasks:
        if delete:
            criterion = {
                "id": {"$in": body.tasks},
            }
        else:
            criterion = {
                "id": {"$in": body.tasks},
                "status": {"$in": ["pending", "queued"]},
            }
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Either tag or tasks must be provided.",
        )

    num_affected = await auth.db.tasks.count_documents(criterion)
    if delete:
        await auth.db.tasks.delete_many(criterion)
    else:
        await auth.db.tasks.update_many(criterion, {"$set": {"status": "deleted"}})
        await auth.db.tasks.update_many(
            criterion, {"$unset": {"error": "", "result": "", "call": ""}}
        )

    return num_affected


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
async def task_sync(body: TasksSync):
    await auth.verify_challenge(body.challenge)

    lost = []
    stale = set(body.known)
    async for task in auth.db.tasks.find(
        {"on_datacenter": body.datacenter, "status": "queued"}
    ):
        if task["id"] not in body.known:
            lost.append(task["id"])
        else:
            stale.remove(task["id"])

    # for all in lost: set status to pending and remove on_datacenter
    await auth.db.tasks.update_many(
        {"id": {"$in": lost}},
        {"$set": {"status": "pending"}, "$unset": {"on_datacenter": 1}},
    )

    return list(stale)


@app.post("/tasks/inspect", tags=["compute"])
async def task_inspect(body: TasksInspect):
    await auth.verify_challenge(body.challenge)

    result = {_: None for _ in body.tasks}
    async for task in auth.db.tasks.find({"id": {"$in": body.tasks}}):
        result[task["id"]] = task["status"]

    return result

class TasksFind(BaseModel):
    challenge: str = Field(..., description="Encrypted challenge from /auth/challenge")
    tag: str = Field(..., description="Tag of which to list all tasks.")


@app.post("/tasks/find", tags=["compute"], response_model=list[str])
async def tasks_find(body: TasksFind):
    """
    Retrieves a list of task IDs based on the given tag.

    - **challenge**: Encrypted challenge for authentication.
    - **tag**: The tag to filter tasks.
    - Returns: A list of task IDs.
    """
    await auth.verify_challenge(body.challenge)

    result = [
        task["id"]
        async for task in auth.db.tasks.find({"tag": body.tag}, {"id": 1, "_id": 0})
    ]

    return result
