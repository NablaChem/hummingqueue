from pydantic import BaseModel, Field
from typing import List, Dict
import re
import time
import random
from fastapi import APIRouter
from pydantic import BaseModel, Field

from .. import auth

app = APIRouter()


@app.get("/tags/inspect", tags=["statistics"])
async def inspect_tags():
    unixminute_now = int(time.time() / 60)
    unixminute_first = unixminute_now - 1
    tags = {}
    async for stats in auth.db.stats_tags.find({"ts": {"$gte": unixminute_first}}):
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


class InspectTagRequest(BaseModel):
    tag: str = Field(..., description="Tag to filter tasks by")


class TagStatusResponse(BaseModel):
    completed: int
    pending: int
    queued: int
    deleted: int
    failed: int
    total: int


@app.post(
    "/tag/inspect",
    tags=["statistics"],
    response_model=TagStatusResponse,
    summary="Inspect tag statistics",
)
async def inspect_tag(request: InspectTagRequest):
    """
    Fetch task statistics for a given tag.

    - **tag**: The tag used to filter tasks.
    - Returns a summary of task counts grouped by status.
    """

    pipeline = [
        {"$match": {"tag": request.tag}},
        {"$group": {"_id": "$status", "count": {"$sum": 1}}},
    ]

    fields = ["error", "completed", "pending", "queued", "deleted"]
    status: Dict[str, int] = {field: 0 for field in fields}

    async for record in auth.db.tasks.aggregate(pipeline):
        status[record["_id"]] = record["count"]

    status["failed"] = status.pop("error")
    status["total"] = sum(status.values())

    return status


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
async def tasks_dequeue(body: TasksDequeue):
    await auth.verify_challenge(body.challenge)

    # update heartbeat
    await auth.db.heartbeat.update_one(
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
    active_queues = [_["queue"] async for _ in auth.db.active_queues.find()]
    random.shuffle(active_queues)
    runid = time.time()
    limit_mb = 30
    payload_size = 0
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
            candidate_ids = [_["_id"] async for _ in candidates]
            if len(candidate_ids) == 0:
                break

            await auth.db.tasks.update_many(
                {
                    "status": "pending",
                    "queues": queue,
                    "$or": [{"inflight": {"$lt": runid - 60}}, {"inflight": None}],
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
            async for task in inflight:
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
                payload_size += len(task["call"])
                if payload_size > limit_mb * 1024 * 1024:
                    break
            if inflights == 0 or payload_size > limit_mb * 1024 * 1024:
                break

    # remove empty queues
    for queue in active_queues:
        if queue not in tasks:
            await auth.db.active_queues.delete_one({"queue": queue})

    if len(logentries) > 0:
        await auth.db.logs.insert_many(logentries)

    return tasks
