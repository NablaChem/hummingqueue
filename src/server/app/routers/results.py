from pydantic import BaseModel, Field
from typing import List
import time
from fastapi import APIRouter
from pydantic import BaseModel, Field

from .. import auth

app = APIRouter()


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
async def results_store(body: ResultsStore):
    await auth.verify_challenge(body.challenge)

    now = time.time()
    njobs = len(body.results)
    coretime = 0
    for result in body.results:
        walltime = result.duration
        doc = await auth.db.tasks.find_one({"id": result.task})
        ncores = doc["ncores"]
        coretime += walltime * ncores

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
        await auth.db.tasks.update_one({"id": result.task}, {"$set": update})

    await auth.db.counters.update_one({"metric": "coretime"}, {"$inc": {"value": coretime}})
    await auth.db.counters.update_one({"metric": "njobs"}, {"$inc": {"value": njobs}})


class ResultsRetrieve(BaseModel):
    tasks: List[str] = Field(..., description="Task IDs")
    challenge: str = Field(..., description="Encrypted challenge from /auth/challenge")


@app.post("/results/retrieve", tags=["compute"])
async def results_retreive(body: ResultsRetrieve):
    await auth.verify_challenge(body.challenge)

    results = {_: None for _ in body.tasks}
    async for task in auth.db.tasks.find({"id": {"$in": body.tasks}}):
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
