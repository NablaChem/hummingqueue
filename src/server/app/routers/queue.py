from pydantic import BaseModel, Field
import time
from fastapi import APIRouter
from pydantic import BaseModel, Field

from .. import auth

app = APIRouter()


class QueueRequirements(BaseModel):
    datacenter: str = Field(..., description="Datacenter name")
    challenge: str = Field(..., description="Encrypted challenge from /auth/challenge")
    version: str = Field(..., description="Python version.")


@app.post("/queue/requirements", tags=["compute"])
def queue_requirements(body: QueueRequirements):
    auth.verify_challenge(body.challenge)

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
    if 0 not in ret:
        ret[0] = {}

    ret[0]["total_coretime"] = auth.db.counters.find_one({"metric": "coretime"})[
        "value"
    ]
    ret[0]["total_jobs"] = auth.db.counters.find_one({"metric": "njobs"})["value"]
    return ret
