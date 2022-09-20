from multiprocessing.sharedctypes import Value
from pydantic import BaseModel, Field, constr, root_validator
from typing import List, Optional
import time

from . import validators
from . import helpers
from . import auth

counter = 0

from fastapi import FastAPI, Request

app = FastAPI()
from .routers import owner

app.include_router(owner.app)


class NodeAuth(BaseModel):
    node_id: str = Field(..., description="User-generated identifier of the node.")
    owner_token: str = Field(
        ..., description="System-generated identifier of the owner."
    )
    compute_token: str = Field(..., description="Authentication for node.")


class UserAuth(BaseModel):
    user_token: str = Field(..., description="System-generated identifier of the user.")
    collaboration_token: Optional[str] = Field(
        ..., description="System-generated identifier of a collaboration."
    )


class JobFetch(NodeAuth):
    in_cache: List[str] = Field(..., description="Already cached containers.")
    cores: int = Field(..., description="Number of cores available.")
    memory_mb: int = Field(..., description="Available memory in MB.")


class JobSpec(UserAuth):
    container_hash: str = Field(..., description="Checksum of the container to run.")
    # command: constr = Field(..., description="Command to run.", max_length=2000)
    # data_source: str = Field(
    #     ..., description="S3 directory containing the data to be present."
    # )
    # data_target: str = Field(
    #     ...,
    #     description="S3 directory where all modified files should be uploaded to.",
    # )
    core_limit: int = Field(..., description="Requested number of cores.")
    memory_mb_limit: int = Field(..., description="Requested memory in MB.")
    time_seconds_limit: int = Field(..., description="Requested wall time duration.")
    tags: Optional[List[str]] = Field(
        ...,
        description="List of tags this calculation belongs to. Unique within a collaboration or (if no collaboration is specified) for a user.",
    )


@app.get("/challenge")
def current_challenge():
    return helpers.get_valid_challenges()["current"]


@app.post("/heartbeat")
def register_heartbeat(body: NodeAuth, request: Request):
    # validate request
    owner = validators.owner_exists(body)
    validators.body_signed(request, owner)
    validators.compute_token_valid(body, owner)

    # handle request
    criteria = {"owner_token": body.owner_token, "node_id": body.node_id}
    heartbeat = criteria.copy()
    heartbeat["seen"] = time.time()
    auth.db.heartbeats.replace_one(criteria, heartbeat)
    return helpers.get_valid_challenges()["current"]


@app.post("/job/fetch")
def job_fetch(body: JobFetch):
    global counter
    jobids = list(range(counter, counter + body.cores))
    counter += body.cores
    return {_: None for _ in jobids}


@app.post("/job/create")
def job_create(body: JobSpec):
    pass
