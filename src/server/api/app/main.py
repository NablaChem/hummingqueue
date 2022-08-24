from pydantic import BaseModel, Field, constr
from typing import List, Optional
import pymongo
import os

counter = 0

from fastapi import FastAPI

app = FastAPI()
db = pymongo.MongoClient(os.getenv("MONGODB_CONNSTR"))


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
    container: str = Field(..., description="Name of the container to run.")
    command: constr = Field(..., description="Command to run.", max_length=2000)
    data_source: str = Field(
        ..., description="S3 directory containing the data to be present."
    )
    data_target: str = Field(
        ...,
        description="S3 directory where all modified files should be uploaded to.",
    )
    core_limit: int = Field(..., description="Requested number of cores.")
    memory_mb_limit: int = Field(..., description="Requested memory in MB.")
    time_seconds_limit: int = Field(..., description="Requested wall time duration.")
    tags: Optional[List[str]] = Field(
        ...,
        description="List of tags this calculation belongs to. Unique within a collaboration or (if no collaboration is specified) for a user.",
    )


@app.post("/heartbeat")
def register_heartbeat(body: NodeAuth):
    # TODO: write to mongodb
    # TODO: rreturn challenge for signed communication
    pass


@app.post("/job/fetch")
def job_fetch(body: JobFetch):
    global counter
    jobids = list(range(counter, counter + body.cores))
    counter += body.cores
    return {_: None for _ in jobids}


@app.post("/job/create")
def job_create(body: JobSpec):
    pass


@app.post("/owner/activate")
def owner_activate():
    # TODO: initial public key load, only works once
    pass
