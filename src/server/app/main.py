from multiprocessing.sharedctypes import Value
from pydantic import BaseModel, Field, constr, root_validator
from typing import List, Optional
from fastapi.responses import HTMLResponse
import time
import inspect

from . import validators
from . import helpers
from . import auth

counter = 0

from fastapi import FastAPI, Request, Response

app = FastAPI(
    docs_url=None,
    redoc_url=None,
    title="Hummingqueue API",
    description="""## Motivation
    
Hummingqueue is an open-source, self-hosted, distributed, and scalable job queue for scientific computing. It is designed to be used by researchers and students to run their computations on a cluster of machines. It can be used to span nodes between a university HPC center, a cloud provider, and a home computer. It is designed to be easy to use and to integrate into existing workflows. It is also designed to be secure and to protect the privacy of its users and the data it works with.

## Authentication
Almost all requests are signed where the signature is expected in the header *hmq-signature* of the request. The signature is a base64-encoded Curve25519 signature of the JSON request body where the entries (also nested entries) are sorted by key.
""",
)

# add routes
routes = ["owner"]
for route in routes:
    module = __import__(f"app.routers.{route}", fromlist=["app"])

    for objname, obj in inspect.getmembers(module, inspect.isclass):
        if issubclass(obj, BaseModel):
            helpers.build_schema_example(obj)

    app.include_router(module.app)


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


class ChallengeResponse(BaseModel):
    challenge: str = Field(..., description="Current challenge.")
    renew_in: int = Field(
        ..., description="Seconds until this challenge will be renewed."
    )


@app.get(
    "/challenge",
    summary="Returns the challenge to sign in requests.",
    description="The challenge needs to be included in all signed requests and helps protect against replay attacks. Requests using the last two challenges will be accepted, but you should renew the challenge before the time in the response runs out. You may not rely on the challenge validity being always identical.",
    tags=["authentication"],
    responses={
        200: {"description": "Normal execution.", "model": ChallengeResponse},
    },
)
def current_challenge():
    challenges, renew_in = helpers.get_valid_challenges()
    return {"challenge": challenges["current"], "renew_in": renew_in}


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


@app.get(
    "/ping",
    tags=["authentication"],
    summary="Check whether the server is up.",
    responses={200: {"description": "Server is up."}},
)
def ping():
    return Response(content="pong", media_type="text/plain")


@app.post("/job/fetch")
def job_fetch(body: JobFetch):
    global counter
    jobids = list(range(counter, counter + body.cores))
    counter += body.cores
    return {_: None for _ in jobids}


@app.post("/job/create")
def job_create(body: JobSpec):
    # validate request
    validators.user_exists(body)


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def rapidoc():
    return (
        """
        <!doctype html>
        <html>
            <head>
                <meta charset="utf-8">
                <script 
                    type="module" 
                    src="https://unpkg.com/rapidoc/dist/rapidoc-min.js"
                ></script>
            </head>
            <body>
            <style>
                    rapi-doc::part(section-operation-tag) {
                    display:none;
                    }
            </style>
            <rapi-doc 
                spec-url='"""
        + app.openapi_url
        + """'
                render-style="focused" 
                show-header="false" 
                allow-server-selection="false" 
                allow-authentication="false" 
                schema-description-expanded="true" 
                default-schema-tab="schema"
                primary-color="#1F3664"
                show-method-in-nav-bar="as-colored-block"
                nav-bg-color="#1F3664"
                nav-text-color="#ffffff"
                nav-accent-color="#dd9633"
                font-size="largest"
                schema-description-expanded="true"
                schema-style="table"
                use-path-in-nav-bar="true"
                ></rapi-doc>
            </body> 
        </html>
    """
    )
