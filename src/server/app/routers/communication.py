from pydantic import BaseModel, Field
from typing import Optional, List
from fastapi import APIRouter, Response

from .. import helpers
from .. import auth

app = APIRouter()


@app.get(
    "/ping",
    tags=["communication"],
    summary="Check whether the server is up.",
    responses={200: {"description": "Server is up."}},
)
def ping():
    return Response(content="pong", media_type="text/plain")
