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


@app.get(
    "/auth/challenge",
    tags=["security"],
)
def auth_challenge():
    now = str(time.time())
    return {"challenge": now}
