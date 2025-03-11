from pydantic import BaseModel, Field
from typing import List, Dict
import re
import base64
import time
import uuid
import random
import json
from fastapi import APIRouter, HTTPException, status
from nacl.signing import VerifyKey
import nacl
from pydantic import BaseModel, Field

from .. import auth

app = APIRouter()


@app.get("/datacenters/inspect", tags=["statistics"])
def inspect_usage():
    now = time.time()
    ret = {}
    for dc in auth.db.heartbeat.find():
        ret[dc["datacenter"]] = max(0, int(now - dc["ts"]))
    return ret


@app.get("/system/status", tags=["statistics"])
def task_inspect():
    return "ok"
