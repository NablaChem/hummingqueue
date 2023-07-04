import time
import hashlib
from . import auth
import enum
import shortuuid
from datetime import datetime, timezone
from fastapi import Header
from pydantic import Field
from nacl.signing import VerifyKey
import base64

HMQ_SIGNATURE_HEADER = Header(
    default=None,
    description="Base-64 encoded signature of the request body.",
)


def utc_now() -> int:
    """Returns the current unix timestamp for UTC time zone."""
    now_utc = datetime.now(timezone.utc)
    return int(now_utc.timestamp())


def build_schema_example(cls):
    """Builds a combined schema example for inherited pydantic models"""
    sw = dict()
    for parent in cls.mro():
        try:
            parent_example = parent.Config.schema_extra["example"]
            sw.update(parent_example)
        except:
            continue
    cls.Config.schema_extra["example"] = sw
