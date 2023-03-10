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
OWNER_TOKEN_FIELD = Field(
    None,
    description="Owner token for the user who is signing the key.",
    regex="^O-[A-Za-z0-9]+$",
)


class TokenTypes(str, enum.Enum):
    OWNER = "O"
    USER = "U"
    JOB = "J"
    PROJECT = "P"


def get_valid_challenges():
    """Returns the current and past time-based challenges which are unguessable for the compute nodes."""
    challenges = dict()

    unixperiod = int(time.time() / auth.CHALLENGE_PERIOD_LENGTH_SECONDS)
    challenges["current"] = hashlib.sha256(
        (auth.salt + str(unixperiod)).encode("utf8")
    ).hexdigest()
    challenges["previous"] = hashlib.sha256(
        (auth.salt + str(unixperiod - 1)).encode("utf8")
    ).hexdigest()

    renew_seconds = auth.CHALLENGE_PERIOD_LENGTH_SECONDS - (
        time.time() % auth.CHALLENGE_PERIOD_LENGTH_SECONDS
    )
    return challenges, int(renew_seconds)


def get_verify_key(authority: str) -> VerifyKey:
    """Returns the verify key for the given authority."""
    user = auth.db.users.find_one({"token": authority})
    if user is None:
        raise ValueError("No such authority.")
    return VerifyKey(base64.b64decode(user["verify_key"]))


def new_token(type: TokenTypes):
    """Returns a new token of the given type."""
    return f"{type.value}-{shortuuid.random()}"


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
