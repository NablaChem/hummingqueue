import time
import hashlib
from . import auth
import secrets


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


def new_token():
    return secrets.token_urlsafe(auth.TOKEN_BYTES)
