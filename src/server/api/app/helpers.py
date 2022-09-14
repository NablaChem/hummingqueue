import time
import hashlib
from . import auth


def get_valid_challenges():
    """Returns the current and past time-based challenges which are unguessable for the compute nodes."""
    challenges = dict()
    unixhour = int(time.time() / 3600)
    challenges["current"] = hashlib.sha256(
        (auth.salt + str(unixhour)).encode("utf8")
    ).hexdigest()
    challenges["previous"] = hashlib.sha256(
        (auth.salt + str(unixhour - 1)).encode("utf8")
    ).hexdigest()
    return challenges
