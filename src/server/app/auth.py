import os
import pymongo
from nacl.signing import VerifyKey
import base64
import base64
import time
from fastapi import HTTPException, status
from nacl.signing import VerifyKey
import nacl

mongo_connstr, mongo_db = os.getenv("MONGODB_CONNSTR").rsplit("/", 1)
client = pymongo.MongoClient(mongo_connstr)
db = client[mongo_db]
admin_signature = VerifyKey(base64.b64decode(os.getenv("ADMIN_SIGN")))

# make sure indexes are available
# tasks
try:
    db.tasks.create_index([("id", pymongo.ASCENDING)], unique=True)
except pymongo.errors.OperationFailure:
    pass
try:
    db.tasks.create_index(
        [("status", pymongo.ASCENDING)], partialFilterExpression={"status": "pending"}
    )
except pymongo.errors.OperationFailure:
    pass
try:
    db.tasks.create_index(
        [("status", pymongo.ASCENDING), ("inflight", pymongo.ASCENDING)], sparse=True
    )
except pymongo.errors.OperationFailure:
    pass
try:
    db.tasks.create_index(
        [
            ("tag", pymongo.ASCENDING),
            ("ncores", pymongo.ASCENDING),
            ("status", pymongo.ASCENDING),
        ]
    )
except pymongo.errors.OperationFailure:
    pass
try:
    db.tasks.create_index([("received", pymongo.ASCENDING)])
except pymongo.errors.OperationFailure:
    pass
try:
    db.tasks.create_index(
        [
            ("tag", pymongo.ASCENDING),
            ("ncores", pymongo.ASCENDING),
            ("status", pymongo.ASCENDING),
            ("duration", pymongo.ASCENDING),
            ("received", pymongo.ASCENDING),
            ("done", pymongo.ASCENDING),
        ]
    )
except pymongo.errors.OperationFailure:
    pass

# functions
try:
    db.functions.create_index([("digest", pymongo.ASCENDING)], unique=True)
except pymongo.errors.OperationFailure:
    pass


def verify_challenge(signed_challenge):
    for user in db.users.find():
        verify_key = VerifyKey(base64.b64decode(user["sign"]))
        try:
            signed = verify_key.verify(
                base64.b64decode(signed_challenge.encode("ascii"))
            ).decode("ascii")
        except nacl.exceptions.BadSignatureError:
            continue
        try:
            signed_time = float(signed)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Challenge is not a signed timestamp.",
            )
        break
    else:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid compute key for challenge",
        )
    if time.time() - signed_time > 60:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Old challenge"
        )
