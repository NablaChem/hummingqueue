import os
import pymongo
from nacl.secret import SecretBox
from nacl.signing import VerifyKey
import base64

mongo_connstr, mongo_db = os.getenv("MONGODB_CONNSTR").rsplit("/", 1)
client = pymongo.MongoClient(mongo_connstr)
db = client[mongo_db]
admin_signature = VerifyKey(base64.b64decode(os.getenv("ADMIN_SIGN")))

# make sure indexes are available
# tasks
db.tasks.create_index([("id", pymongo.ASCENDING)], unique=True)
db.tasks.create_index(
    [("status", pymongo.ASCENDING)], partialFilterExpression={"status": "pending"}
)
db.tasks.create_index(
    [("status", pymongo.ASCENDING), ("inflight", pymongo.ASCENDING)], sparse=True
)
db.tasks.create_index(
    [
        ("tag", pymongo.ASCENDING),
        ("ncores", pymongo.ASCENDING),
        ("status", pymongo.ASCENDING),
    ]
)
db.tasks.create_index([("received", pymongo.ASCENDING)])
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
# functions
db.functions.create_index([("digest", pymongo.ASCENDING)], unique=True)
