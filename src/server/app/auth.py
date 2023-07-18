import os
import pymongo
from nacl.secret import SecretBox
import base64
from redis import Redis

mongo_connstr, mongo_db = os.getenv("MONGODB_CONNSTR").rsplit("/", 1)
client = pymongo.MongoClient(mongo_connstr)
db = client[mongo_db]
encryption_key = SecretBox(base64.b64decode(os.getenv("API_TOKEN")))

redis = Redis(host="redis", db=1)
