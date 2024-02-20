import os
import pymongo
from nacl.secret import SecretBox
from nacl.signing import VerifyKey
import base64

mongo_connstr, mongo_db = os.getenv("MONGODB_CONNSTR").rsplit("/", 1)
client = pymongo.MongoClient(mongo_connstr)
db = client[mongo_db]
encryption_key = SecretBox(base64.b64decode(os.getenv("API_TOKEN")))
admin_signature = VerifyKey(base64.b64decode(os.getenv("ADMIN_SIGN")))
