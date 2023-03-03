import os
import pymongo

mongo_connstr, mongo_db = os.getenv("MONGODB_CONNSTR").rsplit("/", 1)
db = pymongo.MongoClient(mongo_connstr)[mongo_db]
salt = os.getenv("API_SALT")
admin_token = os.getenv("API_ADMINTOKEN")
CHALLENGE_PERIOD_LENGTH_SECONDS = 3600
