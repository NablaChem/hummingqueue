from rq import Queue
from redis import Redis
import time
from . import auth

redis_conn = Redis(host="redis", db=1)
q = Queue(connection=redis_conn)


def refill_redis(njobs: int):
    tasks = []
    logentries = []
    for i in range(njobs):
        task = auth.db.tasks.find_one_and_update(
            {"status": "pending"},
            {"$set": {"status": "queued"}},
            return_document=True,
        )
        if task is None:
            break
        tasks.append(
            {"call": task["call"], "function": task["function"], "hmqid": task["id"]}
        )
        logentries.append({"event": "task/fetch", "id": task["id"], "ts": time.time()})
    if len(logentries) > 0:
        auth.db.logs.insert_many(logentries)
        for task in tasks:
            q.enqueue("hmq.unwrap", **task)


def flow_control():
    # MongoDB -> redis
    while True:
        missing = max(100 - q.count, 0)
        refill_redis(missing)
        time.sleep(1)
