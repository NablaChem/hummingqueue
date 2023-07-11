from rq import Queue, Worker
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
        idmapping = {}
        for task in tasks:
            job = q.enqueue("hmq.unwrap", **task)
            idmapping[task["hmqid"]] = job.id
        redis_conn.hset("id2id", mapping=idmapping)


def update_stats(last_update: float):
    now = time.time()
    unixminute_now = int(now / 60)
    unixminute_last = int(last_update / 60)

    if unixminute_now == unixminute_last:
        return last_update

    workers = Worker.all(connection=redis_conn)
    nworkers = len(workers)
    njobs = q.count + auth.db.tasks.count_documents({"status": "pending"})
    nrunning = len([_ for _ in workers if _.state == "busy"])
    auth.db.stats.insert_one(
        {
            "cores_available": nworkers,
            "cores_used": nrunning,
            "tasks_queued": njobs,
            "tasks_running": nrunning,
            "ts": unixminute_now,
        }
    )
    return now


def flow_control():
    # MongoDB -> redis
    last_update = 0
    while True:
        missing = max(100 - q.count, 0)
        refill_redis(missing)
        last_update = update_stats(last_update)
        time.sleep(1)
