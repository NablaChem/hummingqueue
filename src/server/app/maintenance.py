from rq import Queue, Worker
import time
from . import auth


def refill_redis(njobs: int):
    for queue in auth.db.active_queues.find():
        tasks = []
        logentries = []

        name = queue["queue"]
        q = Queue(name, connection=auth.redis)

        seen_at_least_one = False
        for i in range(njobs):
            task = auth.db.tasks.find_one_and_update(
                {"status": "pending", "queues": name},
                {"$set": {"status": "queued"}},
                return_document=True,
            )
            if task is None:
                break
            seen_at_least_one = True
            tasks.append(
                {
                    "call": task["call"],
                    "function": task["function"],
                    "hmqid": task["id"],
                }
            )
            logentries.append(
                {"event": "task/fetch", "id": task["id"], "ts": time.time()}
            )

        if not seen_at_least_one:
            auth.db.active_queues.delete_one({"queue": name})

        if len(logentries) > 0:
            auth.db.logs.insert_many(logentries)
            idmapping = {}
            for task in tasks:
                job = q.enqueue("hmq.unwrap", **task)
                idmapping[task["hmqid"]] = job.id
            auth.redis.hset("id2id", mapping=idmapping)


def update_stats(last_update: float):
    now = time.time()
    unixminute_now = int(now / 60)
    unixminute_last = int(last_update / 60)

    if unixminute_now == unixminute_last:
        return last_update

    workers = Worker.all(connection=auth.redis)
    nworkers = len(workers)
    queued = sum([_.count for _ in Queue.all(connection=auth.redis)])
    njobs = queued + auth.db.tasks.count_documents({"status": "pending"})
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
        refill_redis(100)
        last_update = update_stats(last_update)
        time.sleep(1)
