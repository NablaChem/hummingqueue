from rq import Queue, Worker
import time
from . import auth


def refill_redis(njobs: int):
    # get currently active data centers
    active_datacenters = {}
    for dc in auth.db.datacenters.find():
        if time.time() - dc["ts"] < 100:
            active_datacenters[dc["datacenter"]] = dc["packages"]

    # build fictitious "any" datacenter
    any_packages = {}
    python_versions = set(
        sum([list(_.keys()) for _ in active_datacenters.values()], [])
    )
    for pyver in python_versions:
        packages = []
        for dc in active_datacenters:
            if pyver in active_datacenters[dc]:
                packages.append(active_datacenters[dc][pyver])

        any_packages[pyver] = set(packages[0])
        for package in packages[1:]:
            any_packages[pyver] = any_packages[pyver].intersection(set(package))
    active_datacenters["any"] = any_packages

    # extend queue
    for queue in auth.db.active_queues.find():
        tasks = []
        logentries = []

        name = queue["queue"]
        q = Queue(name, connection=auth.redis)

        seen_at_least_one = False
        remaining = njobs - len(q)
        for _ in range(remaning):
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

        if remaining > 0 and not seen_at_least_one:
            auth.db.active_queues.delete_one({"queue": name})

        if len(logentries) > 0:
            auth.db.logs.insert_many(logentries)
            idmapping = {}
            for task in tasks:
                job = q.enqueue("hmq.unwrap", **task)
                idmapping[task["hmqid"]] = job.id
            auth.redis.hset("id2id", mapping=idmapping)


def check_active_queues(last_run):
    """Ensures all relevant queues are in the active_queues collection."""
    # run frequency
    if time.time() - last_run < 120:
        return last_run

    pipeline = [
        {"$project": {"queues": 1}},
        {"$unwind": "$queues"},
        {"$group": {"_id": "$queues"}},
    ]
    for queue in auth.db.tasks.aggregate(pipeline):
        auth.db.active_queues.update_one(
            {"queue": queue["_id"]}, {"queue": queue["_id"]}, upsert=True
        )
    return time.time()


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
    last_active_queues = 0
    while True:
        refill_redis(500)
        try:
            last_update = update_stats(last_update)
        except:
            continue
        try:
            last_active_queues = check_active_queues(last_active_queues)
        except:
            continue
        time.sleep(1)
