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


def check_active_queues(last_run):
    """Ensures all relevant queues are in the active_queues collection."""
    # run frequency
    if time.time() - last_run < 20:
        return last_run

    pipeline = [
        {"$match": {"status": "pending"}},
        {"$project": {"queues": 1}},
        {"$unwind": "$queues"},
        {"$group": {"_id": "$queues"}},
    ]
    for queue in auth.db.tasks.aggregate(pipeline):
        auth.db.active_queues.update_one(
            {"queue": queue["_id"]}, {"$set": {"queue": queue["_id"]}}, upsert=True
        )
    return time.time()


def check_stale_jobs(last_run):
    """Ensures that all queued jobs are in redis, otherwise reset to pending.

    Idea: ask for queued, then for redis, then for queued again. Stale jobs
    remain in queued, but are not in redis. Reset them to pending."""
    # run frequency
    if time.time() - last_run < 120:
        return last_run

    # list of queued tasks
    q1 = set([task["id"] for task in auth.db.tasks.find({"status": "queued"})])
    time.sleep(5)

    # list of tasks in redis
    hmq2rq = {
        _[0].decode("ascii"): _[1].decode("ascii")
        for _ in auth.redis.hgetall("id2id").items()
    }
    redis = set(hmq2rq.keys())

    # remove failed ones so they get requeued
    to_be_removed = []
    for hmqtask in redis:
        try:
            j = Job.fetch(hmq2rq[hmqtask], connection=auth.redis)
        except rq.exceptions.NoSuchJobError:
            to_be_removed.append(hmqtask)
            try:
                auth.redis.hdel("id2id", hmqtask)
            except:
                continue
            continue
        if j.is_failed:
            j.delete()
            to_be_removed.append(hmqtask)
            try:
                auth.redis.hdel("id2id", hmqtask)
            except:
                continue
    for hmqtask in to_be_removed:
        redis.remove(hmqtask)
    time.sleep(1)

    # list of queued tasks again
    q2 = set([task["id"] for task in auth.db.tasks.find({"status": "queued"})])

    stale = (q1 & q2) - redis

    logentries = [{"event": "task/requeue", "id": _, "ts": time.time()} for _ in stale]
    if len(stale) > 0:
        auth.db.tasks.update_many(
            {"id": {"$in": list(stale)}, "status": "queued"},
            {"$set": {"status": "pending"}},
        )
        auth.db.logs.insert_many(logentries)

    # drop those which have been deleted but are still in the queue
    should_have_been_deleted = {
        "id": {"$in": list(redis)},
        "status": "deleted",
    }
    for task in auth.db.tasks.find(should_have_been_deleted):
        tid = task["id"]
        try:
            job = rq.job.Job.fetch(hmq2rq[tid], connection=auth.redis)
            job.cancel()
            job.delete()
        except rq.exceptions.NoSuchJobError:
            pass
        auth.redis.hdel("id2id", tid)

    return time.time()


def _cores_from_queue_name(queue_name: str):
    parts = queue_name.split(",")
    for part in parts:
        if part.startswith("nc-"):
            return int(part.split("-")[1])


def update_stats(last_update: float):
    now = time.time()
    unixminute_now = int(now / 60)
    unixminute_last = int(last_update / 60)

    if unixminute_now == unixminute_last:
        return last_update

    njobs = auth.db.tasks.count_documents({"status": {"$in": ["pending", "queued"]}})

    tasks_running = 0
    cores_used = 0
    cores_allocated = 0
    cores_available = 0
    for dc in auth.db.heartbeat.find():
        if now - dc["ts"] < 100:
            tasks_running += dc["running"]
            cores_used += dc["used"]
            cores_allocated += dc["allocated"]
            cores_available += dc["available"]

    auth.db.stats.insert_one(
        {
            "cores_allocated": cores_allocated,
            "cores_available": cores_available,
            "cores_used": cores_used,
            "tasks_queued": njobs,
            "tasks_running": tasks_running,
            "ts": unixminute_now,
        }
    )

    # track tags
    tagdetails = auth.db.tasks.aggregate(
        [
            {"$match": {"received": {"$gt": time.time() - 7 * 24 * 60 * 60}}},
            {
                "$project": {
                    "call": 0,
                    "id": 0,
                    "datacenters": 0,
                    "function": 0,
                    "inflight": 0,
                    "queues": 0,
                    "on_datacenter": 0,
                    "error": 0,
                    "result": 0,
                }
            },
            {
                "$group": {
                    "_id": {"tag": "$tag", "ncores": "$ncores", "status": "$status"},
                    "jobcount": {"$sum": 1},
                    "ncores": {"$first": "$ncores"},
                    "tag": {"$first": "$tag"},
                    "status": {"$first": "$status"},
                    "totalduration": {"$sum": "$duration"},
                    "received": {"$min": "$received"},
                    "updated": {"$max": "$done"},
                }
            },
        ]
    )

    tags = {}
    for line in tagdetails:
        tag = line["tag"]
        if line["updated"] is None:
            updated = line["received"]
        else:
            updated = line["updated"]
        if tag not in tags:
            tags[tag] = {
                "tag": tag,
                "queued": 0,
                "pending": 0,
                "completed": 0,
                "failed": 0,
                "deleted": 0,
                "computetime": 0,
                "received": line["received"],
                "updated": updated,
                "ts": now,
            }
        tags[tag]["received"] = min(tags[tag]["received"], line["received"])
        tags[tag]["updated"] = max(tags[tag]["updated"], updated)
        if line["status"] in [
            "pending",
            "queued",
            "deleted",
            "completed",
            "error",
        ]:
            status = line["status"]
            if status == "error":
                status = "failed"
            tags[tag][status] = line["jobcount"]
        else:
            continue
        tags[tag]["computetime"] += line["ncores"] * line["totalduration"]
    auth.db.stats_tags.insert_many([_ for _ in tags.values()])
    auth.db.stats_tags.delete_many({"ts": {"$lt": now}})
    return now


def flow_control():
    last_active_queues = 0
    last_update_stats = 0
    while True:
        try:
            last_active_queues = check_active_queues(last_active_queues)
        except:
            continue
        try:
            last_update_stats = update_stats(last_update_stats)
        except:
            continue

        time.sleep(3)
