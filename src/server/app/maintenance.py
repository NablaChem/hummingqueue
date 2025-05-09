import time
from . import auth
import asyncio


async def check_active_queues(last_run):
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
    async for queue in auth.db.tasks.aggregate(pipeline):
        await auth.db.active_queues.update_one(
            {"queue": queue["_id"]}, {"$set": {"queue": queue["_id"]}}, upsert=True
        )
    return time.time()


async def update_stats(last_update: float):
    now = time.time()
    unixminute_now = int(now / 60)
    unixminute_last = int(last_update / 60)

    if unixminute_now == unixminute_last:
        return last_update

    njobs = await auth.db.tasks.count_documents({"status": {"$in": ["pending", "queued"]}})

    tasks_running = 0
    cores_used = 0
    cores_allocated = 0
    cores_available = 0
    async for dc in auth.db.heartbeat.find():
        if now - dc["ts"] < 100:
            tasks_running += dc["running"]
            cores_used += dc["used"]
            cores_allocated += dc["allocated"]
            cores_available += dc["available"]

    await auth.db.stats.insert_one(
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
    # identify tags with have work open or have been active in the last week
    # there might be individual tasks which are older for a long-running tag
    activetags_pending = auth.db.tasks.aggregate(
        [
            {"$match": {"status": {"$in": ["pending", "queued"]}}},
            {"$group": {"_id": {"tag": "$tag"}, "tag": {"$first": "$tag"}}},
        ]
    )
    activetags_recent = auth.db.tasks.aggregate(
        [
            {"$match": {"received": {"$gt": time.time() - 7 * 24 * 60 * 60}}},
            {"$group": {"_id": {"tag": "$tag"}, "tag": {"$first": "$tag"}}},
        ]
    )
    activetags_pending = [tag["_id"]["tag"] async for tag in activetags_pending]
    activetags_recent = [tag["_id"]["tag"] async for tag in activetags_recent]
    activetags = list(set(activetags_pending + activetags_recent))

    # build stats for all such tags
    tagdetails = auth.db.tasks.aggregate(
        [
            {"$match": {"tag": {"$in": activetags}}},
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
    async for line in tagdetails:
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
    if len(tags) > 0:
        await auth.db.stats_tags.insert_many([_ for _ in tags.values()])
    await auth.db.stats_tags.delete_many({"ts": {"$lt": now}})
    return now


async def flow_control():
    last_active_queues = 0
    last_update_stats = 0
    while True:
        try:
            last_active_queues = await check_active_queues(last_active_queues)
        except:
            continue
        try:
            last_update_stats = await update_stats(last_update_stats)
        except:
            continue

        await asyncio.sleep(3)
