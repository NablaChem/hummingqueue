import aiohttp
import os
import io
import tarfile
import requests as rq
import uuid
from typing import List, Iterable
import tqdm
import tqdm.asyncio
import asyncio
import aiobotocore.session
import multiprocessing as mp
import time
import collections
import minio

Connection = collections.namedtuple(
    "Connection",
    "user_token api_url project_token s3_access_key s3_secret_key s3_url s3_bucket",
)


def get_connection() -> Connection:
    """Extracts connection details from the machine settings

    Returns
    -------
    Connection
        Named tuple with connection details.
    """
    connectionstring = os.getenv("HUMMING_QUEUE")
    if connectionstring is None:
        print(
            "Please set up the connection string first in the environment variable HUMMING_QUEUE."
        )
        return None

    try:
        userid, rest = connectionstring.split("@")
        apiurl, project = rest.split("/")
    except:
        print("Invalid connection string: userid@apiurl/projectid")
        return None

    connectionstring = os.getenv("HUMMING_QUEUE_S3")
    if connectionstring is None:
        print(
            "Please set up the connection string first in the environment variable HUMMING_QUEUE_S3."
        )
        return None

    try:
        user, rest = connectionstring.split("@")
        s3accesskey, s3secretkey = user.split(":")
        s3url, s3bucket = rest.split("/")
    except:
        print("Invalid connection string: userid@apiurl/projectid")
        return None

    return Connection(
        userid, apiurl, project, s3accesskey, s3secretkey, s3url, s3bucket
    )


def get_s3_client(conn: Connection) -> minio.Minio:
    """Builds a S3 client object from connection information."""
    s3_client = minio.Minio(
        conn.s3url,
        access_key=conn.s3accesskey,
        secret_key=conn.s3secretkey,
        secure=conn.s3url.startswith("https"),
    )
    return s3_client


async def _async_object_stage(
    s3_client,
    directory: str,
    code: str,
    version: str,
    command: str,
    cores: int,
    memorymb: int,
    timeseconds: int,
):
    global counter
    global rank

    api_secret = internal.get_api_secret()
    if api_secret is None:
        counter[rank] += 2
        return {"error": "No API token configured", "directory": directory}

    if os.path.exists(f"{directory}/leruli.job"):
        counter[rank] += 2
        return {"error": "Directory already submitted.", "directory": directory}

    # TODO: detect failed S3 interactions
    bucket = str(uuid.uuid4())
    await s3_client.create_bucket(Bucket=bucket)
    counter[rank] += 1

    # in-memory tar file
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        tar.add(directory, arcname=os.path.basename("."))

        runscript = io.BytesIO(("#!/bin/bash\n" + " ".join(command)).encode("ascii"))
        tarinfo = tarfile.TarInfo(name="run.sh")
        tarinfo.size = runscript.getbuffer().nbytes
        tar.addfile(tarinfo=tarinfo, fileobj=runscript)
    buffer.seek(0)

    # upload
    await s3_client.put_object(Bucket=bucket, Key="run.tgz", Body=buffer)

    # submit to API
    codeversion = f"{code}:{version}"
    payload = {
        "secret": api_secret,
        "bucketid": bucket,
        "name": "default",
        "codeversion": codeversion,
        "cores": cores,
        "memorymb": memorymb,
        "timelimit": timeseconds,
        "directory": directory,
    }

    counter[rank] += 1
    return payload


def job_submit_many(
    directories: Iterable[str],
    codes: Iterable[str],
    versions: Iterable[str],
    commands: Iterable[str],
    cores: Iterable[int],
    memorymb: Iterable[int],
    timeseconds: Iterable[int],
) -> List[str]:
    """Submits many calculations to Leruli Queue/Cloud at once.

    Parameters
    ----------
    directories : Iterable[str]
        Paths to directories, either absolute or relative.
    codes : Iterable[str]
        Codes to run. Needs to be of same length as `directories`.
    versions : Iterable[str]
        Code versions to use. Needs to be of same length as `directories`.
    commands : Iterable[str]
        Commands to execute. Needs to be of same length as `directories`.
    cores : Iterable[int]
        Cores to use. Needs to be of same length as `directories`.
    memorymb : Iterable[int]
        Memory to use in MB. Needs to be of same length as `directories`.
    timeseconds : Iterable[int]
        Time limit for jobs in seconds. Needs to be of same length as `directories`.

    Returns
    -------
    dict[str]
        Keys: directories which could not be submitted, values: reasons for this to happen.
    """

    cases = list(
        zip(directories, codes, versions, commands, cores, memorymb, timeseconds)
    )

    def split(a, n):
        k, m = divmod(len(a), n)
        return (a[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n))

    # TODO find number of procs
    nprocs = 5
    segments = split(cases, nprocs)

    # assign one id to every worker
    idqueue = mp.Queue()
    [idqueue.put(_) for _ in range(nprocs)]
    progress_counter = mp.Array("i", nprocs, lock=False)

    # Uploading
    with tqdm.tqdm(total=len(cases), desc="Uploading job data") as pbar:
        with mp.Pool(
            nprocs,
            initializer=_job_submit_many_initializer,
            initargs=(idqueue, progress_counter),
        ) as p:
            result = p.map_async(_job_submit_many_toasync, segments)
            while True:
                total = sum([progress_counter[_] for _ in range(nprocs)])
                pbar.n = int(total / 2)
                pbar.refresh()
                try:
                    result.successful()
                except:
                    time.sleep(0.1)
                    continue
                result.wait()
                break

        # prepare for stage 2: submission to bulk API
        failed = {}
        cases = []
        for case in sum(result.get(), []):
            if "error" in case:
                failed[case["directory"]] = case["error"]
            else:
                payload = case.copy()
                del payload["directory"]
                cases.append((payload, case["directory"]))

    results = asyncio.run(_job_submit_many_API_toasync(cases))
    for result in results:
        failed.update(results)
    return failed


async def _job_submit_many_API_toasync(cases):
    # build parallel job list
    async with aiohttp.ClientSession() as session:
        jobs = []
        while len(cases) > 0:
            segment, cases = cases[:50], cases[50:]
            jobs.append(_job_submit_many_API(session, segment))

        # defer for async execution
        result = await tqdm.asyncio.tqdm.gather(*jobs, desc="Submitting job batches")

    return result


def _job_submit_many_toasync(cases):
    return asyncio.run(_job_submit_many_worker(cases))


async def _job_submit_many_API(session, segment):
    failed = {}
    async with session.post(
        f"{internal.BASEURL}/v22_1/bulk/job-submit", json=[_[0] for _ in segment]
    ) as res:
        results = await res.json()
        for case, result in zip(segment, results):
            payload, directory = case

            if result["status"] != 200:
                reason = "Job not accepted"
                if result["status"] == 403:
                    reason = "API key not accepted"
                if result["status"] == 412:
                    reason = "Compute secret not set-up"
                if result["status"] == 422:
                    reason = f"Job not accepted: {str(result)}"
                failed[directory] = reason
                continue

            jobid = result["data"]
            _job_submit_finalize(directory, jobid, payload["bucketid"])
    return failed


async def _job_submit_many_worker(cases):
    session = aiobotocore.session.get_session()
    jobs = []
    async with session.create_client(
        "s3",
        endpoint_url=os.getenv("LERULI_S3_SERVER"),
        aws_secret_access_key=os.getenv("LERULI_S3_SECRET"),
        aws_access_key_id=os.getenv("LERULI_S3_ACCESS"),
    ) as s3_client:
        for args in cases:
            jobs.append(_async_object_stage(s3_client, *args))
        cases = await asyncio.gather(*jobs)

    return cases


def _job_submit_many_initializer(idqueue, progress):
    global rank
    global counter
    rank = idqueue.get()
    counter = progress

    # TODO print missing API key only once, abort immediately


def job_submit(
    directory: str,
    container: str,
    command: str,
    cores: int = 1,
    memorymb: int = 4000,
    timeseconds: int = 24 * 60 * 60,
    tags: List[str] = [],
):
    """Submits a given directory content as job to the queue."""
    conn = get_connection()
    try:
        payload = _job_submit_payload(
            directory, container, command, cores, memorymb, timeseconds, tags
        )
    except ValueError as e:
        print(f"Failed: {str(e)}")
        return

    res = rq.post(f"{conn.apiurl}/v22_1/job/create", json=payload)
    if res.status_code != 200:
        print("Cannot submit job. Please check the input.")
        return
    jobid = res.json()

    _job_submit_finalize(directory, jobid, payload["datasource"])


def _job_submit_payload(
    directory: str,
    container: str,
    command: str,
    cores: int,
    memorymb: int,
    timeseconds: int,
    tags: List[str],
):
    conn = get_connection()
    s3_client = get_s3_client(conn)

    if os.path.exists(f"{directory}/hq.job"):
        raise ValueError("Directory already submitted.")

    bucketfolder = str(uuid.uuid4())

    # in-memory tar file
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        tar.add(directory, arcname=os.path.basename("."))
    buffer.seek(0)

    # upload
    s3_client.put_object(
        f"{conn.s3bucket}/{bucketfolder}",
        "input.tgz",
        buffer,
        buffer.getbuffer().nbytes,
    )

    # submit to API
    s3url = f"{conn.s3url}/{conn.s3bucket}/{bucketfolder}"
    payload = {
        "container": container,
        "command": command,
        "user_token": conn.userid,
        "projectid": conn.projectid,
        "data_source": s3url,
        "data_target": s3url,
        "core_limit": cores,
        "memory_mb_limit": memorymb,
        "time_seconds_limit": timeseconds,
        "tags": tags,
    }
    return payload


def _job_submit_finalize(directory, jobid, bucket):
    # local handle
    with open(f"{directory}/hq.job", "w") as fh:
        fh.write(f"{jobid}\n")
    with open(f"{directory}/hq.bucket", "w") as fh:
        fh.write(f"{bucket}\n")
    return jobid
