import subprocess
import click
import time
import socket
import random
import psutil
import shlex
import collections
import requests as rq

Job = collections.namedtuple("Job", "jobid cores memory_mb handle")


class Client:
    def __init__(
        self,
        url: str,
        ownerid: str,
        computesecret: str,
        hostname: str,
        reserve_cores: int,
        reserve_memory_mb: int,
    ):
        self._baseurl = url
        self._ownerid = ownerid
        self._computesecret = computesecret
        self._nodeid = hostname
        self._reserve_cores = reserve_cores
        self._reserve_memory_mb = reserve_memory_mb
        self._jobs = []

    def _post(self, endpoint, payload):
        res = rq.post(f"{self._baseurl}/{endpoint}", json=payload)
        if res.status_code != 200:
            raise ValueError()
        return res.json()

    def _get_nodeauth(self):
        return {
            "nodeid": self._nodeid,
            "ownerid": self._ownerid,
            "computesecret": self._computesecret,
        }

    def heartbeat(self):
        """Tell API node is still around."""
        try:
            self._post(
                "heartbeat",
                self._get_nodeauth(),
            )
        except:
            print("Cannot contact queue. Network failure? Skipping heartbeat.")

    def _available_resources(self):
        # memory
        physical_memory_mb = psutil.virtual_memory().total / 1024 / 1024
        allocated_memory_mb = sum([_.memory_mb for _ in self._jobs])
        available_memory_mb = (
            physical_memory_mb - allocated_memory_mb - self._reserve_memory_mb
        )

        # cores
        physical_cores = psutil.cpu_count()
        allocated_cores = sum([_.cores for _ in self._jobs])
        available_cores = physical_cores - allocated_cores - self._reserve_cores

        return available_cores, available_memory_mb

    def _upload_results(self, jobid):
        self._jobs = [_ for _ in self._jobs if _.jobid != jobid]

    def has_capacity(self):
        # check for completed jobs
        for job in self._jobs:
            status = job.handle.poll()
            if status is not None:
                self._upload_results(job.jobid)
                print(f"{job.jobid} finished with exit code {status}")

        # fetch current resources
        available_cores, available_memory_mb = self._available_resources()
        return available_cores > 0 and available_memory_mb > 0

    def _job_to_command(self, jobdesc):
        return f"sleep {random.randint(5, 15)}"

    def fetch_new_tasks(self):
        available_cores, available_memory_mb = self._available_resources()
        payload = self._get_nodeauth()
        payload["cores"] = available_cores
        payload["memory_mb"] = available_memory_mb
        payload["in_cache"] = ["foo:1.2", "bar:1.1"]

        jobs = self._post("job/fetch", payload)
        print(f"Got {len(jobs)} new jobs for {payload['cores']} offered cores.")
        for jobid, jobdesc in jobs.items():
            command = self._job_to_command(jobdesc)
            p = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            self._jobs.append(
                Job(jobid, jobdesc.core_limit, jobdesc.memory_mb_limit, p)
            )

    def run(self):
        while True:
            while True:
                self.heartbeat()
                if self.has_capacity():
                    break
                time.sleep(2)
            self.fetch_new_tasks()


@click.command()
@click.argument("URL")
@click.argument("ownerid")
@click.argument("computesecret")
@click.option(
    "--hostname",
    default=socket.getfqdn(),
    help="Custom string identifying the node, defaults to the FQDN. Needs to be unique amongst all nodes.",
)
@click.option(
    "--reserve-cores",
    default=0,
    help="Number of cores to reserve for other use of this node.",
)
@click.option(
    "--reserve-memory-mb",
    default=0,
    help="Amount of memory in MB to reserve for other use of this node.",
)
def main(url, ownerid, computesecret, hostname, reserve_cores, reserve_memory_mb):
    """Runs compute jobs on this node.

    The jobs are read from the queue at URL for the machine owner OWNERID authenticated by the COMPUTESECRET."""
    c = Client(url, ownerid, computesecret, hostname, reserve_cores, reserve_memory_mb)
    c.run()


if __name__ == "__main__":
    main()
