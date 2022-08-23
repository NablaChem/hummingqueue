import click
import sys
from . import job
import glob
import time as modtime
from typing import List

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group()
def group_job_submit():
    pass


HUMMING_QUEUE = "user-311e6015-b0fb-4ad0-a5b6-54ba1addec37@api.lvh.me/project-0415bf24-f25b-43e3-8404-75d92e7410e7"
HUMMING_QUEUE_S3 = "username@s3.lvh.me/bucketname"


@group_job_submit.command()
@click.option("--memory", default=4000, help="Memory limit in MB.")
@click.option("--time", default=60 * 24, help="Time limit in minutes.")
@click.option("--cores", default=1, help="Number of cores to allocate.")
@click.option("--tags", multiple=True, default=[], help="Tags to assign.")
@click.option(
    "--batch",
    nargs=1,
    help="Submit multiple directories at once, specified as either a string of a globbing pattern (e.g., case-?/run-*/) or a file with a line-by-line list of directories.",
)
@click.argument("container")
@click.argument("command", nargs=-1, required=True)
def job_submit(
    memory: int,
    time: int,
    cores: int,
    batch: str,
    container: str,
    command: str,
    tags: List[str],
):
    """Submit one or many jobs to the queue.

    CONTAINER is the name of a container with all code and dependencies as uploaded to the queue storage before.
    COMMAND is the command that should be run on the commandline once the directory content of the current directory
    or those specified with --batch has become available on the compute node."""

    if batch is None:
        try:
            jobid = job.job_submit(
                ".", container, command, cores, memory, time * 60, tags
            )
        except ValueError as e:
            print(f"Not submitted: {str(e)}")
            sys.exit(1)
        if jobid is not None:
            print(jobid)
        else:
            sys.exit(1)
    else:
        try:
            with open(batch) as fh:
                directories = []
                for line in fh.readlines():
                    line = line.strip()
                    if len(line) > 0:
                        directories.append(line)
        except:
            directories = glob.glob(batch)

        njobs = len(directories)
        starttime = modtime.time()
        failed = job.job_submit_many(
            directories,
            [container] * njobs,
            [command] * njobs,
            [cores] * njobs,
            [memory] * njobs,
            [time * 60] * njobs,
            [tags] * njobs,
        )
        stoptime = modtime.time()
        for directory in sorted(failed.keys()):
            print(f"Not submitted: {directory} - {failed[directory]}")
        duration = stoptime - starttime
        print(f"Submitted {njobs} in {duration:1.1f}s ({njobs/duration:1.0f}/s)")
        if len(failed) > 0:
            sys.exit(1)


cli = click.CommandCollection(
    sources=[
        group_job_submit,
    ],
    context_settings=CONTEXT_SETTINGS,
)


if __name__ == "__main__":
    sys.exit(cli())
