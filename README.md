*madiler* helps running millions of calculations at scale. Coming from the HPC world, it aims at a scalable subset of SLURM. While common schedulers such as airflow or Luigi emphasize workflows, *madiler* emphasizes scale.

Core features:
- [ ] Submit millions of jobs easily.
- [ ] Share resources of different owners within a collaboration.
- [ ] Add and remove workers easily, even partially.

Terminology:
- Job: One compute problem of at least 1 minute runtime. Needs to specify a time *estimate*, and memory and core requirements.
- Node: One compute node.
- Worker: Controlling job execution on a node.
- Owner: Legal entity responsible for nodes. 
- User: Employee of owner.
- Project: A collaboration of several owners. Can be joined and left by owners.
- Tag: Identifies a group of jobs in a project.
- Owners can submit to their own hardware or to a project.

Infrastructure:
- Jobs get enqueued and may return either a directory or a 256B result JSON. JSON is stored in the metadata of a job, e.g. MongoDB. The directory is tar'd and stored in S3.
- Each job runs a container. The S3 URL of it is part of the specs.

Self-host:
- Install S3 locally, run worker everywhere.
- Optionally: Run own instance of madiler, otherwise use public one.
