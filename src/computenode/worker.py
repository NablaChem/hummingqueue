from rq import Worker
import hmq


class CachedWorker(Worker):
    def execute_job(self, job, queue):
        hmq.api.warm_cache(job.kwargs["function"])
        return super().execute_job(job, queue)
