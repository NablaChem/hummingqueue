# Getting Started

The core idea of the queue is to dispatch arbitrary python functions to be evaluated remotely on compute nodes which cannot be contacted directly such as university compute clusters. Since this means sending code to powerful hardware which is safeguarded for good reason, we sign code such that the compute nodes can verify that this particular call has been issued by an authorized user. This requires some setup.

## Installation

The code base is identical for any user regardless where the calculations will be run. You can install the current version using

```
pip install hmq
```

## Authorization

As a first time user, you need to obtain permission by an instance admin to run code on that particular instance of *hummingqueue*. Since the clusters only accept code that is signed digitally, we need to generate some keys first. To do so, run this in the command line:

```
hmq request_access URL
```

where URL points to the instance you want to obtain access to. This will print a string like this one:

```
ADD-USER_1pFDZAen521Nr8atwL/Vi2D7EYKj0St/y6w+fI+l7fE=_55vQFPg8tm4QPcG239/8CRnAxJgzm0IrodIEmvYnwSA=
```

which contains a [signing request](https://en.wikipedia.org/wiki/Certificate_signing_request). It does not contain your encryption keys which are not to be shared with anyone (including admins). Now pass this string to the admin who will run the following command on their machine:

```
hmq grant_access SIGNING_REQUEST USERNAME
```

which signs the request and grants you permission to use the instance in question. You are now ready to submit code.

## Run code remotely

If you have written code that you want to run in parallel on a compute cluster, now first load the module `import hmq` and use the decorator `hmq.task` to enable a function to be executed remotely:

```
import hmq

# just add the decorator to an existing function
@hmq.task
def costly_calculation(arg1, arg2):
    ...
    return result
```

If you need any external libraries, you need to inline the imports:

```
@hmq.task
def costly_calculation(arg1, arg2):
    import somemodule
    ...
    return result
```

Since modules are cached, this carries no performance cost. Now if you call this function, the call returns immediately and just records the arguments with which it has been called. This ensures you can just reuse existing code logic with minimal changes. 

```
for case in big_dataset:
    costly_calculation(*case)
```

So far, nothing has been sent. Only once you call `.submit()`, all arguments are send to the cluster.

```
tag = costly_calculation.submit()
```

The `tag` is a handle to that group of calculations. You can fetch results from the remote cluster using `tag.pull()` which also returns the total number of outstanding function calls.

```
while True:
    remaining_jobs_in_queue = tag.pull()
    if remaining_jobs_in_queue == 0:
        break
    time.sleep(10)
```

Even while some calculations are still running remotely, you can access all completed calculations using `tag.results`. The order is equivalent to the order of initial function calls. Failed runs hold the error message under `tag.errors`. Note that if you loose access to the task ids managed by `tag`, there is no way to obtain the results of the calculations. If the run takes a long time, you may use `tag.to_file()` and `tag.from_file()` to persist results and task ids.

## Best practices

All functions should be expensive enough to take at least a few seconds, since otherwise the overhead of the remote dispatch and delegation is too high in comparison. Functions should not take longer than about an hour, since compute hardware is often times only reservable for some time, so few jobs will fail because the time limit has been reached.

The queue is tested with submitting hundreds of thousands of calls at the same time.
