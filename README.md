*hummingqueue* helps running millions of calculations at scale. It allows to dispatch python functions to a compute cluster without direct access to the internet. All function calls are signed and encrypted at rest to ensure confidential and secure access to compute power. A core aspect is to allow to span multiple data centers / (university) compute clusters.

Example use:
```python
import hmq

# just add the decorator to an existing function
@hmq.task
def costly_calculation(arg1, arg2):
    ...
    return result

# call the function as if it would be run locally
# all calls return immediately and just record the arguments
for case in big_dataset:
    costly_calculation(*case)

# submit to queue
tag = costly_calculation.submit()

# wait
while True:
    remaining_jobs_in_queue = tag.pull()
    if remaining_jobs_in_queue == 0:
        break
    time.sleep(10)

# access results or errors
tag.results, tag.errors
```

Still in early development.
