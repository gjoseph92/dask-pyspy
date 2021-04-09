# distributed-pyspy

Profile the dask [distributed](https://github.com/dask/distributed) scheduler with the [py-spy](https://github.com/benfred/py-spy) statistical profiler.

```python
import dask
import distributed
from distributed_pyspy import pyspy_on_scheduler


client = distributed.Client()

df = dask.datasets.timeseries(
    start="2000-01-01",
    end="2000-01-31",
    partition_freq="1h",
    freq="60s",
)

with pyspy_on_scheduler("profile.json"):
    df.set_index("id").mean().compute()

# Saves a speedscope profile to `profile.json` locally
```

Using `pyspy_on_scheduler` attaches py-spy to the scheduler process, records a profile, and sends the file back to the client.

By default, profiles are recorded in [speedscope](https://www.speedscope.app/) format.

`distributed-pyspy` (and, transitively, `py-spy`) must be installed in the environment where the scheduler is running.

## Installation

```
python -m pip install git+https://github.com/gjoseph92/distributed-pyspy.git
```

## Privileges

You may need to run the scheduler process as root for py-spy to be able to profile it (especially on macOS). See https://github.com/benfred/py-spy#when-do-you-need-to-run-as-sudo

In a container, even if the scheduler is running as root, you'll need the `SYS_PTRACE` capability: https://github.com/benfred/py-spy#how-do-i-run-py-spy-in-docker