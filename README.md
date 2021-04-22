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

You may need to run the scheduler process as root for py-spy to be able to profile it (especially on macOS). See https://github.com/benfred/py-spy#when-do-you-need-to-run-as-sudo.

In a Docker container, `distributed-pyspy` should "just work" as long as your Docker/moby version is >= 20.10.6.

Why that version? [moby/moby#42083](https://github.com/moby/moby/pull/42083/files) recently allowlisted the `process_vm_readv` system call that py-spy uses, which used to be blocked unless you set `--cap-add SYS_PTRACE`. If you're stuck on a very slightly older version of Docker, you _could_ use the [`seccomp.json`](https://github.com/clubby789/moby/blob/d39b075302c27f77b2de413697a5aacb034d8286/profiles/seccomp/default.json) file from 20.10.6 via `--seccomp=moby-20.10.6-seccomp.json`, if you happen to be reluctant to `--cap-add SYS_PTRACE`.

On Ubuntu-based containers, ptrace system calls are [further blocked](https://www.kernel.org/doc/Documentation/admin-guide/LSM/Yama.rst): processes are prohibited from ptracing each other even within the same UID. To work around this, `distributed-pyspy` automatically uses [`prctl(2)`](https://man7.org/linux/man-pages/man2/prctl.2.html) to mark the scheduler process as ptrace-able by itself and any child processes, then launches py-spy as a child process.

If you're using an older Docker version, then https://github.com/benfred/py-spy#how-do-i-run-py-spy-in-docker still applies.

## Development

Install [Poetry](https://python-poetry.org/docs/#installation). To create a virtual environment, install dev dependencies, and install the package for local development:

```
$ poetry install -E test
```

There is one very very basic end-to-end test. Running it requires Docker and docker-compose, though the building and running of the containers is managed by [pytest-docker-compose](https://github.com/pytest-docker-compose/pytest-docker-compose), so all you have to do is:

```
$ pytest tests
```
and wait a long time for the image to build and run.
