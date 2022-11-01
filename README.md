# dask-profiling

Profile dask [distributed](https://github.com/dask/distributed) clusters with [py-spy](https://github.com/benfred/py-spy).

```python
import dask
import distributed

from dask_profiling import pyspy

client = distributed.Client()

df = dask.datasets.timeseries(
    start="2000-01-01",
    end="2000-01-14",
    partition_freq="1h",
    freq="60s",
)

with pyspy("worker-profiles"):
    df.set_index("id").mean().compute()
```

Using `pyspy` or `pyspy_on_scheduler` attaches a profiler to the Python process, records a profile, and sends the file(s) back to the client.

By default, py-spy profiles are recorded in [speedscope](https://www.speedscope.app/) format.

`dask-profiling` (and, transitively, `py-spy`) must be installed in the environment where the scheduler is running.

`dask-profiling` tries hard to work out-of-the-box, but if your cluster is running inside Docker, or on macOS, you'll need to configure things so it's allowed to run. See the [privileges for py-spy](#privileges-for-py-spy) section.

## Installation

```
python -m pip install git+https://github.com/gjoseph92/dask-profiling.git@main
```

## Usage

The `pyspy` and `pyspy_on_scheduler` functions are context managers. Entering them starts py-spy on the workers / scheduler. Exiting them stops py-spy, sends the profile data back to the client, and writes it to disk.

```python
client = distributed.Client()

with pyspy_on_scheduler("scheduler-profile.json"):
    # Profile the scheduler.
    # Writes to the `scheduler-profile.json` file locally.
    x.compute()

with pyspy("worker-profiles"):
    # Most basic usage.
    # Writes a profile per worker to the `worker-profiles` directory locally.
    # Files are named by worker addresses.
    x.compute()

with pyspy("worker-profiles", native=True):
    # Collect stack traces from native extensions written in Cython, C or C++.
    # You should usually turn this on to get much richer profiling information.
    # However, only recommended when your cluster is running on Linux.
    x.compute()

with pyspy("worker-profiles", workers=2):
    # Only profile 2 workers (selected randomly)
    x.compute()

with pyspy("worker-profiles", workers=['tcp://10.0.1.2:4567', 'tcp://10.0.1.3:5678']):
    # Profile specific workers by specifying their addresses
    x.compute()

with pyspy("worker-profiles", format="flamegraph", gil=True, idle=False, nonblocking=True, extra_pyspy_args=["--foo", "bar"]):
    # Look, you can pass any arguments you want to `py-spy`!
    # Refer to the `py-spy` command reference for what these mean.
    # You don't usually need to do this though. We've picked good defaults for you.
    x.compute()

with pyspy("worker-profiles", log_level="info"):
    # Set py-spy's internal log level.
    # Useful if py-spy isn't behaving.
    # Refer to the ``env_logger`` crate for details:
    # https://docs.rs/env_logger/latest/env_logger/index.html#enabling-logging
    x.compute()
```

For more information, refer to the docstrings of the functions.

By default, profiles are recorded in speedscope format, so just drop them into https://www.speedscope.app to view them.

### Tips & tricks

This is a handy pattern:

```python
with pyspy("worker-profiles"):
    input("Press enter when done profiling")
    # or maybe:
    # time.sleep(10)
```

Ways you can use it:

#### Profiling a cluster that's already running

1. Open a second terminal/Jupyter session/etc.
1. In that session, connect to your existing cluster.
1. Run the block above. Press enter to stop profiling once you feel like you've got enough.

#### Profiling part of a longer computation

```python
persisted = my_thing.persist()

with pyspy("worker-profiles"):
    # Watch the dashboard, hit enter when you think you've got enough.
    input("Press enter when done profiling")

# optional, to get actual result:
persisted.compute()
del persisted
```

## Privileges for py-spy

**tl;dr:**
* On macOS clusters, you have to launch your cluster with `sudo`
* For `docker run`, pass `--cap-add SYS_PTRACE`, or download this newer [`seccomp.json`](https://github.com/moby/moby/blob/d39b075302c27f77b2de413697a5aacb034d8286/profiles/seccomp/default.json) file and use `--seccomp=default.json`.
* On Windows clusters, you're on your own, sorry.

You may need to run the dask process as root for py-spy to be able to profile it (especially on macOS). See https://github.com/benfred/py-spy#when-do-you-need-to-run-as-sudo.

In a Docker container, `dask-profiling` will "just work" for Docker/moby versions >= 21.xx. As of right now (Nov 2022), Docker 21.xx doesn't exist yet, so read on.

[moby/moby#42083](https://github.com/moby/moby/pull/42083/files) allowlisted by default the `process_vm_readv` system call that py-spy uses, which used to be blocked unless you set `--cap-add SYS_PTRACE`. Allowing this specific system call in unprivileged containers has been safe to do for a while (since linux kernel versions > 4.8), but just wasn't enabled in Docker. So your options right now are:
* (low/no security impact) Download the newer [`seccomp.json`](https://github.com/moby/moby/blob/d39b075302c27f77b2de413697a5aacb034d8286/profiles/seccomp/default.json) file from moby/master and pass it to Docker via `--seccomp=default.json`.
* (more convenient) Pass `--cap-add SYS_PTRACE` to Docker. This enables more than you need, but it's one less step.

On Ubuntu-based containers, ptrace system calls are [further blocked](https://www.kernel.org/doc/Documentation/admin-guide/LSM/Yama.rst): processes are prohibited from ptracing each other even within the same UID. To work around this, `dask-profiling` automatically uses [`prctl(2)`](https://man7.org/linux/man-pages/man2/prctl.2.html) to mark the scheduler process as ptrace-able by itself and any child processes, then launches py-spy as a child process.

## Caveats

* If you're running something that crashes your cluster, you probably won't be able to get a profile out of it. Transferring results back to the client relies on a stable connection and things in dask all working properly.
* Profiling slows things down. Especially if using `pyspy_on_scheduler`, expect noticeably slower results. This is probably not a thing you want to have always-on.
* This package is very much in development. I made it for my personal use and am sharing in case it's useful. Please don't be mad if it breaks.

## Development

Install [Poetry](https://python-poetry.org/docs/#installation). To create a virtual environment, install dev dependencies, and install the package for local development:

```
$ poetry install
```

There is one very very basic end-to-end test for py-spy. Running it requires Docker and docker-compose, though the building and running of the containers is managed by [pytest-docker-compose](https://github.com/pytest-docker-compose/pytest-docker-compose), so all you have to do is:

```
$ pytest tests
```
and wait a long time for the image to build and run.
