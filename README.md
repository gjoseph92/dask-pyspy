# dask-profiling

Profile dask [distributed](https://github.com/dask/distributed) clusters with [py-spy](https://github.com/benfred/py-spy).

```python
import dask
import distributed

from dask_profiling import pyspy_on_scheduler

client = distributed.Client()

df = dask.datasets.timeseries(
    start="2000-01-01",
    end="2000-01-14",
    partition_freq="1h",
    freq="60s",
)

with (
    pyspy_on_scheduler("pyspy.json"),
    # ^ Saves a speedscope profile to `pyspy.json` locally
):
    df.set_index("id").mean().compute()
```

Using `pyspy_on_scheduler` attaches a profiler to the scheduler process, records a profile, and sends the file back to the client.

By default, py-spy profiles are recorded in [speedscope](https://www.speedscope.app/) format.

`dask-profiling` (and, transitively, `py-spy`) must be installed in the environment where the scheduler is running.

## Installation

```
python -m pip install git+https://github.com/gjoseph92/dask-profiling.git@main
```

## Privileges for py-spy

**tl;dr:**
* On macOS, you have to launch your cluster with `sudo`
* For `docker run`, pass `--cap-add SYS_PTRACE`, or download this newer [`seccomp.json`](https://github.com/moby/moby/blob/d39b075302c27f77b2de413697a5aacb034d8286/profiles/seccomp/default.json) file and use `--seccomp=default.json`.

You may need to run the dask process as root for py-spy to be able to profile it (especially on macOS). See https://github.com/benfred/py-spy#when-do-you-need-to-run-as-sudo.

In a Docker container, `dask-profiling` will "just work" for Docker/moby versions >= 21.xx. As of right now (Nov 2022), Docker 21.xx doesn't exist yet, so read on.

[moby/moby#42083](https://github.com/moby/moby/pull/42083/files) recently allowlisted the `process_vm_readv` system call that py-spy uses, which used to be blocked unless you set `--cap-add SYS_PTRACE`. Allowing this specific system call in unprivileged containers has been safe to do for a while (since linux kernel versions > 4.8), but just wasn't enabled in Docker. So your options right now are:
* (low/no security impact) Download the newer [`seccomp.json`](https://github.com/moby/moby/blob/d39b075302c27f77b2de413697a5aacb034d8286/profiles/seccomp/default.json) file from moby/master and pass it to Docker via `--seccomp=default.json`.
* (more convenient) Pass `--cap-add SYS_PTRACE` to Docker. This enables more than you need, but it's one less step.

On Ubuntu-based containers, ptrace system calls are [further blocked](https://www.kernel.org/doc/Documentation/admin-guide/LSM/Yama.rst): processes are prohibited from ptracing each other even within the same UID. To work around this, `dask-profiling` automatically uses [`prctl(2)`](https://man7.org/linux/man-pages/man2/prctl.2.html) to mark the scheduler process as ptrace-able by itself and any child processes, then launches py-spy as a child process.

## Development

Install [Poetry](https://python-poetry.org/docs/#installation). To create a virtual environment, install dev dependencies, and install the package for local development:

```
$ poetry install -E test
```

There is one very very basic end-to-end test for py-spy. Running it requires Docker and docker-compose, though the building and running of the containers is managed by [pytest-docker-compose](https://github.com/pytest-docker-compose/pytest-docker-compose), so all you have to do is:

```
$ pytest tests
```
and wait a long time for the image to build and run.
