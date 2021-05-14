# scheduler-profiling

Profile the dask [distributed](https://github.com/dask/distributed) scheduler with [py-spy](https://github.com/benfred/py-spy) or [viztracer](https://github.com/gaogaotiantian/viztracer).

```python
import dask
import distributed

from scheduler_profiling import pyspy_on_scheduler, viztrace_scheduler

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
    viztrace_scheduler(
        "viztracer.json", trace_sparse="distributed.Scheduler.update_graph_hlg"
    ),
    # ^ Saves a Chrome trace to `viztracer.json` locally
):
    df.set_index("id").mean().compute()
```

Using `pyspy_on_scheduler` or `viztrace_scheduler` attaches a profiler to the scheduler process, records a profile, and sends the file back to the client.

By default, py-spy profiles are recorded in [speedscope](https://www.speedscope.app/) format. [Viztracer profiles](https://viztracer.readthedocs.io/en/latest/basic_usage.html#display-report) are recorded in Chrome trace format, viewable with [Perfetto](https://ui.perfetto.dev/) or <chrome://tracing>.

`distributed-pyspy` (and, transitively, `py-spy`/`viztracer`) must be installed in the environment where the scheduler is running.

## Installation

```
python -m pip install git+https://github.com/gjoseph92/distributed-pyspy.git
```

## Privileges for py-spy

You may need to run the scheduler process as root for py-spy to be able to profile it (especially on macOS). See https://github.com/benfred/py-spy#when-do-you-need-to-run-as-sudo.

In a Docker container, `distributed-pyspy` will "just work" for Docker/moby versions >= 21.xx. As of right now (May 2021), Docker 21.xx doesn't exist yet, so read on.

[moby/moby#42083](https://github.com/moby/moby/pull/42083/files) recently allowlisted the `process_vm_readv` system call that py-spy uses, which used to be blocked unless you set `--cap-add SYS_PTRACE`. This has been reasonable/safe to do for a while, but just wasn't enabled. So your options right now are:
* (low/no security impact) Download the newer [`seccomp.json`](https://github.com/clubby789/moby/blob/d39b075302c27f77b2de413697a5aacb034d8286/profiles/seccomp/default.json) file from moby/master and pass it to Docker via `--seccomp=default.json`.
* (more convenient) Pass `--cap-add SYS_PTRACE` to Docker. This enables more than you need, but it's one less step.

On Ubuntu-based containers, ptrace system calls are [further blocked](https://www.kernel.org/doc/Documentation/admin-guide/LSM/Yama.rst): processes are prohibited from ptracing each other even within the same UID. To work around this, `distributed-pyspy` automatically uses [`prctl(2)`](https://man7.org/linux/man-pages/man2/prctl.2.html) to mark the scheduler process as ptrace-able by itself and any child processes, then launches py-spy as a child process.

## Viztracer targeting with `trace_sparse`

`viztrace_scheduler` adds a non-standard option to VizTracer: the `trace_sparse` parameter. With this, you basically give a passlist of names (as strings) of functions to trace. The tracer will only run while those functions are active; at all other times, it'll be paused. Names are given in the same way you would to `unittest.mock.patch`.

`viztrace_scheduler` adds some magic so that if the function you want to trace happens to be registered as a comm handler on the Scheduler, everything will behave as you'd expect. (Since the handler functions are added into `scheduler.handlers` _before_ `viztrace_scheduler` has the chance to patch them, those dicts need to be explicitly updated to hold the patched handlers as well. `viztrace_scheduler` takes care of this for you.)

Examples:

* `trace_sparse="distributed.Scheduler.handle_task_finished"` - Trace only the `handle_task_finished` function, and everything it calls while running
* `trace_sparse=["dask.highlevelgraph.HighLevelGraph.__dask_distributed_unpack__", "distributed.Scheduler.handle_task_finished"]` - Trace both high-level-graph unpacking and `handle_task_finished`

Note that this doesn't work very well on async functions. `distributed.comm.tcp.TCP.read` doesn't work that well, for example.

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

There are currently no tests for viztracer.
