import asyncio
import logging
import os
import random
import signal
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Union, cast

import distributed

from .prctl import allow_ptrace

logger = logging.getLogger(__name__)


class PySpyer:
    def __init__(
        self,
        output: Optional[str] = None,
        format: str = "speedscope",
        rate: int = 100,
        subprocesses: bool = True,
        function: bool = False,
        gil: bool = False,
        threads: bool = False,
        idle: bool = True,
        nonblocking: bool = False,
        native: bool = False,
        extra_pyspy_args: Iterable[str] = (),
        log_level: Optional[str] = None,
    ) -> None:
        self.output = output
        self.log_level = log_level
        self.pyspy_args: List[str] = ["--format", format, "--rate", str(rate)] + [
            flag
            for flag, active in {
                "--subprocesses": subprocesses,
                "--function": function,
                "--gil": gil,
                "--threads": threads,
                "--idle": idle,
                "--nonblocking": nonblocking,
                "--native": native,
            }.items()
            if active
        ]
        self.pyspy_args.extend(extra_pyspy_args)
        self.proc = None
        self._tempfile = None
        self._run_failed_msg = None

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.pyspy_args}>"

    async def start(self) -> None:
        if self.output is None:
            self._tempfile = tempfile.NamedTemporaryFile(suffix="pyspy.json")
            self.output = self._tempfile.name

        pid = os.getpid()
        try:
            # Allow subprocesses of this process to ptrace it.
            # Since we'll start py-spy as a subprocess, it will be below the current PID
            # in the process tree, and therefore allowed to trace its parent.
            allow_ptrace(pid)
        except OSError as e:
            self._run_failed_msg = str(e)
        else:
            self._run_failed_msg = None

        self.proc = await asyncio.create_subprocess_exec(
            "py-spy",
            "record",
            "--pid",
            str(pid),
            "--output",
            self.output,
            *self.pyspy_args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=(
                None
                if self.log_level is None
                else dict(os.environ, RUST_LOG=self.log_level)
            ),
        )
        # check if it terminated immediately
        await asyncio.sleep(0.2)
        if self.proc.returncode is not None:
            await self._handle_process_done()

    async def _stop(self) -> None:
        if self.proc is None:
            raise RuntimeError(
                "No py-spy subprocess found. Either `get_profile()` was "
                "already called, or the process never started successfully."
            )

        try:
            self.proc.send_signal(signal.SIGINT)
        except ProcessLookupError:
            msg = f"py-spy subprocess {self.proc.pid} already terminated (it probably never ran?)."
            self._run_failed_msg = (
                msg
                if not self._run_failed_msg
                else f"{msg}\nNOTE:{self._run_failed_msg}"
            )
            logger.warning(msg)

        await self._handle_process_done()

    async def _handle_process_done(self) -> None:
        stdout, stderr = await self.proc.communicate()  # TODO timeout
        retcode = self.proc.returncode
        error: Optional[Exception] = None
        if retcode != 0:
            msgs = [
                f"py-spy exited with code {retcode}",
                f"py-spy stderr:\n{stderr.decode()}",
                f"py-spy stdout:\n{stdout.decode()}",
            ]
            if self._run_failed_msg:
                msgs.insert(0, self._run_failed_msg)
            for msg in msgs:
                logging.warn(msg)
            error = RuntimeError("\n".join(msgs))

        self.proc = None
        if error:
            raise error

    def _maybe_close_tempfile(self):
        if self._tempfile is not None:
            self._tempfile.close()
        self._tempfile = None

    async def get_profile(self) -> bytes:
        try:
            await self._stop()
        except RuntimeError:
            raise
        else:
            assert self.output is not None
            with open(self.output, "rb") as f:
                return f.read()  # TODO streaming!
        finally:
            self._maybe_close_tempfile()

    # async def close(self):
    #     try:
    #         await self._stop()
    #     except RuntimeError:
    #         pass
    #     finally:
    #         self._maybe_close_tempfile()


@contextmanager
def pyspy_on_scheduler(
    output: Union[str, os.PathLike],
    format: str = "speedscope",
    rate: int = 100,
    subprocesses: bool = True,
    function: bool = False,
    gil: bool = False,
    threads: bool = False,
    idle: bool = True,
    nonblocking: bool = False,
    native: bool = False,
    extra_pyspy_args: Iterable[str] = (),
    log_level: Optional[str] = None,
    client: Optional[distributed.Client] = None,
):
    """
    Spy on the Scheduler with py-spy.

    Use as a context manager (similar to `distributed.performance_report`) to record a py-spy
    profile of the scheduler.

    When the context manager exits, the profile is sent back to the client and saved to
    the ``output`` path.

    Parameters
    ----------
    output:
        *Local* path to save the profile to, once it's sent back from the scheduler.
    format:
        Output file format [default: flamegraph]  [possible values: flamegraph, raw, speedscope]
    rate:
        The number of samples to collect per second [default: 100]
    subprocesses:
        Profile subprocesses of the original process
    function:
        Aggregate samples by function name instead of by line number
    gil:
        Only include traces that are holding on to the GIL
    threads:
        Show thread ids in the output
    idle:
        Include stack traces for idle threads
    nonblocking:
        Don't pause the python process when collecting samples. Setting this option
        will reduce the performance impact of sampling, but may lead to inaccurate results
    native:
        Collect stack traces from native extensions written in Cython, C or C++
    extra_pyspy_args:
        Iterable of any extra arguments to pass to ``py-spy``.
    log_level:
        The log level for ``py-spy`` (useful for debugging py-spy issues).
        If None (default), the defaults are unchanged (only error-level logs).
        Typically a string like ``"warn"``, ``"info"``, etc, which is set as the ``RUST_LOG``
        environment variable. See documentation of the ``env_logger`` crate for details:
        https://docs.rs/env_logger/latest/env_logger/index.html#enabling-logging.
    client:
        The distributed Client to use. If None (default), the default client is used.
    """
    client = client or distributed.worker.get_client()
    assert client

    # make the directory if necessary
    path = Path(output).resolve(strict=False)
    path.parent.mkdir(parents=True, exist_ok=True)

    async def _inject_pyspy(
        dask_scheduler: distributed.Scheduler,
    ) -> None:
        if hasattr(dask_scheduler, "_pyspy"):
            raise RuntimeError("py-spy is already running on this worker!")
        plugin = PySpyer(
            output=None,
            format=format,
            rate=rate,
            subprocesses=subprocesses,
            function=function,
            gil=gil,
            threads=threads,
            idle=idle,
            nonblocking=nonblocking,
            native=native,
            extra_pyspy_args=extra_pyspy_args,
            log_level=log_level,
        )
        await plugin.start()
        dask_scheduler._pyspy = plugin

    client.run_on_scheduler(_inject_pyspy)
    try:
        yield
    finally:

        async def _get_profile(dask_scheduler: distributed.Scheduler):
            try:
                return await dask_scheduler._pyspy.get_profile()
            finally:
                del dask_scheduler._pyspy

        data = client.run_on_scheduler(_get_profile)
        data = cast(bytes, data)
        with open(path, "wb") as f:
            f.write(data)


@contextmanager
def pyspy(
    output: Union[str, os.PathLike],
    workers: Optional[Union[int, Sequence[str]]] = None,
    format: str = "speedscope",
    rate: int = 100,
    subprocesses: bool = True,
    function: bool = False,
    gil: bool = False,
    threads: bool = False,
    idle: bool = True,
    nonblocking: bool = False,
    native: bool = False,
    extra_pyspy_args: Iterable[str] = (),
    log_level: Optional[str] = None,
    client: Optional[distributed.Client] = None,
):
    """
    Spy on dask workers with py-spy.

    Use as a context manager (similar to `distributed.performance_report`) to record a py-spy
    profile on each worker.

    When the context manager exits, the profiles are sent back to the client and saved to
    the ``output`` directory.

    Parameters
    ----------
    output:
        *Local* directory to save the profiles to, once they're sent back from the scheduler.
        A separate file will be created for each worker, named for that worker's address.
    workers:
        Workers to profile. If None (default), py-spy runs on all workers. Pass an int to randomly
        select that many workers, or a sequence of worker addresses (strings) to run on specific workers.
    format:
        Output file format [default: flamegraph]  [possible values: flamegraph, raw, speedscope]
    rate:
        The number of samples to collect per second [default: 100]
    subprocesses:
        Profile subprocesses of the original process
    function:
        Aggregate samples by function name instead of by line number
    gil:
        Only include traces that are holding on to the GIL
    threads:
        Show thread ids in the output
    idle:
        Include stack traces for idle threads
    nonblocking:
        Don't pause the python process when collecting samples. Setting this option
        will reduce the performance impact of sampling, but may lead to inaccurate results
    native:
        Collect stack traces from native extensions written in Cython, C or C++
    extra_pyspy_args:
        Iterable of any extra arguments to pass to ``py-spy``.
    log_level:
        The log level for ``py-spy`` (useful for debugging py-spy issues).
        If None (default), the defaults are unchanged (only error-level logs).
        Typically a string like ``"warn"``, ``"info"``, etc, which is set as the ``RUST_LOG``
        environment variable. See documentation of the ``env_logger`` crate for details:
        https://docs.rs/env_logger/0.8.3/env_logger/#enabling-logging.
    client:
        The distributed Client to use. If None (default), the default client is used.
    """
    client = client or distributed.worker.get_client()
    assert client

    if isinstance(workers, int):
        workers = random.sample(list(client.scheduler_info()["workers"]), workers)

    # make the directory if necessary
    path = Path(output).resolve(strict=False)
    path.mkdir(parents=True, exist_ok=True)
    if not path.is_dir():
        raise ValueError(f"{path} is not a directory")

    async def _inject_pyspy(
        dask_worker: distributed.Worker,
    ) -> None:
        if hasattr(dask_worker, "_pyspy"):
            raise RuntimeError("py-spy is already running on this worker!")
        plugin = PySpyer(
            output=None,
            format=format,
            rate=rate,
            subprocesses=subprocesses,
            function=function,
            gil=gil,
            threads=threads,
            idle=idle,
            nonblocking=nonblocking,
            native=native,
            extra_pyspy_args=extra_pyspy_args,
            log_level=log_level,
        )
        await plugin.start()
        dask_worker._pyspy = plugin

    client.run(_inject_pyspy, workers=workers)
    try:
        yield
    finally:

        async def _get_profile(dask_worker: distributed.Worker):
            try:
                return await dask_worker._pyspy.get_profile()
            finally:
                del dask_worker._pyspy

        results = client.run(_get_profile, workers=workers)
        results = cast(Dict[str, bytes], results)
        for addr, data in results.items():
            base = addr.replace("://", "-").replace(".", "_").replace(":", "-")
            ext = (
                "json"
                if format == "speedscope"
                else "svg"
                if format == "flamegraph"
                else "pyspy"
            )
            with open(path / f"{base}.{ext}", "wb") as f:
                f.write(data)
