import asyncio
import logging
import os
import signal
import tempfile
from contextlib import contextmanager
from typing import Iterable, List, Optional, Union, TypeVar

import distributed
from distributed.diagnostics import SchedulerPlugin

from .prctl import allow_ptrace

logger = logging.getLogger(__name__)


class PySpyScheduler(SchedulerPlugin):
    _HANDLER_NAME = "get_py_spy_profile"

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

    async def start(self, scheduler) -> None:
        if self.output is None:
            self._tempfile = tempfile.NamedTemporaryFile(suffix="pyspy.json")
            self.output = self._tempfile.name

        # HACK: inject a `get_py_spy_profile` handler into the scheduler,
        # so we can retrieve the data more easily. Until we can stream back files,
        # there's probably not any advantage to this over an async
        # `run_on_scheduler` to retrieve the data.
        self.scheduler = scheduler
        if self._HANDLER_NAME in scheduler.handlers:
            raise RuntimeError(
                f"A py-spy plugin is already registered: "
                f"{scheduler.handlers[self._HANDLER_NAME]} vs {self._get_py_spy_profile}!"
            )
        else:
            scheduler.handlers[self._HANDLER_NAME] = self._get_py_spy_profile

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
                "No py-spy subprocess found. Either `get_profile_from_scheduler()` was "
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
        # Remove our injected handler
        del self.scheduler.handlers[self._HANDLER_NAME]
        # TODO should we remove the plugin as well?
        # At this point, there's not much reason to be using a plugin...
        if error:
            raise error

    def _maybe_close_tempfile(self):
        if self._tempfile is not None:
            self._tempfile.close()
        self._tempfile = None

    # This handler gets injected into the scheduler
    async def _get_py_spy_profile(self, comm=None) -> Union[bytes, RuntimeError]:
        try:
            await self._stop()
        except RuntimeError:
            raise
        else:
            with open(self.output, "rb") as f:
                return f.read()  # TODO streaming!
        finally:
            self._maybe_close_tempfile()

    async def close(self):
        try:
            await self._stop()
        finally:
            self._maybe_close_tempfile()


T = TypeVar("T")


def start_pyspy_on_scheduler(
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
    client: Optional[distributed.Client] = None,
) -> None:
    """
    Add a `PySpyScheduler` plugin to the Scheduler, and start it.
    """
    client = client or distributed.worker.get_client()

    async def _inject_pyspy(
        dask_scheduler: distributed.Scheduler,
    ) -> Optional[RuntimeError]:
        plugin = PySpyScheduler(
            output=output,
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
        dask_scheduler.add_plugin(plugin)
        return await plugin.start(dask_scheduler)

    client.run_on_scheduler(_inject_pyspy)


def get_profile_from_scheduler(
    path: Union[str, os.PathLike], client: Optional[distributed.Client] = None
) -> None:
    """
    Stop the current `PySpyScheduler` plugin, send back its profile data, and write it to ``path``.
    """
    client = client or distributed.worker.get_client()

    async def _get_profile():
        return await getattr(client.scheduler, PySpyScheduler._HANDLER_NAME)()

    data = client.sync(_get_profile)
    with open(path, "wb") as f:
        f.write(data)


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
        will reduce the perfomance impact of sampling, but may lead to inaccurate results
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

    start_pyspy_on_scheduler(
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
        client=client,
    )
    try:
        yield
    finally:
        get_profile_from_scheduler(output, client=client)
