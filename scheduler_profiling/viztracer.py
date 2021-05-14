import functools
import os
import tempfile
from contextlib import contextmanager
from typing import Callable, Literal, Optional, Sequence, TypeVar, Union, cast

import distributed
import viztracer
from distributed.diagnostics import SchedulerPlugin


def trace_sparse(func):
    "Only collect traces while `func` is running"
    tracer = viztracer.get_tracer()
    if not tracer or not tracer.log_sparse:
        return func

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        tracer = viztracer.get_tracer()
        if tracer and tracer.log_sparse:
            tracer.resume()
            try:
                return func(*args, **kwargs)
            finally:
                tracer.pause()

        return func(*args, **kwargs)

    return wrapper


# Import logic vendored from `unittest.mock`
# (since it's a private API, would rather not import it directly)
# https://github.com/python/cpython/blob/04ce4c7/Lib/unittest/mock.py#L1225-L1241
def _dot_lookup(thing, comp, import_path):
    try:
        return getattr(thing, comp)
    except AttributeError:
        __import__(import_path)
        return getattr(thing, comp)


def _importer(target):
    components = target.split(".")
    import_path = components.pop(0)
    thing = __import__(import_path)

    for comp in components:
        import_path += ".%s" % comp
        thing = _dot_lookup(thing, comp, import_path)
    return thing


FuncT = TypeVar("FuncT", bound=Callable)


def add_decorator(
    name: str, scheduler: distributed.Scheduler, decorator: Callable[[FuncT], FuncT]
) -> Callable[[], None]:
    obj_name, attr = name.rsplit(".", 1)
    obj = _importer(obj_name)
    type_obj = obj if isinstance(obj, type) else type(obj)
    original = getattr(type_obj, attr)
    assert callable(original), f"{name} is not callable: {original!r}"
    patched = decorator(original)
    setattr(type_obj, attr, patched)

    # Check if the method is a handler on the scheduler; if so, patch it there too
    handler_name = None
    handler_dict = None
    if obj_name == "distributed.Scheduler":
        for hd in (scheduler.stream_handlers, scheduler.handlers):
            for k, v in hd.items():
                # Compare underlying function, not bound method https://stackoverflow.com/a/15977850
                # NOTE: can switch to `==` once py3.7 support is dropped
                if getattr(v, "__func__", None) is original:
                    handler_name = k
                    handler_dict = hd
                    hd[k] = patched_method = getattr(scheduler, attr)
                    # ^ get bound method; `original` is plain function on the type object
                    assert patched_method.__func__ is patched
                    break
            if handler_name is not None:
                break

    def unpatch() -> None:
        setattr(type_obj, attr, original)
        if handler_name is not None:
            handler_dict[handler_name] = original

    return unpatch


class VizTracerScheduler(SchedulerPlugin):
    _HANDLER_NAME = "get_viztracer_profile"

    def __init__(
        self,
        format: Literal["json", "json.gz", "html"] = "json",
        tracer_entries: int = 1000000,
        verbose: Literal[0, 1] = 1,
        max_stack_depth: int = -1,
        include_files: Optional[Sequence[str]] = None,
        exclude_files: Optional[Sequence[str]] = None,
        ignore_c_function: bool = False,
        ignore_frozen: bool = False,
        log_func_retval: bool = False,
        log_func_args: bool = False,
        log_print: bool = False,
        log_gc: bool = False,
        log_sparse: Union[str, Sequence[str]] = (),
        trace_sparse: Union[str, Sequence[str]] = (),
        log_async: bool = False,
        vdb: bool = False,
        file_info: bool = False,
        register_global: bool = True,
        plugins=None,
    ) -> None:

        if plugins is None:
            plugins = []

        if isinstance(log_sparse, str):
            log_sparse = (log_sparse,)

        self.patch_log_sparse_names: Sequence[str] = log_sparse
        self.log_sparse_unpatchers: Sequence[Callable[[], None]] = ()
        sparse = bool(log_sparse)
        if sparse and not register_global:
            raise ValueError("`register_global=True` is required with `log_sparse`")

        if isinstance(trace_sparse, str):
            trace_sparse = (trace_sparse,)

        self.patch_trace_sparse_names: Sequence[str] = trace_sparse
        self.trace_sparse_unpatchers: Sequence[Callable[[], None]] = ()
        sparse |= bool(trace_sparse)
        if sparse and not register_global:
            raise ValueError("`register_global=True` is required with `trace_sparse`")

        self.format = format
        self.tracer = None
        self._tempfile = None
        self.kwargs = dict(
            tracer_entries=tracer_entries,
            verbose=verbose,
            max_stack_depth=max_stack_depth,
            include_files=include_files,
            exclude_files=exclude_files,
            ignore_c_function=ignore_c_function,
            ignore_frozen=ignore_frozen,
            log_func_retval=log_func_retval,
            log_func_args=log_func_args,
            log_print=log_print,
            log_gc=log_gc,
            log_sparse=sparse,
            log_async=log_async,
            vdb=vdb,
            file_info=file_info,
            register_global=register_global,
            plugins=plugins,
        )

    async def start(self, scheduler: distributed.Scheduler) -> None:
        self.scheduler = scheduler
        self._tempfile = tempfile.NamedTemporaryFile(suffix=f"viztracer.{self.format}")
        if self._HANDLER_NAME in scheduler.handlers:
            raise RuntimeError(
                "A viztracer plugin is already registered: "
                f"{scheduler.handlers[self._HANDLER_NAME]} vs {self._get_profile}!"
            )
        else:
            scheduler.handlers[self._HANDLER_NAME] = self._get_profile

        self.tracer = viztracer.VizTracer(
            output_file=self._tempfile.name, **self.kwargs
        )

        # NOTE: this has to come after the tracer is instantiated, so `get_tracer()` works within `log_sparse`
        self.log_sparse_unpatchers = [
            add_decorator(name, scheduler, viztracer.log_sparse)
            for name in self.patch_log_sparse_names
        ]
        self.trace_sparse_unpatchers = [
            add_decorator(name, scheduler, trace_sparse)
            for name in self.patch_trace_sparse_names
        ]

        if self.tracer.log_sparse:
            if self.patch_trace_sparse_names:
                # HACK: to make `trace_sparse` work, the tracer must have already been started. otherwise,
                # https://github.com/gaogaotiantian/viztracer/blob/1c7080b61f2/src/viztracer/modules/snaptrace.c#L583
                # segfaults since `cur_tracer` is NULL.
                # (`cur_tracer` in the C extension != `register_global` at the Python level!)
                # TODO contribute `trace_sparse` upstream to viztracer
                self.tracer.start()
                self.tracer.pause()
            else:
                # this is how `log_sparse` is implemented:
                # https://github.com/gaogaotiantian/viztracer/blob/1c7080b61f/src/viztracer/main.py#L239-L244
                self.tracer.enable = True
        else:
            self.tracer.start()

    async def _get_profile(self, comm=None) -> bytes:
        if self.tracer is None:
            raise RuntimeError(
                "viztracer never started, or has already been stopped and the profile retrieved"
            )
        try:
            self.tracer.stop()
            self.tracer.save()

            assert self._tempfile is not None, "No tempfile"
            return self._tempfile.read()
        finally:
            self._cleanup()

    def _cleanup(self):
        if self._tempfile:
            self._tempfile.close()
            self._tempfile = None
        if self.tracer:
            self.tracer.cleanup()
            self.tracer = None
        if self.log_sparse_unpatchers:
            [unpatch() for unpatch in self.log_sparse_unpatchers]
            self.log_sparse_unpatchers = ()
        if self.trace_sparse_unpatchers:
            [unpatch() for unpatch in self.trace_sparse_unpatchers]
            self.trace_sparse_unpatchers = ()
        self.scheduler.handlers.pop(self._HANDLER_NAME, None)

    async def close(self):
        self._cleanup()


def get_profile_from_scheduler(
    path: Union[str, os.PathLike], client: distributed.Client
) -> None:
    """
    Stop the current `VizTracerScheduler` plugin, send back its profile data, and write it to ``path``.
    """

    async def _get_profile():
        return await getattr(client.scheduler, VizTracerScheduler._HANDLER_NAME)()

    data = client.sync(_get_profile)
    data = cast(bytes, data)
    if not data:
        raise RuntimeError("Trace data was empty!")
    with open(path, "wb") as f:
        f.write(data)


@contextmanager
def viztrace_scheduler(
    output: Union[str, os.PathLike],
    tracer_entries: int = 1000000,
    verbose: Literal[0, 1] = 1,
    max_stack_depth: int = -1,
    include_files: Optional[Sequence[str]] = None,
    exclude_files: Optional[Sequence[str]] = None,
    ignore_c_function: bool = False,
    ignore_frozen: bool = False,
    log_func_retval: bool = False,
    log_func_args: bool = False,
    log_print: bool = False,
    log_gc: bool = False,
    log_sparse: Union[str, Sequence[str]] = (),
    trace_sparse: Union[str, Sequence[str]] = (),
    log_async: bool = False,
    vdb: bool = False,
    file_info: bool = False,
    register_global: bool = True,
    plugins=None,
    client: Optional[distributed.Client] = None,
):
    """
    Profile the Scheduler with viztracer.

    Use as a context manager (similar to `distributed.performance_report`) to record a viztracer
    profile of the scheduler.

    When the context manager exits, the profile is sent back to the client and saved to
    the ``output`` path.

    All parameters are the same as the `viztracer.VizTracer` object, except ``log_sparse``
    and ``trace_sparse`` which have special behavior described in the Note section.

    Unlike py-spy, since viztracer is works best on very short samples, you'd typically use
    ``trace_sparse`` to target just the function(s) you're interested in, and run this on a
    short computation.

    Note
    ----
    You can set tracing to only happen while specific functions are running with the ``*_sparse``
    parameters. ``log_sparse`` is very basic, and will just record when the function starts and stops,
    without any tracing within it. ``trace_sparse`` is typically what you want: it will record traces
    while that function is running, and pause the tracer otherwise.

    To use the ``*_sparse`` options, pass the names of functions to trace as strings, the exact same
    way you would to ``unittest.mock.patch``.

    For example, ``trace_sparse="dask.highlevelgraph.HighLevelGraph.__dask_distributed_unpack__"``.
    When profiling ends, all patches are un-done.

    This mostly "just works" even for cases where (in theory) it shouldn't, thanks to this magic behavior:
    if the name refers to a ``distributed.Scheduler`` method that's registered as a comm handler
    in the ``scheduler.stream_handlers`` or ``scheduler.handlers`` dicts, the patched method is
    also automatically inserted into the handler dict (this is un-done when profiling ends).

    Note that any use of ``@viztracer.log_sparse`` in code will be ignored, since the way
    ``viztracer.log_sparse`` is implemented, a global ``VizTracer`` object has to be registered
    at *import time* for the decorator to do anything. Since the whole point of this is to start
    a ``VizTracer`` object once the scheduler is already running, your only option is to pass names
    in ``log_sparse``.

    Parameters
    ----------
    output:
        *Local* path to save the profile to, once it's sent back from the scheduler.
        The extension of the path determines the output format; should be ``.json``,
        ``.json.gz``, or ``.html``.
    tracer_entries:
        Size of circular buffer. The user can only specify this value when
        instantiate ``VizTracer`` object or if they use command line

        Please be aware that a larger number of entries also means more disk space,
        RAM usage and loading time. Be familiar with your computer's limit.

        ``tracer_entries`` means how many entries ``VizTracer`` can store. It's not a byte number.
    verbose:
        Verbose level of VizTracer. Can be set to ``0`` so it won't print anything while tracing
    max_stack_depth:
        Specify the maximum stack depth VizTracer will trace. ``-1`` means infinite.
    include_files:
        Specify the files or folders that VizTracer will trace. If it's not empty, VizTracer
        will function in whitelist mode, any files/folders not included will be ignored.

        Because converting code filename in tracer is too expensive, we will only compare the
        input and its absolute path against code filename, which could be a relative path.
        That means, if you run your program using relative path, but gives the ``include_files``
        an absolute path, it will not be able to detect.

        Can't be set with ``exclude_files``
    exclude_files:
        Specify the files or folders that VizTracer will not trace. If it's not empty,
        VizTracer will function in blacklist mode, any files/folders not included will be ignored.

        Because converting code filename in tracer is too expensive, we will only compare the input
        and its absolute path against code filename, which could be a relative path.
        That means, if you run your program using relative path, but gives the ``exclude_files``
        an absolute path, it will not be able to detect.

        Can't be set with ``include_files``
    ignore_c_function:
        Whether trace c function
    ignore_frozen:
        Whether trace functions from frozen functions(mostly import stuff)
    log_func_retval:
        Whether log the return value of the function as string in report entry
    log_func_args:
        Whether log the arguments of the function as string in report entry
    log_print:
        Whether replace the ``print`` function to log in VizTracer report
    log_gc:
        Whether log garbage collector
    log_sparse:
        See note section for details. Functions to wrap in the `viztracer.log_sparse` decorator.
    trace_sparse:
        See note section for details. Only trace while these functions are running. The tracer is unpaused
        when the function starts, then paused when it ends.
    log_async:
        Whether to record asyncio tasks as "threads"
    vdb:
        whether make viztracer instrument for vdb, which would affect the overhead and the file size a bit
    file_info:
        Undocumented in viztracer.
    register_global:
        whether register the tracer globally, so every file can use ``get_tracer()`` to get
        this tracer. When command line entry is used, the tracer will be automatically registered.
        When ``VizTracer()`` is manually instantiated, it will be registered as well by default.

        Some functions may require a globally registered tracer to work.
    plugins:
        Undocumented in viztracer.
    client:
        The distributed Client to use. If None (default), the default client is used.
    """

    if plugins is None:
        plugins = []

    _client = client or distributed.worker.get_client()
    _, format = os.path.splitext(output)

    async def _inject_viztracer(dask_scheduler: distributed.Scheduler):
        plugin = VizTracerScheduler(
            # Type "str | Unknown" cannot be assigned to type "Literal['json', 'json.gz', 'html']"
            format=format,  # type: ignore
            tracer_entries=tracer_entries,
            verbose=verbose,
            max_stack_depth=max_stack_depth,
            include_files=include_files,
            exclude_files=exclude_files,
            ignore_c_function=ignore_c_function,
            ignore_frozen=ignore_frozen,
            log_func_retval=log_func_retval,
            log_func_args=log_func_args,
            log_print=log_print,
            log_gc=log_gc,
            log_sparse=log_sparse,
            trace_sparse=trace_sparse,
            log_async=log_async,
            vdb=vdb,
            file_info=file_info,
            register_global=register_global,
            plugins=plugins,
        )
        dask_scheduler.add_plugin(plugin)
        return await plugin.start(dask_scheduler)

    _client.run_on_scheduler(_inject_viztracer)
    try:
        yield
    finally:
        get_profile_from_scheduler(output, _client)
