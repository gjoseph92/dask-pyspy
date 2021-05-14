import functools
import inspect
from typing import Callable, TypeVar

import distributed
import viztracer


def trace_sparse(func):
    "Only collect traces while `func` is running"
    tracer = viztracer.get_tracer()
    if not tracer or not tracer.log_sparse:
        return func

    if inspect.iscoroutinefunction(func):
        # TODO this async mode doesn't work that well, since it also profiles whatever other
        # coroutines get context-switched in while awaiting this one.
        @functools.wraps(func)
        # Function declaration "wrapper" is obscured by a declaration of the same name
        async def wrapper(*args, **kwargs):  # type: ignore
            tracer = viztracer.get_tracer()
            if tracer and tracer.log_sparse:
                tracer.resume()
                try:
                    return await func(*args, **kwargs)
                finally:
                    tracer.pause()

            return func(*args, **kwargs)

    else:

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
