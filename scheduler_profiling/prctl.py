"""
Enable other processes to ptrace this one via the ``prctl`` system call.

Only needed on Ubuntu. See

* https://www.kernel.org/doc/Documentation/admin-guide/LSM/Yama.rst
* https://wiki.ubuntu.com/SecurityTeam/Roadmap/KernelHardening#ptrace_Protection
* https://man7.org/linux/man-pages/man2/prctl.2.html

Note that we could have used https://github.com/seveas/python-prctl, but since that package doesn't
have prebuilt wheels, it would be more of a pain to install than making the one syscall ourselves via libc.
"""
import ctypes
import ctypes.util
from typing import Optional

# https://github.com/torvalds/linux/blob/master/include/uapi/linux/prctl.h#L155-L156
PR_SET_PTRACER = 0x59616D61
PR_SET_PTRACER_ANY = -1


def allow_ptrace(pid: Optional[int] = None):
    """
    Allow the PID to ptrace this process.

    If ``pid`` is None, any process is allowed.
    """
    libc = ctypes.CDLL(ctypes.util.find_library("c"))

    try:
        prctl = libc.prctl
    except AttributeError:
        raise OSError(
            "Your OS does not support the `prctl` syscall (only available on Linux), "
            "so we can't automatically allow py-spy to run without elevated permissions.\n"
            "You'll need to run the scheduler with sudo access (or if in Docker, just `--cap-add SYS_PTRACE`).\n"
            "https://github.com/benfred/py-spy#when-do-you-need-to-run-as-sudo"
        )

    prctl.argtypes = [
        ctypes.c_int,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
    ]

    prctl(PR_SET_PTRACER, pid if pid is not None else PR_SET_PTRACER_ANY, 0, 0, 0)
