import json
import pathlib
import platform
import time

import dask
import distributed
import pytest

import dask_profiling

pytest_plugins = ["docker_compose"]


def core_test(client: distributed.Client, tmp_path: pathlib.Path) -> None:
    df = dask.datasets.timeseries().persist()

    scheduler_prof_path = tmp_path / "profile.json"
    worker_prof_dir = tmp_path / "workers"
    with dask_profiling.pyspy_on_scheduler(
        scheduler_prof_path, client=client
    ), dask_profiling.pyspy(worker_prof_dir, client=client):
        df.set_index("id").size.compute(client=client)

    with open(scheduler_prof_path) as f:
        # Check the file is valid JSON
        profile = json.load(f)
        assert profile

    assert worker_prof_dir.exists()
    assert len(list(worker_prof_dir.glob("*.json"))) == len(
        client.scheduler_info()["workers"]
    )
    for p in worker_prof_dir.glob("*.json"):
        with open(p) as f:
            # Check the file is valid JSON
            profile = json.load(f)
            assert profile


@pytest.mark.skipif(
    platform.system() != "Linux", reason="py-spy always requires root on macOS"
)
def test_local(tmp_path):
    client = distributed.Client(set_as_default=False)
    core_test(client, tmp_path)
    client.shutdown()
    client.close()


def test_prctl_on_docker(module_scoped_container_getter, tmp_path):
    network_info = module_scoped_container_getter.get("scheduler").network_info[0]
    time.sleep(1)  # HACK: wait for distributed to actually start
    client = distributed.Client(
        f"tcp://{network_info.hostname}:{network_info.host_port}", set_as_default=False
    )

    core_test(client, tmp_path)
