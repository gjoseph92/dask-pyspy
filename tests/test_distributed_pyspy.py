import json
import pathlib
import platform

import dask
import distributed
import pytest

import distributed_pyspy

pytest_plugins = ["docker_compose"]


def core_test(client: distributed.Client, tmp_path: pathlib.Path) -> None:
    df = dask.datasets.timeseries().persist()
    distributed.wait(df)

    prof_path = tmp_path / "profile.json"
    with distributed_pyspy.pyspy_on_scheduler(prof_path, client=client):
        distributed.wait(df.set_index("id").persist())

    with open(prof_path) as f:
        # Check the file is valid JSON
        profile = json.load(f)

    assert profile


@pytest.mark.skipif(
    platform.system() != "Linux", reason="py-spy always requires root on macOS"
)
def test_local(tmp_path):
    client = distributed.Client()
    core_test(client, tmp_path)
    client.shutdown()
    client.close()


def test_prctl_on_docker(module_scoped_container_getter, tmp_path):
    network_info = module_scoped_container_getter.get("scheduler").network_info[0]
    client = distributed.Client(
        f"tcp://{network_info.hostname}:{network_info.host_port}"
    )

    core_test(client, tmp_path)
