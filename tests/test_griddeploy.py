"""Tests for the GridDeployer class."""

from pathlib import Path

import pytest

from aqua.core.gridbuilder import griddeploy as griddeploy_module
from aqua.core.gridbuilder.griddeploy import GridDeployer

pytestmark = [pytest.mark.aqua]


@pytest.fixture
def deployer(monkeypatch, tmp_path):
    """Create a GridDeployer with a fully mocked ConfigPath."""

    class DummyConfigPath:
        def __init__(self, loglevel="WARNING"):
            self.loglevel = loglevel
            self.configdir = str(tmp_path)

        def get_reader_filenames(self):
            return "ignored-reader.yaml", "ignored-grids-folder"

    monkeypatch.setattr(griddeploy_module, "ConfigPath", DummyConfigPath)
    return GridDeployer(loglevel="DEBUG")


def test_grids_deploy_path_single_string(deployer):
    single_dict = {"path": "{{ BUCKET }}/path/to/grid_a.nc"}

    extracted = deployer._grids_deploy_path(single_dict, source_grid_name="grid-a")

    assert extracted == ["/path/to/grid_a.nc"]


def test_grids_deploy_path_dict_multiple_entries(deployer):
    single_dict = {
        "path": {
            "low": "{{ BUCKET }}/path/to/grid_b_low.nc",
            "high": "{{ BUCKET }}/path/to/grid_b_high.nc",
        }
    }

    extracted = deployer._grids_deploy_path(single_dict, source_grid_name="grid-b")

    assert extracted == ["/path/to/grid_b_low.nc", "/path/to/grid_b_high.nc"]


def test_grids_deploy_path_invalid_format_returns_empty(deployer):
    single_dict = {"path": "/path/without/template/grid.nc"}

    extracted = deployer._grids_deploy_path(single_dict, source_grid_name="grid-invalid")

    assert extracted == []


def test_deploy_raises_if_default_grids_path_missing(deployer, monkeypatch):
    monkeypatch.setattr(griddeploy_module, "load_yaml", lambda _: {"paths": {}})
    monkeypatch.setattr(griddeploy_module, "load_multi_yaml", lambda **_: {"grids": {}})

    with pytest.raises(ValueError, match="Default grids path is not set in the config file"):
        deployer.deploy("ifs-r100")


def test_deploy_exact_grid_triggers_single_download(deployer, monkeypatch, tmp_path):
    calls = []
    grids_path = str(tmp_path / "target-grids")

    monkeypatch.setattr(griddeploy_module, "load_yaml", lambda _: {"paths": {"grids": grids_path}})
    monkeypatch.setattr(
        griddeploy_module,
        "load_multi_yaml",
        lambda **_: {"grids": {"ifs-r100": {"path": "{{ BUCKET }}/grids/ifs/r100.nc"}}},
    )

    def fake_download(grid_dir, grid_name, targetdir, bucket="unused"):
        calls.append((grid_dir, grid_name, targetdir))

    monkeypatch.setattr(deployer, "_download_grid", fake_download)

    deployer.deploy("ifs-r100")

    assert calls == [("/grids/ifs", "r100.nc", grids_path)]


def test_deploy_wildcard_deploys_all_matches(deployer, monkeypatch, tmp_path):
    calls = []
    grids_path = str(tmp_path / "target-grids")

    monkeypatch.setattr(griddeploy_module, "load_yaml", lambda _: {"paths": {"grids": grids_path}})
    monkeypatch.setattr(
        griddeploy_module,
        "load_multi_yaml",
        lambda **_: {
            "grids": {
                "ifs-r100": {"path": "{{ BUCKET }}/grids/ifs/r100.nc"},
                "ifs-r200": {
                    "path": {
                        "atm": "{{ BUCKET }}/grids/ifs/r200_atm.nc",
                        "oce": "{{ BUCKET }}/grids/ifs/r200_oce.nc",
                    }
                },
                "icon-r100": {"path": "{{ BUCKET }}/grids/icon/r100.nc"},
            }
        },
    )

    def fake_download(grid_dir, grid_name, targetdir, bucket="unused"):
        calls.append((grid_dir, grid_name, targetdir))

    monkeypatch.setattr(deployer, "_download_grid", fake_download)

    deployer.deploy("ifs-*")

    assert calls == [
        ("/grids/ifs", "r100.nc", grids_path),
        ("/grids/ifs", "r200_atm.nc", grids_path),
        ("/grids/ifs", "r200_oce.nc", grids_path),
    ]


def test_deploy_wildcard_no_match_does_not_download(deployer, monkeypatch, tmp_path):
    calls = []
    grids_path = str(tmp_path / "target-grids")

    monkeypatch.setattr(griddeploy_module, "load_yaml", lambda _: {"paths": {"grids": grids_path}})
    monkeypatch.setattr(griddeploy_module, "load_multi_yaml", lambda **_: {"grids": {"ifs-r100": {"path": "x"}}})
    monkeypatch.setattr(deployer, "_download_grid", lambda *args, **kwargs: calls.append((args, kwargs)))

    deployer.deploy("nemo-*")

    assert calls == []


def test_download_grid_creates_file_with_mocked_response(deployer, monkeypatch, tmp_path):
    requested = {"url": None, "stream": None}

    class DummyResponse:
        def __init__(self, chunks):
            self._chunks = chunks

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def raise_for_status(self):
            return None

        def iter_content(self, chunk_size=8192):
            assert chunk_size == 8192
            for chunk in self._chunks:
                yield chunk

    def fake_get(url, stream=True):
        requested["url"] = url
        requested["stream"] = stream
        return DummyResponse([b"abc", b"", b"def"])

    monkeypatch.setattr(griddeploy_module.requests, "get", fake_get)

    targetdir = tmp_path / "grids"
    deployer._download_grid(
        grid_dir="/nested/folder",
        grid_name="mygrid.nc",
        targetdir=str(targetdir),
        bucket="https://example.com/base//",
    )

    output_file = targetdir / "nested" / "folder" / "mygrid.nc"
    assert output_file.exists()
    assert output_file.read_bytes() == b"abcdef"
    assert requested["stream"] is True
    assert requested["url"] == "https://example.com/base/nested/folder/mygrid.nc"


def test_download_grid_skips_if_file_exists(deployer, monkeypatch, tmp_path):
    called = {"get": 0}

    def fake_get(*args, **kwargs):
        called["get"] += 1
        raise AssertionError("requests.get should not be called when file exists")

    monkeypatch.setattr(griddeploy_module.requests, "get", fake_get)

    targetdir = tmp_path / "grids"
    existing_file = targetdir / "nested" / "folder" / "mygrid.nc"
    Path(existing_file.parent).mkdir(parents=True, exist_ok=True)
    existing_file.write_bytes(b"existing")

    deployer._download_grid(grid_dir="nested/folder", grid_name="mygrid.nc", targetdir=str(targetdir))

    assert called["get"] == 0
    assert existing_file.read_bytes() == b"existing"
