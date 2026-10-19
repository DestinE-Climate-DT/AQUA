"""Tests for the standalone aqua-web publishing commands."""

import sys
from pathlib import Path
from unittest.mock import Mock

import pytest
import yaml
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "cli" / "aqua-web"))
import push_analysis  # noqa: E402
import push_s3  # noqa: E402


@pytest.mark.parametrize("experiment,ensemble", [("catalog/model/experiment/r1", True), ("catalog/model/experiment", False)])
def test_publish_local_repository(tmp_path, monkeypatch, experiment, ensemble):
    """Publishing generates content for every format without changing cwd."""
    output = tmp_path / "output"
    source = output / experiment
    source.mkdir(parents=True)
    (source / "figure.svg").write_text('<svg xmlns="http://www.w3.org/2000/svg"><desc>name: test</desc></svg>')
    (source / "experiment.yaml").write_text("menu: Example\n")
    config = tmp_path / "grouping.yaml"
    config.write_text("diagnostics: {}\n")
    repo = tmp_path / "aqua-web"
    repo.mkdir()
    transfers = []

    def fake_download(client, bucket, key, destination):
        transfers.append((bucket, key, "download"))
        return False

    def fake_upload_file(client, bucket, path, key):
        transfers.append((bucket, key, "file"))

    def fake_upload_directory(client, bucket, path, prefix):
        transfers.append((bucket, prefix, "directory"))

    monkeypatch.setattr(push_analysis, "create_client", Mock(return_value=object()))
    monkeypatch.setattr(push_analysis, "get_file_from_s3", fake_download)
    monkeypatch.setattr(push_analysis, "upload_file_to_s3", fake_upload_file)
    monkeypatch.setattr(push_analysis, "upload_directory_to_s3", fake_upload_directory)
    cwd = Path.cwd()
    push_analysis.push_analysis(
        output, experiment, config, repository=f"local:{repo}", formats=("svg",), ensemble=ensemble, update=False
    )

    assert Path.cwd() == cwd
    content_dir = repo / "content" / "svg" / experiment
    assert (content_dir / "content.yaml").is_file()
    assert (content_dir / "content.json").is_file()
    expected = {"catalog": {"model": {"experiment": ["r1"]} if ensemble else ["experiment"]}}
    assert yaml.safe_load((repo / "content" / "experiments.yaml").read_text()) == expected
    assert experiment in (repo / "updated.txt").read_text()
    assert ("aqua-web", f"content/svg/{experiment}", "directory") in transfers


def test_experiment_list_defaults_to_r1(tmp_path):
    """Three-column list entries use the default realization."""
    listing = tmp_path / "experiments.txt"
    listing.write_text("# comment\n\ncatalog model experiment\n")
    assert list(push_analysis._experiments(listing, ensemble=True)) == ["catalog/model/experiment/r1"]


@pytest.mark.parametrize("code,exit_code", [("AccessDenied", 1), ("NoSuchBucket", 2), ("Other", 3)])
def test_s3_errors_preserve_exit_codes(code, exit_code):
    """The S3 API exposes the documented CLI error codes."""
    client = Mock()
    client.upload_file.side_effect = ClientError({"Error": {"Code": code, "Message": code}}, "PutObject")
    with pytest.raises(push_s3.S3TransferError) as exc:
        push_s3.upload_file_to_s3(client, "aqua-web", "file.svg", "content/file.svg")
    assert exc.value.exit_code == exit_code


def test_missing_s3_object_is_optional():
    """A missing remote index can be created during the first publish."""
    client = Mock()
    client.download_file.side_effect = ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
    assert push_s3.get_file_from_s3(client, "aqua-web", "content/experiments.yaml", "experiments.yaml") is False
