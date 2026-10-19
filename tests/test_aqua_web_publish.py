"""Tests for AQUA's aqua-web publishing commands."""

import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest
import yaml
from botocore.exceptions import ClientError

from aqua.core.console import aqua_analysis as push_analysis
from aqua.core.console import main as console_main
from aqua.core.console import make_contents, push_s3


@pytest.mark.parametrize("experiment,ensemble", [("catalog/model/experiment/r1", True), ("catalog/model/experiment", False)])
@pytest.mark.parametrize("entrypoint", ["api", "aqua"])
def test_publish_without_git_repository(tmp_path, monkeypatch, capfd, experiment, ensemble, entrypoint):
    """Publishing builds content in a temporary workspace without Git."""
    output = tmp_path / "output"
    source = output / experiment
    source.mkdir(parents=True)
    (source / "figure.svg").write_text('<svg xmlns="http://www.w3.org/2000/svg"><desc>name: test</desc></svg>')
    (source / "experiment.yaml").write_text("menu: Example\n")
    config = tmp_path / "grouping.yaml"
    config.write_text("diagnostics: {}\n")
    transfers = []
    published = {}

    def fake_download(client, bucket, key, destination):
        transfers.append((bucket, key, "download"))
        return False

    def fake_upload_file(client, bucket, path, key):
        transfers.append((bucket, key, "file"))
        published["experiments"] = yaml.safe_load(Path(path).read_text())

    def fake_upload_directory(client, bucket, path, prefix):
        transfers.append((bucket, prefix, "directory"))
        published["content"] = yaml.safe_load((Path(path) / "content.yaml").read_text())
        published["source"] = Path(path)

    monkeypatch.setattr(push_analysis, "create_client", Mock(return_value=object()))
    monkeypatch.setattr(push_analysis, "get_file_from_s3", fake_download)
    monkeypatch.setattr(push_analysis, "upload_file_to_s3", fake_upload_file)
    monkeypatch.setattr(push_analysis, "upload_directory_to_s3", fake_upload_directory)
    log = Mock()
    monkeypatch.setattr(push_analysis, "logger", log)
    cwd = Path.cwd()
    if entrypoint == "aqua":
        argv = ["aqua", "push", str(output), experiment, "-c", str(config), "-f", "svg"]
        if not ensemble:
            argv.append("--no-ensemble")
        monkeypatch.setattr(sys, "argv", argv)
        assert console_main.main() == 0
    else:
        push_analysis.push_analysis(output, experiment, config, formats=("svg",), ensemble=ensemble)

    assert Path.cwd() == cwd
    expected = {"catalog": {"model": {"experiment": ["r1"]} if ensemble else ["experiment"]}}
    assert published["experiments"] == expected
    assert published["content"]["experiment"]["label"] == "Example"
    assert not published["source"].exists()
    assert ("aqua-web", f"content/svg/{experiment}", "directory") in transfers
    assert log.info.call_args_list[0].args == ("Running push-analysis with AQUA version %s", push_analysis.aqua_version)
    log.info.assert_any_call(
        "Target: catalog=%s model=%s experiment=%s realization=%s",
        "catalog",
        "model",
        "experiment",
        "r1" if ensemble else "n/a",
    )
    log.info.assert_any_call("Transferring %s figures to s3://%s/%s", "svg", "aqua-web", f"content/svg/{experiment}")
    assert log.info.call_args_list[-1].args == ("Transfer completed successfully",)
    assert capfd.readouterr().out == ""


def test_aqua_push_preserves_transfer_exit_code(monkeypatch, tmp_path):
    """The AQUA entry point returns the publisher's failure status."""
    monkeypatch.setattr(sys, "argv", ["aqua", "push", str(tmp_path), "catalog/model/experiment/r1"])
    monkeypatch.setattr(console_main, "push_execute", Mock(return_value=3))
    assert console_main.main() == 3


def test_publisher_help_excludes_legacy_git_options(capsys):
    """The publisher help describes a remote site without legacy Git options."""
    with pytest.raises(SystemExit) as exc:
        push_analysis.main(["--help"])
    assert exc.value.code == 0
    help_text = capsys.readouterr().out
    assert "Publish AQUA analysis figures to a remote site." in help_text
    assert "aqua-web" not in help_text
    assert "--no-update" not in help_text
    assert "--repository" not in help_text


def test_aqua_push_help_describes_remote_site(monkeypatch, capsys):
    """The installed AQUA subcommand uses the same generic description."""
    monkeypatch.setattr(sys, "argv", ["aqua", "push", "--help"])
    with pytest.raises(SystemExit) as exc:
        console_main.main()
    assert exc.value.code == 0
    help_text = capsys.readouterr().out
    assert "Publish AQUA analysis figures to a remote site." in help_text
    assert "aqua-web" not in help_text


@pytest.mark.parametrize(
    "level,expected",
    [
        (None, "INFO"),
        ("debug", "DEBUG"),
        ("INFO", "INFO"),
        ("warning", "WARNING"),
        ("ErRoR", "ERROR"),
        ("critical", "CRITICAL"),
        ("1", "DEBUG"),
        ("2", "INFO"),
        ("3", "WARNING"),
        ("4", "ERROR"),
        ("5", "CRITICAL"),
    ],
)
def test_publisher_accepts_named_and_numeric_log_levels(tmp_path, monkeypatch, level, expected):
    """The CLI passes standard AQUA log level names to the publisher."""
    publish = Mock()
    monkeypatch.setattr(push_analysis, "push_analysis", publish)
    arguments = [str(tmp_path), "catalog/model/experiment/r1"]
    if level is not None:
        arguments.extend(["-l" if level.isdecimal() else "--loglevel", level])
    assert push_analysis.main(arguments) == 0
    assert publish.call_args.kwargs["loglevel"] == expected


@pytest.mark.parametrize("level", ["0", "6", "TRACE"])
def test_publisher_rejects_invalid_log_level(tmp_path, level):
    """Unknown names and out-of-range numeric levels are rejected."""
    with pytest.raises(SystemExit) as exc:
        push_analysis.main([str(tmp_path), "catalog/model/experiment/r1", "--loglevel", level])
    assert exc.value.code == 2


def test_publisher_logs_aqua_version_at_start(tmp_path, monkeypatch):
    """The version is reported before input validation or publishing starts."""
    log = Mock()
    monkeypatch.setattr(push_analysis, "logger", log)
    with pytest.raises(FileNotFoundError):
        push_analysis.push_analysis(tmp_path / "missing", "catalog/model/experiment/r1", tmp_path / "missing.yaml")
    log.info.assert_called_once_with("Running push-analysis with AQUA version %s", push_analysis.aqua_version)


@pytest.mark.parametrize(
    "row,ensemble,expected",
    [
        ("catalog model experiment", True, "catalog/model/experiment/r1"),
        ("climatedt-gen2 ICON-5km baseline-hist r1 default", True, "climatedt-gen2/ICON-5km/baseline-hist/r1"),
        ("catalog model experiment r2 scenario source", True, "catalog/model/experiment/r2"),
        ("catalog model experiment r2 scenario", False, "catalog/model/experiment"),
    ],
)
def test_experiment_list_columns(tmp_path, row, ensemble, expected):
    """List rows use the path columns and ignore optional metadata."""
    listing = tmp_path / "experiments.txt"
    listing.write_text(f"# comment\n\n{row}\n")
    assert list(push_analysis._experiments(listing, ensemble=ensemble)) == [expected]


@pytest.mark.parametrize("code,exit_code", [("AccessDenied", 1), ("NoSuchBucket", 2), ("Other", 3)])
def test_s3_errors_preserve_exit_codes(code, exit_code, capfd):
    """The S3 API exposes the documented CLI error codes."""
    client = Mock()
    client.upload_file.side_effect = ClientError({"Error": {"Code": code, "Message": code}}, "PutObject")
    with pytest.raises(push_s3.S3TransferError) as exc:
        push_s3.upload_file_to_s3(client, "aqua-web", "file.svg", "content/file.svg")
    assert exc.value.exit_code == exit_code
    assert capfd.readouterr().out == ""


def test_missing_s3_object_is_optional():
    """A missing remote index can be created during the first publish."""
    client = Mock()
    client.download_file.side_effect = ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
    assert push_s3.get_file_from_s3(client, "aqua-web", "content/experiments.yaml", "experiments.yaml") is False


def test_s3_success_logs_without_stdout(capfd, monkeypatch):
    """The S3 API sends transfer messages through the AQUA logger."""
    log = Mock()
    monkeypatch.setattr(push_s3, "logger", log)
    push_s3.upload_file_to_s3(Mock(), "aqua-web", "figure.svg", "content/figure.svg")
    push_s3.get_file_from_s3(Mock(), "aqua-web", "content/experiments.yaml", "experiments.yaml")
    assert capfd.readouterr().out == ""
    log.debug.assert_any_call("Uploaded %s to bucket %s as %s", "figure.svg", "aqua-web", "content/figure.svg")
    log.debug.assert_any_call("Downloaded %s from bucket %s to %s", "content/experiments.yaml", "aqua-web", "experiments.yaml")
    log.info.assert_not_called()


def test_rsync_transfer_details_use_debug(monkeypatch):
    """Rsync output is routed through the AQUA logger at DEBUG level."""
    log = Mock()
    monkeypatch.setattr(push_analysis, "logger", log)
    run = Mock(return_value=subprocess.CompletedProcess(["rsync"], 0, stdout="figure.svg\nsent 123 bytes\n", stderr=""))
    monkeypatch.setattr(push_analysis.subprocess, "run", run)
    push_analysis._run("rsync", "source", "destination")
    log.debug.assert_any_call("%s", "figure.svg")
    log.info.assert_not_called()
    run.assert_called_once_with(("rsync", "source", "destination"), cwd=None, capture_output=True, text=True, check=False)


def test_failed_transfer_has_no_completion_message(tmp_path, monkeypatch):
    """A failed upload cannot emit the overall success message."""
    output = tmp_path / "output" / "catalog" / "model" / "experiment" / "r1"
    output.mkdir(parents=True)
    config = tmp_path / "grouping.yaml"
    config.write_text("diagnostics: {}\n")
    monkeypatch.setattr(push_analysis, "create_client", Mock(return_value=object()))
    monkeypatch.setattr(push_analysis, "get_file_from_s3", Mock(return_value=False))
    monkeypatch.setattr(push_analysis, "make_contents", Mock())
    monkeypatch.setattr(push_analysis, "upload_file_to_s3", Mock())
    monkeypatch.setattr(push_analysis, "upload_directory_to_s3", Mock(side_effect=push_s3.S3TransferError("upload failed", 3)))
    log = Mock()
    monkeypatch.setattr(push_analysis, "logger", log)
    with pytest.raises(push_s3.S3TransferError):
        push_analysis.push_analysis(output.parents[3], "catalog/model/experiment/r1", config, formats=("svg",))
    assert all(call.args != ("Transfer completed successfully",) for call in log.info.call_args_list)


def test_content_generation_has_one_startup_message(tmp_path, monkeypatch):
    """Content generation reports its target and directory once."""
    config = tmp_path / "grouping.yaml"
    config.write_text("diagnostics: {figures: figure}\n")
    log = Mock()
    monkeypatch.setattr(make_contents, "logger", log)
    monkeypatch.setattr(make_contents, "make_content", Mock())
    make_contents.main(
        experiment="catalog/model/experiment/r1",
        configfile=config,
        format="svg",
        root=tmp_path,
    )
    log.info.assert_called_once_with(
        "Generating %s content for %s in %s",
        "svg",
        "catalog/model/experiment/r1",
        tmp_path / "content" / "svg",
    )
