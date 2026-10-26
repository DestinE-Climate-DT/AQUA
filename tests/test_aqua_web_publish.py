"""Tests for AQUA's figure publishing commands."""

import json
import logging
import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest
import yaml
from botocore.exceptions import ClientError
from PIL import Image, PngImagePlugin
from pypdf import PdfWriter

from aqua.core.console import main as console_main
from aqua.core.console import make_contents, push_analysis, push_s3

pytestmark = pytest.mark.aqua


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

    def fake_download(client, bucket, key, destination, **kwargs):
        transfers.append((bucket, key, "download"))
        return False

    def fake_upload_file(client, bucket, path, key, **kwargs):
        transfers.append((bucket, key, "file"))
        published["experiments"] = yaml.safe_load(Path(path).read_text())

    def fake_upload_directory(client, bucket, path, prefix, **kwargs):
        transfers.append((bucket, prefix, "directory"))
        published["content"] = yaml.safe_load((Path(path) / "content.yaml").read_text())
        published["source"] = Path(path)

    monkeypatch.setattr(push_analysis, "create_client", Mock(return_value=object()))
    monkeypatch.setattr(push_analysis, "get_file_from_s3", fake_download)
    monkeypatch.setattr(push_analysis, "upload_file_to_s3", fake_upload_file)
    monkeypatch.setattr(push_analysis, "upload_directory_to_s3", fake_upload_directory)
    log = Mock()
    monkeypatch.setattr(push_analysis, "log_configure", Mock(return_value=log))
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


def test_publish_list_merges_existing_experiments(tmp_path, monkeypatch):
    """Publishing several realizations preserves the remote experiment index."""
    output = tmp_path / "output"
    for realization in ("r1", "r2"):
        source = output / "catalog" / "model" / "experiment" / realization
        source.mkdir(parents=True)
        (source / f"figure-{realization}.svg").write_text('<svg xmlns="http://www.w3.org/2000/svg"/>')
    listing = tmp_path / "experiments.txt"
    listing.write_text("catalog model experiment r1 default\ncatalog model experiment r2 default\n")
    config = tmp_path / "grouping.yaml"
    config.write_text("diagnostics: {}\n")
    remote = {"catalog": {"model": ["previous"]}}
    published = {}

    def download(client, bucket, key, destination, **kwargs):
        Path(destination).write_text(yaml.safe_dump(remote))
        return True

    def upload_index(client, bucket, path, key, **kwargs):
        nonlocal remote
        remote = yaml.safe_load(Path(path).read_text())

    def upload_figures(client, bucket, path, prefix, **kwargs):
        published[prefix] = yaml.safe_load((Path(path) / "content.yaml").read_text())

    monkeypatch.setattr(push_analysis, "create_client", Mock(return_value=object()))
    monkeypatch.setattr(push_analysis, "get_file_from_s3", download)
    monkeypatch.setattr(push_analysis, "upload_file_to_s3", upload_index)
    monkeypatch.setattr(push_analysis, "upload_directory_to_s3", upload_figures)

    push_analysis.push_analysis(output, listing, config, formats=("svg",))

    assert remote == {"catalog": {"model": {"previous": ["r1"], "experiment": ["r1", "r2"]}}}
    assert set(published) == {
        "content/svg/catalog/model/experiment/r1",
        "content/svg/catalog/model/experiment/r2",
    }
    for realization in ("r1", "r2"):
        key = f"content/svg/catalog/model/experiment/{realization}"
        assert f"catalog/model/experiment/{realization}/figure-{realization}.svg" in published[key]["files"]


@pytest.mark.parametrize("figure_status", [0, 23])
def test_aqua_push_rsync_status(tmp_path, monkeypatch, figure_status):
    """Rsync publication keeps its transfer status and never needs S3."""
    experiment = "catalog/model/experiment/r1"
    source = tmp_path / "output" / experiment
    source.mkdir(parents=True)
    (source / "figure.svg").write_text('<svg xmlns="http://www.w3.org/2000/svg"/>')
    config = tmp_path / "grouping.yaml"
    config.write_text("diagnostics: {}\n")
    commands = []

    def fake_run(command, **kwargs):
        commands.append(command)
        if command[2].startswith("user@example:"):
            return subprocess.CompletedProcess(command, 23, stdout="", stderr="remote index missing")
        return subprocess.CompletedProcess(
            command, figure_status if "--relative" in command else 0, stdout="", stderr="transfer failed"
        )

    monkeypatch.setattr(push_analysis.subprocess, "run", fake_run)
    monkeypatch.setattr(push_analysis, "create_client", Mock(side_effect=AssertionError("S3 must not be used")))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "aqua",
            "push",
            str(tmp_path / "output"),
            experiment,
            "-c",
            str(config),
            "-f",
            "svg",
            "--rsync",
            "user@example:/site",
        ],
    )
    logger = Mock()
    monkeypatch.setattr(push_analysis, "log_configure", Mock(return_value=logger))

    assert console_main.main() == figure_status
    assert len(commands) == 3  # optional index download, index upload, figure transfer
    assert "--relative" in commands[-1]
    if figure_status:
        logger.info.assert_any_call(
            "Transferring %s figures to %s/%s", "svg", "user@example:/site", f"content/svg/{experiment}/"
        )
        assert all(call.args != ("Transfer completed successfully",) for call in logger.info.call_args_list)
    else:
        assert logger.info.call_args_list[-1].args == ("Transfer completed successfully",)


def test_aqua_push_preserves_transfer_exit_code(monkeypatch, tmp_path):
    """The AQUA entry point returns the publisher's failure status."""
    monkeypatch.setattr(sys, "argv", ["aqua", "push", str(tmp_path), "catalog/model/experiment/r1"])
    monkeypatch.setattr(push_analysis, "push_analysis", Mock(side_effect=push_s3.S3TransferError("upload failed", 3)))
    assert console_main.main() == 3


def test_aqua_push_dispatches_to_push_execute(monkeypatch, tmp_path):
    """The console forwards the push command to its executor."""
    monkeypatch.setattr(sys, "argv", ["aqua", "push", str(tmp_path), "catalog/model/experiment/r1"])
    execute = Mock(return_value=3)
    monkeypatch.setattr(console_main, "push_execute", execute)
    assert console_main.main() == 3
    assert execute.call_count == 1


def test_aqua_push_rejects_invalid_format(monkeypatch, tmp_path):
    """Invalid formats fail before the publishing routine is called."""
    monkeypatch.setattr(sys, "argv", ["aqua", "push", str(tmp_path), "catalog/model/experiment/r1", "-f", "jpeg"])
    publish = Mock()
    monkeypatch.setattr(push_analysis, "push_analysis", publish)
    assert console_main.main() == 2
    publish.assert_not_called()


def test_aqua_push_help_describes_remote_site(monkeypatch, capsys):
    """The AQUA subcommand help describes a remote site without legacy Git options."""
    monkeypatch.setattr(sys, "argv", ["aqua", "push", "--help"])
    with pytest.raises(SystemExit) as exc:
        console_main.main()
    assert exc.value.code == 0
    help_text = capsys.readouterr().out
    assert "Publish AQUA analysis figures to a remote site." in help_text
    assert "default: WARNING" in help_text
    assert "aqua-web" not in help_text
    assert "--no-update" not in help_text
    assert "--repository" not in help_text


@pytest.mark.parametrize(
    "level,expected",
    [
        (None, "WARNING"),
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
    arguments = ["aqua", "push", str(tmp_path), "catalog/model/experiment/r1"]
    if level is not None:
        arguments.extend(["-l" if level.isdecimal() else "--loglevel", level])
    monkeypatch.setattr(sys, "argv", arguments)
    assert console_main.main() == 0
    assert publish.call_args.kwargs["loglevel"] == expected


@pytest.mark.parametrize("level", ["0", "6", "TRACE"])
def test_publisher_rejects_invalid_log_level(tmp_path, monkeypatch, level):
    """Unknown names and out-of-range numeric levels are rejected."""
    monkeypatch.setattr(sys, "argv", ["aqua", "push", str(tmp_path), "catalog/model/experiment/r1", "--loglevel", level])
    with pytest.raises(SystemExit) as exc:
        console_main.main()
    assert exc.value.code == 2


def test_publisher_logs_aqua_version_at_start(tmp_path, monkeypatch):
    """The version is reported before input validation or publishing starts."""
    log = Mock()
    monkeypatch.setattr(push_analysis, "log_configure", Mock(return_value=log))
    with pytest.raises(FileNotFoundError):
        push_analysis.push_analysis(tmp_path / "missing", "catalog/model/experiment/r1", tmp_path / "missing.yaml")
    log.info.assert_called_once_with("Running push-analysis with AQUA version %s", push_analysis.aqua_version)


@pytest.mark.parametrize("level", ["WARNING", "DEBUG"])
def test_publisher_configures_logger_at_execution(tmp_path, level):
    """Direct publishing configures its logger before input validation."""
    with pytest.raises(FileNotFoundError):
        push_analysis.push_analysis(tmp_path, "catalog/model/experiment/r1", tmp_path / "missing.yaml", loglevel=level)
    assert logging.getLogger("push_analysis").level == getattr(logging, level)


def test_publisher_passes_debug_level_to_s3(tmp_path, monkeypatch):
    """S3 transfer helpers retain the requested log level during publication."""
    experiment = "catalog/model/experiment/r1"
    source = tmp_path / "output" / experiment
    source.mkdir(parents=True)
    (source / "figure.svg").write_text('<svg xmlns="http://www.w3.org/2000/svg"/>')
    config = tmp_path / "grouping.yaml"
    config.write_text("diagnostics: {}\n")
    client = Mock()
    client.download_file.side_effect = ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
    monkeypatch.setattr(push_analysis, "create_client", Mock(return_value=client))

    push_analysis.push_analysis(tmp_path / "output", experiment, config, formats=("svg",), loglevel="DEBUG")

    assert client.upload_file.called
    assert logging.getLogger("push_s3").level == logging.DEBUG


def test_content_generator_configures_logger_at_execution(tmp_path):
    """The content generator honors its requested level before reading config."""
    with pytest.raises(FileNotFoundError):
        make_contents.main(configfile=tmp_path / "missing.yaml", loglevel="DEBUG")
    assert logging.getLogger("make_contents").level == logging.DEBUG


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
    assert push_analysis._experiments(listing, ensemble=ensemble) == [expected]


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
    monkeypatch.setattr(push_s3, "log_configure", Mock(return_value=log))
    push_s3.upload_file_to_s3(Mock(), "aqua-web", "figure.svg", "content/figure.svg")
    push_s3.get_file_from_s3(Mock(), "aqua-web", "content/experiments.yaml", "experiments.yaml")
    assert capfd.readouterr().out == ""
    log.debug.assert_any_call("Uploaded %s to bucket %s as %s", "figure.svg", "aqua-web", "content/figure.svg")
    log.debug.assert_any_call("Downloaded %s from bucket %s to %s", "content/experiments.yaml", "aqua-web", "experiments.yaml")
    log.info.assert_not_called()


def test_s3_cli_uploads_nested_directory(tmp_path, monkeypatch):
    """The standalone S3 command preserves relative paths in an uploaded directory."""
    source = tmp_path / "figures"
    figure = source / "nested" / "figure.svg"
    figure.parent.mkdir(parents=True)
    figure.write_text("<svg/>")
    client = Mock()
    monkeypatch.setattr(push_s3, "create_client", Mock(return_value=client))

    assert push_s3.cli_main(["bucket", str(source), "-d", "content/svg"]) == 0
    client.upload_file.assert_called_once_with(str(figure), "bucket", "content/svg/nested/figure.svg")
    assert logging.getLogger("push_s3").level == logging.WARNING


@pytest.mark.parametrize("code,expected", [("AccessDenied", 1), ("NoSuchBucket", 2), ("InternalError", 3)])
def test_s3_cli_reports_upload_failures(tmp_path, monkeypatch, code, expected):
    """The standalone command returns the documented S3 failure status."""
    figure = tmp_path / "figure.svg"
    figure.write_text("<svg/>")
    client = Mock()
    client.upload_file.side_effect = ClientError({"Error": {"Code": code, "Message": code}}, "PutObject")
    monkeypatch.setattr(push_s3, "create_client", Mock(return_value=client))

    assert push_s3.cli_main(["bucket", str(figure)]) == expected


@pytest.mark.parametrize("code,expected", [(None, 0), ("NoSuchKey", 0), ("AccessDenied", 1)])
def test_s3_cli_download_status(tmp_path, monkeypatch, code, expected):
    """A missing index is optional; access failures abort the download command."""
    client = Mock()
    if code is not None:
        client.download_file.side_effect = ClientError({"Error": {"Code": code, "Message": code}}, "GetObject")
    monkeypatch.setattr(push_s3, "create_client", Mock(return_value=client))
    destination = tmp_path / "experiments.yaml"

    assert push_s3.cli_main(["-g", "bucket", str(destination), "-d", "content/experiments.yaml"]) == expected
    client.download_file.assert_called_once_with("bucket", "content/experiments.yaml", str(destination))


def test_rsync_transfer_details_use_debug(monkeypatch):
    """Rsync output is routed through the AQUA logger at DEBUG level."""
    log = Mock()
    monkeypatch.setattr(push_analysis, "log_configure", Mock(return_value=log))
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
    monkeypatch.setattr(push_analysis, "log_configure", Mock(return_value=log))
    with pytest.raises(push_s3.S3TransferError):
        push_analysis.push_analysis(output.parents[3], "catalog/model/experiment/r1", config, formats=("svg",))
    assert all(call.args != ("Transfer completed successfully",) for call in log.info.call_args_list)


def test_content_generation_has_one_startup_message(tmp_path, monkeypatch):
    """Content generation reports its target and directory once."""
    config = tmp_path / "grouping.yaml"
    config.write_text("diagnostics: {figures: figure}\n")
    log = Mock()
    monkeypatch.setattr(make_contents, "log_configure", Mock(return_value=log))
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


@pytest.mark.parametrize("ensemble", [True, False])
def test_content_generation_scans_all_experiments(tmp_path, ensemble):
    """A full scan rebuilds the index and content for both layout styles."""
    root = tmp_path / "site"
    experiment = "catalog/model/experiment/r1" if ensemble else "catalog/model/experiment"
    source = root / "content" / "svg" / experiment
    source.mkdir(parents=True)
    (source / "figure.svg").write_text('<svg xmlns="http://www.w3.org/2000/svg"/>')
    index = root / "content" / "experiments.yaml"
    index.write_text("stale: value\n")
    config = tmp_path / "grouping.yaml"
    config.write_text("diagnostics: {Figures: figure}\n")

    make_contents.main(configfile=config, root=root, format="svg", ensemble=ensemble)

    expected = {"catalog": {"model": {"experiment": ["r1"]} if ensemble else ["experiment"]}}
    assert yaml.safe_load(index.read_text()) == expected
    content = yaml.safe_load((source / "content.yaml").read_text())
    assert content["diagnostics"] == ["Figures"]
    assert json.loads((source / "content.json").read_text()) == content


@pytest.mark.parametrize("fmt", ["png", "pdf"])
def test_content_generation_reads_figure_metadata(tmp_path, fmt):
    """Generated content includes embedded metadata for non-SVG figures."""
    source = tmp_path / "content" / fmt / "catalog" / "model" / "experiment" / "r1"
    source.mkdir(parents=True)
    figure = source / f"figure.{fmt}"
    if fmt == "png":
        metadata = PngImagePlugin.PngInfo()
        metadata.add_text("name", "Example")
        Image.new("RGB", (1, 1)).save(figure, pnginfo=metadata)
    else:
        writer = PdfWriter()
        writer.add_blank_page(width=10, height=10)
        writer.add_metadata({"/Title": "Example"})
        with figure.open("wb") as stream:
            writer.write(stream)
    config = tmp_path / "grouping.yaml"
    config.write_text("diagnostics: {Figures: figure}\n")

    make_contents.main(experiment="catalog/model/experiment/r1", configfile=config, root=tmp_path, format=fmt)

    content = yaml.safe_load((source / "content.yaml").read_text())
    entry = content["files"][f"catalog/model/experiment/r1/figure.{fmt}"]
    assert entry["grouping"] == "Figures"
    assert entry["name" if fmt == "png" else "title"] == "Example"
