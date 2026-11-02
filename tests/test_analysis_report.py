"""Tests for the aqua analysis summary report (aqua.core.analysis.report)."""

import os

import pandas as pd
import pytest

from aqua.core.analysis import Analysis, AnalysisReport

pytestmark = pytest.mark.console

RED, RESET = "\x1b[31;20m", "\x1b[0m"

FAILING_LOG = "\n".join(
    [
        f"{RED}2026-10-15 13:57:11 :: OutputSaver :: ERROR    -> Loaded NetCDF: /scratch/a/file1.nc{RESET}",
        f"{RED}2026-10-15 13:57:12 :: OutputSaver :: ERROR    -> Loaded NetCDF: /scratch/b/file2.nc{RESET}",
        "2026-10-15 13:57:13 | smmregrid.Regridder |  WARNING -> Weights not found",
        "2026-10-15 13:57:14,123 - distributed.worker - ERROR - Worker lost",
        "2026-10-15 13:57:15 :: Timeseries CLI :: INFO     -> Saved PNG: /scratch/png/fig.png",
        "Traceback (most recent call last):",
        '  File "cli_timeseries.py", line 10, in <module>',
        "    main()",
        "KeyError: '2t'",
        "2026-10-15 13:59:00 :: Timeseries CLI :: CRITICAL -> Diagnostic aborted",
    ]
)
CLEAN_LOG = "2026-10-15 13:57:11 :: Biases CLI :: INFO     -> Saved NetCDF: /scratch/nc/out.nc\n"


def _write(path, text="", mtime=None):
    """Write a file and optionally set its modification time."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    if mtime is not None:
        os.utime(path, (mtime.timestamp(), mtime.timestamp()))
    return path


def _result(path, exit_code, start):
    """Tool result as recorded by Analysis."""
    return {
        "collection": "c",
        "tool": "t",
        "logfile": str(path),
        "exit_code": exit_code,
        "start": start,
        "end": start + pd.Timedelta(minutes=2),
    }


@pytest.fixture
def run_dir(tmp_path):
    """Run directory with a failing and a clean tool log, an untracked old log and fresh/stale/empty outputs."""
    run_start = pd.Timestamp.now() - pd.Timedelta(minutes=10)
    old = run_start - pd.Timedelta(days=1)

    results = [
        _result(_write(tmp_path / "atmosphere2d-timeseries.log", FAILING_LOG), 1, run_start),
        _result(_write(tmp_path / "atmosphere2d-biases.log", CLEAN_LOG), 0, run_start),
    ]
    _write(tmp_path / "ocean3d-drift.log", FAILING_LOG, mtime=old)  # from an earlier run

    _write(tmp_path / "atmosphere2d" / "png" / "fig.png", "png")
    _write(tmp_path / "atmosphere2d" / "netcdf" / "out.nc", "")  # empty output
    _write(tmp_path / "atmosphere2d" / "pdf" / "old.pdf", "pdf", mtime=old)
    _write(tmp_path / "ocean3d" / "netcdf" / "old.nc", "nc", mtime=old)
    return tmp_path, run_start, results


def test_report_parsing_and_status(run_dir):
    """Messages are parsed from all log formats, grouped, and tools are ranked by status."""
    path, run_start, results = run_dir
    summary = AnalysisReport(str(path), run_start=run_start, tool_results=results).collect()

    # only the logs of the tools run are considered
    assert summary["tools"] == 2
    assert summary["status"] == {"FAILED": 1, "OK": 1}
    failing, clean = summary["logs"]
    assert failing["name"] == "atmosphere2d-timeseries.log"
    assert failing["counts"]["ERROR"] == 3
    assert failing["counts"]["CRITICAL"] == 1
    assert failing["counts"]["WARNING"] == 1
    # the two OutputSaver errors differ only by path and are grouped together
    grouped = {key: entry["count"] for key, entry in failing["messages"].items()}
    assert grouped[("ERROR", "OutputSaver", "Loaded NetCDF: <path>")] == 2
    assert ("WARNING", "smmregrid.Regridder", "Weights not found") not in grouped
    assert failing["tracebacks"][0][-1] == "KeyError: '2t'"
    assert failing["saved"] == {"png": 1}
    assert clean["saved"] == {"netcdf": 1}


@pytest.mark.parametrize(
    "exit_code, log, status",
    [(0, CLEAN_LOG, "OK"), (0, FAILING_LOG, "ERRORS"), (2, CLEAN_LOG, "FAILED")],
)
def test_report_status(tmp_path, exit_code, log, status):
    """The status combines the exit code and the logged errors."""
    run_start = pd.Timestamp.now()
    results = [_result(_write(tmp_path / "tool.log", log), exit_code, run_start)]
    summary = AnalysisReport(str(tmp_path), run_start=run_start, tool_results=results).collect()
    assert summary["status"] == {status: 1}


def test_report_outputs(run_dir):
    """Output files older than the run start are counted as stale, empty files are flagged."""
    path, run_start, results = run_dir
    summary = AnalysisReport(str(path), run_start=run_start, tool_results=results).collect()

    assert summary["files"] == {"png": 1, "nc": 1}
    assert summary["stale_files"] == 2
    assert summary["empty_files"] == 1
    assert summary["outputs"]["ocean3d"]["stale"] == {"nc": 1}

    summary = AnalysisReport(str(path), run_start=run_start, tool_results=results, scan_outputs=False).collect()
    assert summary["outputs"] == {} and summary["files"] == {}


def test_report_markdown_and_timestamped_file(run_dir):
    """The report is written with a run-start timestamp, so successive runs do not overwrite it."""
    path, run_start, results = run_dir
    report = AnalysisReport(
        str(path),
        run_start=run_start,
        tool_results=results,
        metadata={"model": "IFS", "exp": "hist", "realization": "r1"},
        include_warnings=True,
    )
    text = report.to_markdown()

    assert text.startswith("# AQUA analysis report: IFS / hist / r1")
    for section in ["## Summary", "## Tools", "## Output files", "## Errors and warnings"]:
        assert section in text
    assert "KeyError: '2t'" in text
    assert "Weights not found" in text
    assert "ocean3d-drift.log" not in text

    first = report.write()
    assert os.path.basename(first) == f"aqua-analysis-report_{run_start:%Y%m%d-%H%M%S}.md"
    second = AnalysisReport(str(path), run_start=run_start + pd.Timedelta(seconds=1), tool_results=results).write()
    assert first != second and os.path.exists(first) and os.path.exists(second)


def test_report_missing_logfile(tmp_path):
    """A tool whose logfile was never written is reported, not raised."""
    run_start = pd.Timestamp.now()
    summary = AnalysisReport(
        str(tmp_path), run_start=run_start, tool_results=[_result(tmp_path / "ghost.log", 0, run_start)]
    ).collect()
    assert summary["logs"][0]["missing"]
    assert summary["status"] == {"ERRORS": 1}


def test_report_missing_dir(tmp_path):
    """A missing run directory raises an explicit error."""
    with pytest.raises(FileNotFoundError):
        AnalysisReport(str(tmp_path / "missing"), run_start=pd.Timestamp.now(), tool_results=[])


@pytest.fixture
def analysis(tmp_path):
    """Analysis instance with an output directory set."""
    analysis = Analysis(loglevel="WARNING")
    analysis.output_dir = str(tmp_path / "out")
    analysis.model, analysis.exp, analysis.realization = "IFS", "hist", "r1"
    return analysis


def test_analysis_records_tool_results(analysis, tmp_path):
    """Analysis records the exit code of each tool and writes it in the report."""
    script = _write(
        tmp_path / "cli_fail.py",
        "import sys\nprint('2026-10-15 13:57:11 :: Dummy CLI :: ERROR    -> Something broke')\nsys.exit(2)\n",
    )
    exit_code = analysis.run_diagnostic_tool(
        collection="dummy",
        tool="fail",
        script_path=str(script),
        extra_args="",
        logfile=f"{analysis.output_dir}/dummy-fail.log",
    )
    assert exit_code == 2
    assert analysis.tool_results[0]["exit_code"] == 2

    with open(analysis.write_report(), encoding="utf-8") as f:
        text = f.read()
    assert "\u274c **1 of 1 tools failed**" in text
    assert ":x:" not in text and "<sub>" not in text  # readable in a terminal
    assert "Something broke" in text


def test_analysis_setup_checker_failure_report(analysis, tmp_path):
    """A failing setup checker writes a report of its own log only, without scanning outputs."""
    script = _write(
        tmp_path / "cli_setup_checker.py",
        "import sys\nprint('2026-10-15 13:57:11 :: SetupChecker :: CRITICAL -> No data found')\nsys.exit(1)\n",
    )
    _write(tmp_path / "out" / "atmosphere2d-biases.log", FAILING_LOG)  # leftover from an earlier run
    _write(tmp_path / "out" / "atmosphere2d" / "png" / "fig.png", "png")

    with pytest.raises(SystemExit):
        analysis.run_setup_checker(script_path=str(script))

    reports = [f for f in os.listdir(analysis.output_dir) if f.startswith("aqua-analysis-report_")]
    with open(os.path.join(analysis.output_dir, reports[0]), encoding="utf-8") as f:
        text = f.read()
    assert "`setup_checker.log`" in text and "No data found" in text
    assert "atmosphere2d-biases.log" not in text
    assert "## Output files" not in text
