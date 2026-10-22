"""
Summary report of an ``aqua analysis`` run.

Every diagnostic tool launched by ``aqua analysis`` writes its own log file in the run
directory (``<outputdir>/<catalog>/<model>/<exp>/<realization>``), which makes it hard to
see at a glance whether a run had significant problems. ``AnalysisReport`` scans the logs
of the tools run by ``Analysis`` and the output folders, and writes a single timestamped
Markdown report, ready to be pasted into a GitLab/GitHub issue. The report uses Unicode emoji and
no HTML apart from collapsible tracebacks, so that it is readable also from a terminal (e.g. ``less``
or ``glow``) on HPC machines.

The report contains:
    - run metadata (model, exp, dates, host, AQUA version, duration)
    - one status line per tool (exit code, duration, ERROR/CRITICAL/WARNING counts, tracebacks)
    - the distinct error messages of each log, grouped and counted
    - the number of figures and NetCDF files written in each output folder, separating the
      files produced by this run from those left over by earlier runs in the same folder

Example:
    import pandas as pd
    from aqua.core.analysis import AnalysisReport

    results = [{"collection": "atmosphere2d", "tool": "biases", "logfile": "run/atmosphere2d-biases.log",
                "exit_code": 0, "start": pd.Timestamp("2026-10-15 12:55"), "end": pd.Timestamp("2026-10-15 12:57")}]
    report = AnalysisReport("run", run_start=pd.Timestamp("2026-10-15 12:55"), tool_results=results)
    report_file = report.write()
"""

import getpass
import os
import re
import socket
from collections import Counter

import pandas as pd

from aqua.core.logger import log_configure
from aqua.core.version import __version__ as aqua_version

TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
REPORT_PREFIX = "aqua-analysis-report"

ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*m")
# AQUA logger "<time> :: <name> :: <LEVEL> -> <msg>" and smmregrid "<time> | <name> | <LEVEL> -> <msg>"
AQUA_LINE = re.compile(
    r"^(?P<time>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s*(?:::|\|)\s*(?P<name>.*?)\s*(?:::|\|)\s*"
    r"(?P<level>DEBUG|INFO|WARNING|ERROR|CRITICAL)\s*->\s?(?P<msg>.*)$"
)
# Plain python/dask logging, e.g. "2026-10-15 13:57:11,123 - distributed.worker - ERROR - msg"
GENERIC_LINE = re.compile(r"^(?:\S+ \S+ - )?(?P<name>[\w.]+) - (?P<level>WARNING|ERROR|CRITICAL) - (?P<msg>.*)$")
TRACEBACK_START = "Traceback (most recent call last):"
# OutputSaver lines such as "Saved PNG: /path/to/file.png" (only visible at INFO level or lower)
SAVED_LINE = re.compile(r"^Saved (?P<kind>\w+):")
# used to group messages differing only by file paths or memory addresses
PATH_PATTERN = re.compile(r"(?<![\w.])/[^\s'\",;:)]+")
ADDR_PATTERN = re.compile(r"0x[0-9a-fA-F]+")

FIGURE_EXTENSIONS = ("png", "pdf", "svg")
DATA_EXTENSIONS = ("nc",)

# tool status, ordered by severity
FAILED = "FAILED"
ERRORS = "ERRORS"
OK = "OK"
STATUS_ORDER = [FAILED, ERRORS, OK]
# Unicode emoji instead of GitHub shortcodes (e.g. ":x:"), which are not rendered in a terminal
STATUS_ICON = {FAILED: "\u274c", ERRORS: "\u26a0\ufe0f", OK: "\u2705"}


class AnalysisReport:
    """Collect the logs and outputs of an ``aqua analysis`` run into a Markdown report."""

    def __init__(
        self,
        run_dir,
        run_start,
        tool_results,
        run_end=None,
        metadata=None,
        include_warnings=False,
        scan_outputs=True,
        max_messages=20,
        loglevel="WARNING",
    ):
        """
        Initialize the report.

        Args:
            run_dir (str): Run directory containing the tool logs and output folders.
            run_start (pd.Timestamp): Start of the run. Output files older than this are
                considered leftovers of earlier runs in the same folder.
            tool_results (list): One dict per tool run, as recorded by ``Analysis``, with keys
                ``collection``, ``tool``, ``logfile``, ``exit_code``, ``start``, ``end``.
                Only these logfiles are parsed, other logs in the run directory are ignored.
            run_end (pd.Timestamp, optional): End of the run. Defaults to the report creation time.
            metadata (dict, optional): Run metadata (model, exp, ...) shown at the top of the report.
            include_warnings (bool, optional): List the distinct WARNING messages too. Defaults to False.
            scan_outputs (bool, optional): Count the output files of the run. Defaults to True.
            max_messages (int, optional): Maximum number of distinct messages listed per log. Defaults to 20.
            loglevel (str, optional): Logging level. Defaults to "WARNING".
        """
        self.loglevel = loglevel
        self.logger = log_configure(log_level=loglevel, log_name="AnalysisReport")

        self.run_dir = os.path.abspath(os.path.expandvars(run_dir))
        if not os.path.isdir(self.run_dir):
            raise FileNotFoundError(f"Analysis run directory {self.run_dir} not found.")

        self.run_start = pd.Timestamp(run_start)
        self.run_end = pd.Timestamp(run_end) if run_end is not None else None
        self.tool_results = tool_results
        self.metadata = metadata or {}
        self.include_warnings = include_warnings
        self.scan_outputs = scan_outputs
        self.max_messages = max_messages

        self.logs = None
        self.outputs = None

    def collect(self):
        """
        Parse the tool logs and scan the output folders of the run directory.

        Returns:
            dict: Summary with the per-tool results, the output counts and the overall totals.
        """
        self.run_end = self.run_end or pd.Timestamp.now()
        self.logs = [self._parse_log(result) for result in self.tool_results]
        self.logs.sort(key=lambda log: (STATUS_ORDER.index(log["status"]), log["name"]))
        self.outputs = self._scan_outputs() if self.scan_outputs else {}
        return self.summary()

    def summary(self):
        """
        Overall totals of the collected run.

        Returns:
            dict: Counts of tools per status, messages per level and output files per extension.
        """
        if self.logs is None:
            self.collect()
        levels = Counter()
        for log in self.logs:
            levels.update(log["counts"])
        files = Counter()
        stale = empty = 0
        for folder in self.outputs.values():
            files.update(folder["fresh"])
            stale += sum(folder["stale"].values())
            empty += folder["empty"]
        return {
            "tools": len(self.logs),
            "status": dict(Counter(log["status"] for log in self.logs)),
            "levels": dict(levels),
            "tracebacks": sum(len(log["tracebacks"]) for log in self.logs),
            "files": dict(files),
            "stale_files": stale,
            "empty_files": empty,
            "logs": self.logs,
            "outputs": self.outputs,
        }

    def to_markdown(self):
        """
        Render the report as Markdown (GitLab/GitHub flavoured).

        Returns:
            str: The Markdown report.
        """
        summary = self.summary()
        lines = [f"# AQUA analysis report: {self._title()}", ""]
        lines += self._md_metadata()
        lines += self._md_summary(summary)
        lines += self._md_tools()
        lines += self._md_outputs()
        lines += self._md_messages()
        lines.append(f"*Generated by AQUA {aqua_version} on {pd.Timestamp.now():{TIME_FORMAT}}*")
        return "\n".join(lines) + "\n"

    def write(self, filename=None):
        """
        Write the Markdown report in the run directory.

        Args:
            filename (str, optional): Output file. Defaults to
                ``<run_dir>/aqua-analysis-report_<run start as YYYYmmdd-HHMMSS>.md``,
                so that reports of successive runs in the same folder do not overwrite each other.

        Returns:
            str: Path of the written report.
        """
        text = self.to_markdown()
        if filename is None:
            filename = os.path.join(self.run_dir, f"{REPORT_PREFIX}_{self.run_start:%Y%m%d-%H%M%S}.md")
        with open(filename, "w", encoding="utf-8") as f:
            f.write(text)
        self.logger.info("Analysis report written to %s", filename)
        return filename

    def _parse_log(self, result):
        """Parse the log of a tool run, grouping messages by level, logger name and normalised text."""
        path = os.path.abspath(result["logfile"])
        info = {
            "path": path,
            "name": os.path.basename(path),
            "exit_code": result.get("exit_code"),
            "duration": result["end"] - result["start"],
            "missing": not os.path.isfile(path),
            "counts": Counter(),
            "messages": {},
            "tracebacks": [],
            "saved": Counter(),
        }
        if info["missing"]:
            self.logger.warning("Logfile %s not found, it will be reported as missing.", path)
        else:
            self._read_log(path, info)
        info["status"] = self._status(info)
        return info

    def _read_log(self, path, info):
        """Fill message counts, distinct messages, tracebacks and saved files from a logfile."""
        traceback = None
        with open(path, encoding="utf-8", errors="replace") as f:
            for raw in f:
                line = ANSI_ESCAPE.sub("", raw.rstrip("\n"))

                if traceback is not None:
                    # a traceback ends at the first non-indented line, i.e. the exception itself
                    traceback.append(line)
                    if line and not line[0].isspace():
                        info["tracebacks"].append(traceback)
                        traceback = None
                    continue
                if line.startswith(TRACEBACK_START):
                    traceback = [line]
                    continue

                match = AQUA_LINE.match(line) or GENERIC_LINE.match(line)
                if not match:
                    continue
                fields = match.groupdict()
                level, name, msg = fields["level"], fields["name"].strip(), fields["msg"].strip()
                info["counts"][level] += 1
                if level == "INFO":
                    saved = SAVED_LINE.match(msg)
                    if saved:
                        info["saved"][saved.group("kind").lower()] += 1
                    continue
                if level == "DEBUG" or (level == "WARNING" and not self.include_warnings):
                    continue

                key = (level, name, ADDR_PATTERN.sub("<addr>", PATH_PATTERN.sub("<path>", msg)))
                entry = info["messages"].setdefault(key, {"count": 0, "example": msg, "first": fields.get("time")})
                entry["count"] += 1

        if traceback is not None:  # log truncated in the middle of a traceback
            info["tracebacks"].append(traceback)

    @staticmethod
    def _status(log):
        """Tool status from the exit code and the logged errors."""
        if log["exit_code"] != 0:
            return FAILED
        if log["missing"] or log["counts"]["ERROR"] or log["counts"]["CRITICAL"] or log["tracebacks"]:
            return ERRORS
        return OK

    def _scan_outputs(self):
        """Count output files per first-level folder and extension, split into fresh and stale."""
        outputs = {}
        for entry in sorted(os.scandir(self.run_dir), key=lambda e: e.name):
            if not entry.is_dir():
                continue
            folder = {"fresh": Counter(), "stale": Counter(), "empty": 0}
            for root, _, files in os.walk(entry.path):
                for fname in files:
                    if fname.startswith("."):
                        continue
                    stat = os.stat(os.path.join(root, fname))
                    ext = fname.rsplit(".", 1)[-1].lower() if "." in fname else "other"
                    ext = ext if ext in FIGURE_EXTENSIONS + DATA_EXTENSIONS else "other"
                    fresh = pd.Timestamp.fromtimestamp(stat.st_mtime) >= self.run_start
                    folder["fresh" if fresh else "stale"][ext] += 1
                    if fresh and stat.st_size == 0:
                        folder["empty"] += 1
            outputs[entry.name] = folder
        return outputs

    def _title(self):
        """Short identifier of the run used as report title."""
        keys = ["model", "exp", "realization"]
        if all(self.metadata.get(k) for k in keys):
            return " / ".join(str(self.metadata[k]) for k in keys)
        return self.run_dir

    def _md_metadata(self):
        """Markdown table with the run metadata."""
        rows = [(k.capitalize(), v) for k, v in self.metadata.items() if v is not None]
        rows += [
            ("Run directory", f"`{self.run_dir}`"),
            ("Run start", f"{self.run_start:{TIME_FORMAT}}"),
            ("Run end", f"{self.run_end:{TIME_FORMAT}}"),
            ("Duration", _format_duration(self.run_end - self.run_start)),
            ("Host", socket.gethostname()),
            ("User", getpass.getuser()),
            ("AQUA version", aqua_version),
        ]
        lines = ["| | |", "|---|---|"]
        lines += [f"| {key} | {_md_cell(value)} |" for key, value in rows]
        return lines + [""]

    def _md_summary(self, summary):
        """Markdown overview of the run status."""
        status, levels, files = summary["status"], summary["levels"], summary["files"]
        failed, errors = status.get(FAILED, 0), status.get(ERRORS, 0)
        if failed:
            headline = f"{STATUS_ICON[FAILED]} **{failed} of {summary['tools']} tools failed**"
        elif errors:
            headline = f"{STATUS_ICON[ERRORS]} **{errors} of {summary['tools']} tools logged errors**"
        else:
            headline = f"{STATUS_ICON[OK]} **All {summary['tools']} tools completed without errors**"

        lines = [
            "## Summary",
            "",
            headline,
            "",
            f"- **Tools**: {summary['tools']} run, {failed} failed (non-zero exit code), "
            f"{errors} with logged errors, {status.get(OK, 0)} clean",
            f"- **Log messages**: {levels.get('CRITICAL', 0)} CRITICAL, {levels.get('ERROR', 0)} ERROR, "
            f"{levels.get('WARNING', 0)} WARNING, {summary['tracebacks']} tracebacks",
        ]
        if self.scan_outputs:
            fresh = ", ".join(f"{files[ext]} {ext}" for ext in sorted(files)) or "none"
            lines.append(f"- **Files written by this run**: {fresh}")
        if summary["empty_files"]:
            lines.append(f"- {STATUS_ICON[ERRORS]} **Empty files written by this run**: {summary['empty_files']}")
        if summary["stale_files"]:
            lines.append(f"- **Files left over from earlier runs** (older than the run start): {summary['stale_files']}")
        return lines + [""]

    def _md_tools(self):
        """Markdown table with one row per tool log."""
        lines = [
            "## Tools",
            "",
            "| Status | Log | Exit | Duration | CRITICAL | ERROR | WARNING | Tracebacks | Saved files |",
            "|---|---|---|---|---|---|---|---|---|",
        ]
        for log in self.logs:
            saved = ", ".join(f"{n} {kind}" for kind, n in sorted(log["saved"].items())) or "-"
            name = f"`{log['name']}`" + (" (missing)" if log["missing"] else "")
            counts = log["counts"]
            lines.append(
                f"| {STATUS_ICON[log['status']]} {log['status']} | {name} | {log['exit_code']} "
                f"| {_format_duration(log['duration'])} | {counts['CRITICAL']} | {counts['ERROR']} "
                f"| {counts['WARNING']} | {len(log['tracebacks'])} | {saved} |"
            )
        return lines + [""]

    def _md_outputs(self):
        """Markdown table with the output files per folder."""
        if not self.outputs:
            return []
        extensions = list(FIGURE_EXTENSIONS + DATA_EXTENSIONS) + ["other"]
        lines = [
            "## Output files",
            "",
            "Files written by this run in each output folder; older files are counted as *stale*.",
            "",
            "| Folder | " + " | ".join(extensions) + " | Empty | Stale |",
            "|---|" + "---|" * (len(extensions) + 2),
        ]
        for name, folder in self.outputs.items():
            counts = " | ".join(str(folder["fresh"][ext]) for ext in extensions)
            lines.append(f"| `{name}` | {counts} | {folder['empty']} | {sum(folder['stale'].values())} |")
        return lines + [""]

    def _md_messages(self):
        """Markdown section with the distinct messages and tracebacks of each problematic log."""
        logs = [log for log in self.logs if log["messages"] or log["tracebacks"]]
        if not logs:
            return []
        title = "Errors and warnings" if self.include_warnings else "Errors"
        lines = [f"## {title}", ""]
        severity = {"CRITICAL": 0, "ERROR": 1, "WARNING": 2}
        for log in logs:
            messages = sorted(log["messages"].items(), key=lambda kv: (severity[kv[0][0]], -kv[1]["count"]))
            lines += [f"### `{log['name']}` ({log['status']})", "", "```text"]
            for (level, name, _), entry in messages[: self.max_messages]:
                text = _truncate(entry["example"], 400)
                lines.append(f"{entry['count']:>5} x {level:<8} {name}: {text}  [first at {entry['first'] or '?'}]")
            if len(messages) > self.max_messages:
                lines.append(f"... and {len(messages) - self.max_messages} more distinct messages")
            lines += ["```", ""]
            for traceback in log["tracebacks"]:
                # exception outside <summary>, so that it is visible also when HTML is not rendered
                lines += [
                    f"**Traceback**: `{_truncate(traceback[-1], 200)}`",
                    "",
                    "<details><summary>Full traceback</summary>",
                    "",
                    "```text",
                    *traceback[-40:],
                    "```",
                    "",
                    "</details>",
                    "",
                ]
        return lines


def _format_duration(delta):
    """Format a pd.Timedelta as H:MM:SS."""
    seconds = max(int(delta.total_seconds()), 0)
    return f"{seconds // 3600}:{seconds % 3600 // 60:02d}:{seconds % 60:02d}"


def _truncate(text, length):
    """Truncate a string to a maximum length."""
    return text if len(text) <= length else text[: length - 3] + "..."


def _md_cell(value):
    """Escape a value to be used inside a Markdown table cell."""
    return str(value).replace("|", "\\|").replace("<", "&lt;").replace(">", "&gt;")
