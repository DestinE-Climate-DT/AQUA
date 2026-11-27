#!/usr/bin/env python3
"""Write a markdown report of the errors of the latest AQUA analysis run.

The report is read from the AQUA analysis monitor
(https://climate-web.polito.it/aqua-analysis/) and is meant to be pasted into a
GitHub issue. The issue body goes to stdout (or to --output), a suggested issue
title to stderr.

The monitor is protected by basic authentication: the credentials are read from the
environment variables AQUA_MONITOR_USER and AQUA_MONITOR_PASSWORD, never from the
command line.

Example:
    export AQUA_MONITOR_USER=aqua AQUA_MONITOR_PASSWORD=...
    ./aqua_analysis_issue_report.py --output report.md
    ./aqua_analysis_issue_report.py --exp baseline-hist --max-lines 20
"""

import argparse
import base64
import json
import os
import sys
import urllib.error
import urllib.request

DEFAULT_URL = "https://climate-web.polito.it/aqua-analysis"
EXIT_NO_ERRORS = 3  # distinct exit code so that a workflow can skip opening an issue


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--url", default=DEFAULT_URL, help="Base URL of the monitor (default: %(default)s)")
    parser.add_argument("--run-id", help="Report this run id instead of the latest one")
    for key in ("catalog", "model", "exp", "realization", "machine", "owner"):
        parser.add_argument(f"--{key}", help=f"Only consider runs with this {key} when picking the latest")
    parser.add_argument(
        "--max-lines", type=int, default=30, help="Maximum lines kept per traceback (default: %(default)s, 0 for all)"
    )
    parser.add_argument("--title-file", help="Also write the suggested issue title to this file")
    parser.add_argument("-o", "--output", help="Write the report to this file instead of stdout")
    return parser.parse_args()


def fetch_json(url, auth):
    """Download and decode a JSON file from the monitor."""
    request = urllib.request.Request(url, headers={"Authorization": auth, "Cache-Control": "no-cache"})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.load(response)
    except urllib.error.HTTPError as err:
        hint = " (check AQUA_MONITOR_USER and AQUA_MONITOR_PASSWORD)" if err.code == 401 else ""
        sys.exit(f"Cannot read {url}: HTTP {err.code}{hint}")
    except urllib.error.URLError as err:
        sys.exit(f"Cannot read {url}: {err.reason}")


def pick_run(index, args):
    """Return the entry of the runs index to report: the given run id or the latest run."""
    runs = index.get("runs", [])
    if args.run_id:
        runs = [run for run in runs if run["id"] == args.run_id]
    for key in ("catalog", "model", "exp", "realization", "machine", "owner"):
        if getattr(args, key):
            runs = [run for run in runs if run.get(key) == getattr(args, key)]
    if not runs:
        sys.exit("No run matches the request")
    return max(runs, key=lambda run: run.get("run_start") or "")


def fence(text):
    """Wrap text in a code fence longer than any backtick sequence it contains."""
    ticks = "```"
    while ticks in text:
        ticks += "`"
    return f"{ticks}text\n{text}\n{ticks}"


def trim(text, max_lines):
    """Keep the last max_lines lines of a traceback, where the actual exception is."""
    lines = text.strip("\n").split("\n")
    if max_lines and len(lines) > max_lines:
        lines = [f"... ({len(lines) - max_lines} lines omitted)"] + lines[-max_lines:]
    return "\n".join(lines)


def tool_section(error, tool, max_lines):
    """Markdown for one tool with problems: its messages and tracebacks."""
    lines = [f"### `{error['name']}`", ""]
    if tool:
        lines += [
            f"Status `{error['status']}`, exit code {tool.get('exit')}, {tool.get('critical', 0)} critical, "
            f"{tool.get('error', 0)} error, {tool.get('warning', 0)} warning, "
            f"{tool.get('tracebacks', 0)} traceback(s). Log: `{tool.get('log')}`",
            "",
        ]
    messages = error.get("messages") or []
    if messages:
        table = [
            f"{m['count']} × {m['level']} {m['message']}" + (f"  [first at {m['first_at']}]" if m.get("first_at") else "")
            for m in messages
        ]
        lines += [fence("\n".join(table)), ""]
    for tb in error.get("tracebacks") or []:
        lines += [f"<details><summary>{tb['title']}</summary>", "", fence(trim(tb["text"], max_lines)), "", "</details>", ""]
    return lines


def build_report(run, max_lines):
    """Build the markdown body of the issue."""
    summary = run.get("summary") or {}
    tools = {tool["name"]: tool for tool in run.get("tools") or []}
    errors = run.get("errors") or []
    experiment = " / ".join(filter(None, [run.get("catalog"), run.get("model"), run.get("exp"), run.get("realization")]))

    lines = [
        f"## Errors in the AQUA analysis run of {run.get('run_start', '').replace('T', ' ')}",
        "",
        "| | |",
        "|---|---|",
        f"| Experiment | {experiment} |",
        f"| Source | `{run.get('source')}` |",
        f"| Status | `{run.get('status')}` |",
        f"| Machine | {run.get('machine')} ({run.get('host')}) |",
        f"| User | {run.get('owner')} |",
        f"| AQUA version | {run.get('aqua_version')} |",
        f"| Run | {run.get('run_start')} → {run.get('run_end')} ({run.get('duration')}) |",
        f"| Run directory | `{run.get('run_directory')}` |",
        f"| Report | `{run.get('remote_path')}` |",
        "",
    ]
    lines += [
        f"{summary.get('tools_run')} tools run: {summary.get('tools_failed')} failed, "
        f"{summary.get('tools_with_errors')} with errors, {summary.get('tools_clean')} clean. "
        f"{summary.get('critical')} critical, {summary.get('error')} error and "
        f"{summary.get('warning')} warning messages, "
        f"{summary.get('tracebacks')} traceback(s).",
        "",
    ]
    if errors:
        lines += [
            "## Tools with problems",
            "",
            "| Tool | Status | Critical | Error | Warning | Tracebacks |",
            "|---|---|---|---|---|---|",
        ]
        for err in errors:
            tool = tools.get(err["name"], {})
            lines.append(
                f"| `{err['name']}` | {err['status']} | {tool.get('critical', '')} | {tool.get('error', '')} | "
                f"{tool.get('warning', '')} | {tool.get('tracebacks', '')} |"
            )
        lines += ["", "## Details", ""]
        for err in errors:
            lines += tool_section(err, tools.get(err["name"]), max_lines)
    return "\n".join(lines).rstrip("\n") + "\n"


def main():
    args = parse_args()
    user, password = os.environ.get("AQUA_MONITOR_USER"), os.environ.get("AQUA_MONITOR_PASSWORD")
    if not user or not password:
        sys.exit("Set AQUA_MONITOR_USER and AQUA_MONITOR_PASSWORD")
    auth = "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()
    base = args.url.rstrip("/")

    entry = pick_run(fetch_json(f"{base}/data/runs.json", auth), args)
    run = fetch_json(f"{base}/data/runs/{entry['id']}.json", auth)

    if not run.get("errors"):
        sys.stderr.write(f"Run {run['id']} ({run.get('status')}) has no errors to report\n")
        sys.exit(EXIT_NO_ERRORS)

    report = build_report(run, args.max_lines)
    title = f"[Analysis] Errors in the {run.get('model')} {run.get('exp')} run of {run.get('run_start', '')[:10]}"
    sys.stderr.write(f"Suggested title: {title}\n")
    if args.title_file:
        with open(args.title_file, "w", encoding="utf-8") as handle:
            handle.write(title)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as handle:
            handle.write(report)
    else:
        sys.stdout.write(report)


if __name__ == "__main__":
    main()
