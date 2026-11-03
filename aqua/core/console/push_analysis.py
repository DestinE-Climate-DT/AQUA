#!/usr/bin/env python3
"""Collect AQUA analysis figures and publish them to a remote site.

Example: ``aqua push output catalog/model/experiment/r1``
"""

import argparse
import getpass
import logging
import os
import shutil
import socket
import subprocess
import tempfile

import pandas as pd
from dateutil.tz import tzlocal

from aqua import __version__ as aqua_version
from aqua.core.logger import log_configure

from .make_contents import main as make_contents
from .push_s3 import S3TransferError, create_client, get_file_from_s3, upload_directory_to_s3, upload_file_to_s3

FORMATS = ("pdf", "png", "svg")


def push_parser(parser=None):
    """Add publishing options to a parser, creating one if needed."""
    if parser is None:
        parser = argparse.ArgumentParser(description="Publish AQUA analysis figures to a remote site.")
    parser.add_argument("indir", help="Analysis output directory")
    parser.add_argument("exps", help="Experiment path or a text file listing experiments")
    parser.add_argument("-b", "--bucket", default="aqua-web", help="Name of the S3 bucket to publish to")
    parser.add_argument("-c", "--config", help="Configuration file path")
    parser.add_argument("--no-ensemble", action="store_true", help="Do not assume ensemble structure")
    parser.add_argument("-f", "--format", default=",".join(FORMATS), help=f"Formats to publish (default: {','.join(FORMATS)})")
    parser.add_argument(
        "-l",
        "--loglevel",
        type=str.upper,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="DEBUG, INFO, WARNING, ERROR, CRITICAL (default: INFO)",
    )
    parser.add_argument("-s", "--rsync", dest="rsync_target", help="rsync target directory, e.g. user@host:/path")
    return parser


def _rsync(*args, logger, cwd=None, check=True, options="-avz"):
    """Run rsync, optionally allowing a nonzero exit status."""
    command = ("rsync", options, *args)
    result = subprocess.run(command, cwd=cwd, capture_output=True, text=True, check=False)
    for line in result.stdout.splitlines():
        logger.debug("%s", line)
    if check and result.returncode:
        logger.error("Rsync failed: %s", result.stderr.strip())
        raise subprocess.CalledProcessError(result.returncode, command, output=result.stdout, stderr=result.stderr)
    return result


def _experiments(exps, ensemble):
    """Resolve an experiment path or read a list of experiments."""
    exps = os.path.expanduser(os.fspath(exps))
    if os.path.isfile(exps):
        experiments = []
        with open(exps) as file:
            lines = file.read().splitlines()
        for line in lines:
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            parts = line.split()
            # Analysis may add a simulation-kind column; only path columns are published.
            selected = parts[:4] if ensemble else parts[:3]
            # Keep each column a single safe path component.
            if len(selected) < 3 or any(part in (".", "..") or "/" in part for part in selected):
                raise ValueError(f"Invalid experiment list entry: {line}")
            if ensemble and len(selected) == 3:
                selected.append("r1")
            experiments.append("/".join(selected))
        return experiments
    parts = exps.split("/")
    if len(parts) not in ((3, 4) if ensemble else (3,)) or any(part in ("", ".", "..") for part in parts):
        raise ValueError("EXPS must be catalog/model/experiment[/realization] or a list file")
    # A direct three-part ensemble path also defaults to realization r1.
    if ensemble and len(parts) == 3:
        parts.append("r1")
    return ["/".join(parts)]


def _collect_figures(indir, workspace, experiment, formats):
    """Copy analysis output and experiment metadata into publishable content."""
    source = os.path.join(indir, experiment)
    if not os.path.isdir(source):
        raise FileNotFoundError(f"Experiment directory does not exist: {source}")
    for fmt in formats:
        dest = os.path.join(workspace, "content", fmt, experiment)
        os.makedirs(dest, exist_ok=True)
        for root, _, files in os.walk(source):
            for figure in files:
                if figure.endswith(f".{fmt}"):
                    shutil.copy2(os.path.join(root, figure), os.path.join(dest, figure))
        stamp = f"{pd.Timestamp.now(tz=tzlocal()).isoformat()} {getpass.getuser()} @ {socket.gethostname()}\n"
        with open(os.path.join(dest, "last_update.txt"), "w") as file:
            file.write(stamp)
        metadata = os.path.join(source, "experiment.yaml")
        if os.path.isfile(metadata):
            shutil.copy2(metadata, os.path.join(dest, "experiment.yaml"))


def _get_experiments_file(workspace, bucket, rsync_target, client, logger, loglevel):
    """Retrieve the current experiments.yaml, used by the dashboard to keep track of the experiments to display."""
    destination = os.path.join(workspace, "content", "experiments.yaml")
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    if rsync_target:
        result = _rsync(
            f"{rsync_target}/content/experiments.yaml",
            destination,
            logger=logger,
            check=False,
        )
        if result.returncode:
            if result.returncode == 23 and "No such file or directory" in result.stderr:
                logger.info("No remote experiments.yaml found; creating it")
            else:
                logger.error("Rsync failed: %s", result.stderr.strip())
                raise subprocess.CalledProcessError(result.returncode, result.args, output=result.stdout, stderr=result.stderr)
    elif not get_file_from_s3(client, bucket, "content/experiments.yaml", destination, loglevel=loglevel):
        logger.info("No remote experiments.yaml found; creating it")


def _publish(workspace, experiment, formats, bucket, rsync_target, client, logger, loglevel):
    """Publish the merged index and the selected experiment's figures."""
    index = os.path.join(workspace, "content", "experiments.yaml")
    if rsync_target:
        for fmt in formats:
            relative = f"content/{fmt}/{experiment}/"
            logger.info("Transferring %s figures to %s/%s", fmt, rsync_target, relative)
            # --relative recreates the content/format/experiment path at the remote root.
            _rsync(
                "--relative",
                "--chmod=D775,F664",
                relative,
                f"{rsync_target}/",
                logger=logger,
                cwd=workspace,
            )
        if os.path.isfile(index):
            _rsync(index, f"{rsync_target}/content/experiments.yaml", logger=logger)
    else:
        for fmt in formats:
            source = os.path.join(workspace, "content", fmt, experiment)
            destination = f"content/{fmt}/{experiment}"
            logger.info("Transferring %s figures to s3://%s/%s", fmt, bucket, destination)
            upload_directory_to_s3(client, bucket, source, destination, loglevel=loglevel)
        if os.path.isfile(index):
            upload_file_to_s3(client, bucket, index, "content/experiments.yaml", loglevel=loglevel)


def push_analysis(
    indir,
    exps,
    config,
    bucket="aqua-web",
    formats=FORMATS,
    ensemble=True,
    rsync_target=None,
    loglevel="INFO",
):
    """Collect, describe, and publish each experiment's analysis output.

    Args:
        indir (str or path-like): Root directory containing analysis output.
        exps (str or path-like): Experiment path or file listing experiments.
        config (str or path-like): Diagnostic grouping YAML file.
        bucket (str): Destination S3 bucket.
        formats (tuple[str]): Image formats to publish.
        ensemble (bool): Use the four-level experiment structure.
        rsync_target (str, optional): Rsync destination in place of S3.
        loglevel (str): AQUA logging level for the publishing commands. Defaults to INFO.
    """
    logger = log_configure(log_level=loglevel, log_name="push_analysis")
    logger.info("Running push-analysis with AQUA version %s", aqua_version)
    indir = os.path.realpath(os.path.expanduser(os.fspath(indir)))
    config = os.path.realpath(os.path.expanduser(os.fspath(config)))
    exps = os.path.expanduser(os.fspath(exps))
    if os.path.isfile(exps):
        exps = os.path.realpath(exps)
    if not os.path.isfile(config):
        raise FileNotFoundError(f"Configuration file not found: {config}")
    if not os.path.isdir(indir):
        raise FileNotFoundError(f"Input directory does not exist: {indir}")
    experiments = _experiments(exps, ensemble)
    if not experiments:
        raise ValueError("No experiments to publish")
    for experiment in experiments:
        catalog, model, exp, *members = experiment.split("/")
        logger.info(
            "Target: catalog=%s model=%s experiment=%s realization=%s",
            catalog,
            model,
            exp,
            members[0] if members else "n/a",
        )
    with tempfile.TemporaryDirectory(prefix="aqua-web-") as tmp:
        workspace = tmp
        client = None if rsync_target else create_client()
        for experiment in experiments:
            _collect_figures(indir, workspace, experiment, formats)
            _get_experiments_file(workspace, bucket, rsync_target, client, logger, loglevel)
            for fmt in formats:
                make_contents(
                    force=True,
                    experiment=experiment,
                    configfile=config,
                    ensemble=ensemble,
                    loglevel=loglevel,
                    format=fmt,
                    root=workspace,
                )
            _publish(workspace, experiment, formats, bucket, rsync_target, client, logger, loglevel)
    logger.info("Transfer completed successfully")


def push_options(args):
    """Resolve parsed CLI arguments for the publishing routine."""
    formats = tuple(fmt.strip() for fmt in args.format.split(","))
    if not formats or any(fmt not in FORMATS for fmt in formats):
        raise ValueError(f"--format must be a comma-separated selection of {', '.join(FORMATS)}")
    config = args.config
    if config is None:
        root = os.environ.get("AQUA_CONFIG") or os.path.join(os.path.expanduser("~"), ".aqua")
        config = os.path.join(root, "analysis", "config.grouping.yaml")
    return {
        "indir": args.indir,
        "exps": args.exps,
        "config": config,
        "bucket": args.bucket,
        "formats": formats,
        "ensemble": not args.no_ensemble,
        "rsync_target": args.rsync_target,
        "loglevel": args.loglevel,
    }


def push_execute(args, logger=None):
    """Publish from parsed CLI arguments and return the process exit status."""
    if logger is None:
        logger = logging.getLogger("AQUA")
    try:
        options = push_options(args)
    except ValueError as exc:
        logger.error("%s", exc)
        return 2
    try:
        push_analysis(**options)
    except S3TransferError as exc:
        return exc.exit_code
    except (FileNotFoundError, ValueError, subprocess.CalledProcessError) as exc:
        logger.error("Publishing failed: %s", exc)
        # Keep rsync's exit status so callers can distinguish transfer failures.
        return exc.returncode if isinstance(exc, subprocess.CalledProcessError) else 1
    return 0
