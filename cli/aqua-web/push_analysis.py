#!/usr/bin/env python3
"""Collect AQUA analysis figures and publish aqua-web content.

Example: ``python push_analysis.py --no-update --repository local:/tmp/aqua-web output catalog/model/experiment/r1``
"""

import argparse
import getpass
import os
import shutil
import socket
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path

from make_contents import main as make_contents
from push_s3 import S3TransferError, create_client, get_file_from_s3, upload_directory_to_s3, upload_file_to_s3

from aqua.core.logger import log_configure

logger = log_configure(log_level="INFO", log_name="push_analysis")
FORMATS = ("pdf", "png", "svg")


def _run(*command, cwd=None):
    """Run a Git or rsync command without invoking a shell."""
    subprocess.run(command, cwd=cwd, check=True)


def _experiments(exps, ensemble):
    """Resolve an experiment path or read a list of experiments."""
    if exps.is_file():
        for line in exps.read_text().splitlines():
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            parts = line.split()
            count = 4 if ensemble else 3
            if len(parts) not in ((3, 4) if ensemble else (3,)) or any(part in (".", "..") or "/" in part for part in parts):
                raise ValueError(f"Invalid experiment list entry: {line}")
            yield "/".join(parts[:count] if len(parts) == count else [*parts, "r1"])
    else:
        parts = exps.as_posix().split("/")
        if len(parts) not in ((3, 4) if ensemble else (3,)) or any(part in ("", ".", "..") for part in parts):
            raise ValueError("EXPS must be catalog/model/experiment[/realization] or a list file")
        yield "/".join(parts if len(parts) == (4 if ensemble else 3) else [*parts, "r1"])


def _collect_figures(indir, repo, experiment, formats):
    """Copy analysis output and experiment metadata into aqua-web content."""
    source = indir / experiment
    if not source.is_dir():
        raise FileNotFoundError(f"Experiment directory does not exist: {source}")
    for fmt in formats:
        dest = repo / "content" / fmt / experiment
        dest.mkdir(parents=True, exist_ok=True)
        for figure in source.rglob(f"*.{fmt}"):
            shutil.copy2(figure, dest / figure.name)
        stamp = f"{datetime.now().astimezone().isoformat()} {getpass.getuser()} @ {socket.gethostname()}\n"
        (dest / "last_update.txt").write_text(stamp)
        metadata = source / "experiment.yaml"
        if metadata.is_file():
            shutil.copy2(metadata, dest / metadata.name)


def _get_experiments_file(repo, bucket, rsync_target, client):
    """Retrieve the current list so make_contents can merge into it."""
    destination = repo / "content" / "experiments.yaml"
    destination.parent.mkdir(parents=True, exist_ok=True)
    if rsync_target:
        result = subprocess.run(
            ["rsync", "-avz", f"{rsync_target}/content/experiments.yaml", str(destination)],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode:
            logger.warning("Could not retrieve experiments.yaml from %s: %s", rsync_target, result.stderr.strip())
    elif not get_file_from_s3(client, bucket, "content/experiments.yaml", str(destination)):
        logger.info("No remote experiments.yaml found; creating it")


def _publish(repo, experiment, formats, bucket, rsync_target, client):
    """Publish the merged index and the selected experiment's figures."""
    index = repo / "content" / "experiments.yaml"
    if rsync_target:
        if index.is_file():
            _run("rsync", "-avz", str(index), f"{rsync_target}/content/experiments.yaml")
        for fmt in formats:
            relative = f"content/{fmt}/{experiment}/"
            _run("rsync", "-avz", "--relative", "--chmod=D775,F664", relative, f"{rsync_target}/", cwd=repo)
    else:
        if index.is_file():
            upload_file_to_s3(client, bucket, str(index), "content/experiments.yaml")
        for fmt in formats:
            source = repo / "content" / fmt / experiment
            upload_directory_to_s3(client, bucket, str(source), f"content/{fmt}/{experiment}")


def push_analysis(
    indir,
    exps,
    config,
    repository="DestinE-Climate-DT/aqua-web",
    bucket="aqua-web",
    formats=FORMATS,
    ensemble=True,
    update=True,
    rsync_target=None,
    loglevel="INFO",
):
    """Collect, describe, and publish each experiment's analysis output.

    Args:
        indir (str or Path): Root directory containing analysis output.
        exps (str or Path): Experiment path or file listing experiments.
        config (str or Path): Diagnostic grouping YAML file.
        repository (str): GitHub repository or ``local:PATH``.
        bucket (str): Destination S3 bucket.
        formats (tuple[str]): Image formats to publish.
        ensemble (bool): Use the four-level experiment structure.
        update (bool): Commit and push the aqua-web repository update.
        rsync_target (str, optional): Rsync destination in place of S3.
        loglevel (str): AQUA logging level for the publishing commands.
    """
    log_configure(log_level=loglevel, log_name="push_analysis")
    log_configure(log_level=loglevel, log_name="push_s3")
    indir = Path(indir).expanduser().resolve()
    config = Path(config).expanduser().resolve()
    exps = Path(exps).expanduser()
    if exps.is_file():
        exps = exps.resolve()
    if not config.is_file():
        raise FileNotFoundError(f"Configuration file not found: {config}")
    if not indir.is_dir():
        raise FileNotFoundError(f"Input directory does not exist: {indir}")
    experiments = list(_experiments(exps, ensemble))
    if not experiments:
        raise ValueError("No experiments to publish")
    if rsync_target:
        update = False

    with tempfile.TemporaryDirectory(prefix="aqua-web-") as tmp:
        if repository.startswith("local:"):
            repo = Path(repository.removeprefix("local:")).expanduser().resolve()
            if not repo.is_dir():
                raise FileNotFoundError(f"Local repository does not exist: {repo}")
        else:
            repo = Path(tmp) / "aqua-web"
            if update:
                _run("git", "clone", f"git@github.com:{repository}.git", str(repo))
            else:
                repo.mkdir()
        if update:
            _run("git", "pull", "--ff-only", cwd=repo)

        client = None if rsync_target else create_client()
        updated = [
            f"Updated figures in bucket {bucket}",
            f"on {datetime.now().astimezone().isoformat()} for the following experiments:",
        ]
        for experiment in experiments:
            logger.info("Processing %s", experiment)
            _collect_figures(indir, repo, experiment, formats)
            _get_experiments_file(repo, bucket, rsync_target, client)
            for fmt in formats:
                make_contents(
                    force=True,
                    experiment=experiment,
                    configfile=str(config),
                    ensemble=ensemble,
                    loglevel=loglevel,
                    format=fmt,
                    root=repo,
                )
            _publish(repo, experiment, formats, bucket, rsync_target, client)
            updated.append(experiment)
        (repo / "updated.txt").write_text("\n".join(updated) + "\n")

        if update:
            _run("git", "add", "updated.txt", cwd=repo)
            _run("git", "commit", "-m", "update figures", cwd=repo)
            _run("git", "push", cwd=repo)


def main(argv=None):
    """Run the publishing command and return its exit status."""
    parser = argparse.ArgumentParser(description="Publish AQUA analysis figures to aqua-web.")
    parser.add_argument("indir", type=Path, help="Analysis output directory")
    parser.add_argument("exps", help="Experiment path or a text file listing experiments")
    parser.add_argument("-b", "--bucket", default="aqua-web")
    parser.add_argument("-c", "--config", type=Path)
    parser.add_argument("-d", "--no-update", action="store_true")
    parser.add_argument("--no-ensemble", action="store_true")
    parser.add_argument("-f", "--format", default=",".join(FORMATS))
    parser.add_argument("-l", "--loglevel", type=int, choices=range(1, 6), default=2)
    parser.add_argument("-r", "--repository", default="DestinE-Climate-DT/aqua-web")
    parser.add_argument("-s", "--rsync", dest="rsync_target")
    args = parser.parse_args(argv)
    formats = tuple(fmt.strip() for fmt in args.format.split(","))
    if not formats or any(fmt not in FORMATS for fmt in formats):
        parser.error("--format must be a comma-separated selection of pdf,png,svg")
    loglevel = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")[args.loglevel - 1]
    config = args.config
    if config is None:
        root = os.environ.get("AQUA_CONFIG") or str(Path.home() / ".aqua")
        config = Path(root) / "analysis" / "config.grouping.yaml"
    try:
        push_analysis(
            args.indir,
            args.exps,
            config,
            args.repository,
            args.bucket,
            formats,
            not args.no_ensemble,
            not args.no_update,
            args.rsync_target,
            loglevel,
        )
    except S3TransferError as exc:
        return exc.exit_code
    except (FileNotFoundError, ValueError, subprocess.CalledProcessError) as exc:
        logger.error("Publishing failed: %s", exc)
        return exc.returncode if isinstance(exc, subprocess.CalledProcessError) else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
