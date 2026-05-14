#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
cli_ensemble_mean_slurm.py
==========================
AQUA wrapper to submit parallel SLURM jobs for the ensemble mean computation.
One job is submitted per variable per experiment, mirroring the structure of
cli_drop_parallel_slurm.py.

Use with caution — can submit many sbatch jobs at once.

Usage
-----
    # Dry-run: print sbatch commands without submitting
    python cli_ensemble_mean_slurm.py -c auto_LRA_overnight.yaml

    # Submit jobs (definitive)
    python cli_ensemble_mean_slurm.py -c auto_LRA_overnight.yaml -d

    # Submit with 8 dask workers per job and max 4 concurrent jobs
    python cli_ensemble_mean_slurm.py -c auto_LRA_overnight.yaml -d -w 8 -p 4

    # Overwrite existing output
    python cli_ensemble_mean_slurm.py -c auto_LRA_overnight.yaml -d -o

    # Run via AQUA Singularity container
    python cli_ensemble_mean_slurm.py -c auto_LRA_overnight.yaml -d -s

Configuration file format (same drop YAML, data section drives the loop)
-------------------------------------------------------------------------
target:
  resolution: r100
  frequency: monthly
  stat: mean
  catalog: climatedt-gen2

paths:
  outdir: /path/to/lra/output
  tmpdir: /path/to/tmp

options:
  loglevel: INFO
  overwrite: false
  compact: xarray

slurm:
  partition: compute
  username:
  account: ab0995
  nodes: 1
  ntasks_per_node: 8
  time: "02:00:00"
  mem: "128G"

# Realizations to include in the ensemble mean.
# Listed here at the top level; can also be overridden per source.
ensemble:
  realizations: [r1, r2, r3, r4, r5]

data:
  IFS-FESOM-10km:
    story-nudging-hist:
      lra-r100-monthly:
        vars: [2t, msl, tprate]
        workers: 8
      # Override realizations for a specific source:
      # other-source:
      #   vars: [2t]
      #   realizations: [r1, r2, r3]
"""

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

import jinja2

from aqua.core.configurer import ConfigPath
from aqua.core.util import get_arg, load_yaml, to_list


# ---------------------------------------------------------------------------
# Helpers (mirrored from cli_drop_parallel_slurm.py)
# ---------------------------------------------------------------------------

def is_job_running(job_name: str, username: str) -> bool:
    """Return True if a job with this name is already in the SLURM queue."""
    output = subprocess.run(
        ["squeue", "-u", username, "--format", "%j"],
        capture_output=True, check=True,
    )
    running = output.stdout.decode("utf-8").splitlines()[1:]
    return job_name in running


def load_jinja_template(template_file: str) -> jinja2.Template:
    """
    Load a Jinja2 template from an absolute path.

    Args:
        template_file: Absolute or relative path to the .j2 template.

    Returns:
        jinja2.Template ready to render.

    Raises:
        FileNotFoundError: if the template file does not exist.
    """
    template_file = os.path.abspath(template_file)
    if not os.path.exists(template_file):
        raise FileNotFoundError(f"Cannot find template file: {template_file}")

    loader = jinja2.FileSystemLoader(searchpath=os.path.dirname(template_file))
    env    = jinja2.Environment(loader=loader, trim_blocks=True, lstrip_blocks=True)
    return env.get_template(os.path.basename(template_file))


# ---------------------------------------------------------------------------
# Core job submission
# ---------------------------------------------------------------------------

def submit_sbatch(
    model: str,
    exp: str,
    source: str,
    varname: str,
    realizations: list[str],
    slurm_dict: dict,
    yaml_file: str,
    template_file: str,
    workers: int = 1,
    definitive: bool = False,
    overwrite: bool = False,
    dependency: str | None = None,
    singularity: bool = False,
    log_dir: str = "log",
) -> str:
    """
    Render the Jinja2 SLURM template and submit (or print) the sbatch script.

    Args:
        model:         Model name.
        exp:           Experiment name.
        source:        Source name.
        varname:       Variable to process in this job.
        realizations:  List of realization IDs, e.g. ['r1','r2','r3','r4','r5'].
        slurm_dict:    SLURM options from the config yaml.
        yaml_file:     Path to the config YAML (passed through to the job script).
        template_file: Path to the aqua_ensemble_mean.j2 Jinja2 template.
        workers:       Number of Dask workers per SLURM node.
        definitive:    If True, actually submit; otherwise just print the sbatch command.
        overwrite:     If True, pass -o flag to ensemble-mean CLI.
        dependency:    SLURM job ID to wait for (afterok dependency).
        singularity:   If True, wrap command in the AQUA container.
        log_dir:       Directory for SLURM stdout/stderr logs.

    Returns:
        SLURM job ID string if submitted, '0' otherwise.
    """
    # Build a unique, readable job name
    job_name      = "_".join([model, exp, source, varname])
    full_job_name = f"ensmean_{job_name}"

    # Resolve AQUA root (two levels up from this file's directory)
    aquapath = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )

    os.makedirs(log_dir, exist_ok=True)

    jinja_dict = {
        "job_name":        full_job_name,
        "username":        slurm_dict.get("username", "a270260"),
        "partition":       slurm_dict.get("partition", "compute"),
        "account":         slurm_dict.get("account", "ab0995"),
        "nodes":           str(slurm_dict.get("nodes", 1)),
        "ntasks_per_node": str(slurm_dict.get("ntasks_per_node", workers)),
        "time":            slurm_dict.get("time", "02:00:00"),
        "memory":          slurm_dict.get("mem", "128G"),
        "log_output":      f"{log_dir}/ensmean_{job_name}_%j.out",
        "log_error":       f"{log_dir}/ensmean_{job_name}_%j.err",
        "dependency":      dependency,
        "singularity":     singularity,
        "machine":         ConfigPath().get_machine(),
        "aqua":            aquapath,
        # Passed through to the aqua ensemble-mean CLI
        "config":          yaml_file,
        "model":           model,
        "exp":             exp,
        "source":          source,
        "varname":         varname,
        "realizations":    " ".join(realizations),  # space-separated for CLI
        "definitive":      definitive,
        "overwrite":       overwrite,
    }

    # Check if already queued
    username = jinja_dict["username"]
    if username and is_job_running(full_job_name, username):
        print(f"  [SKIP] Job '{full_job_name}' is already running in SLURM queue.")
        return "0"

    # Render template
    template = load_jinja_template(template_file)
    rendered = template.render(jinja_dict)

    # Write temporary job script
    tempfile = f"_tmp_ensmean_{job_name}.job"
    with open(tempfile, "w", encoding="utf8") as fh:
        fh.write(rendered)

    sbatch_cmd = ["sbatch", tempfile]

    if definitive:
        try:
            result = subprocess.run(
                sbatch_cmd, capture_output=True, check=True
            ).stdout.decode("utf-8")
            jobid = re.findall(r"\b\d+\b", result)[-1]
            print(f"  [SUBMITTED] {full_job_name} → job {jobid}")
            if os.path.exists(tempfile):
                os.remove(tempfile)
            return jobid
        except subprocess.CalledProcessError as e:
            print(f"  [ERROR] sbatch failed for {full_job_name}")
            print(f"    returncode : {e.returncode}")
            print(f"    stdout     : {e.stdout.decode()}")
            print(f"    stderr     : {e.stderr.decode()}")
            if os.path.exists(tempfile):
                os.remove(tempfile)
            return "0"
    else:
        # Dry-run: print what would be submitted
        print(f"  [DRY-RUN] {' '.join(sbatch_cmd)}")
        print(f"  --- script for {full_job_name} ---")
        print(rendered)
        print()
        if os.path.exists(tempfile):
            os.remove(tempfile)
        return "0"


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_arguments(arguments: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "AQUA ensemble mean parallel SLURM submitter.\n"
            "Submits one job per variable across all models/exps/sources in the config."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-c", "--config", type=str, required=True,
        help="Path to the AQUA DROP-style YAML configuration file.",
    )
    parser.add_argument(
        "-t", "--template", type=str,
        default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "aqua_ensemble_mean.j2"),
        help="Path to the Jinja2 SLURM template. Defaults to aqua_ensemble_mean.j2 next to this script.",
    )
    parser.add_argument(
        "-d", "--definitive", action="store_true",
        help="Actually submit jobs (default: dry-run, only prints sbatch commands).",
    )
    parser.add_argument(
        "-o", "--overwrite", action="store_true",
        help="Overwrite existing ensemble mean output files.",
    )
    parser.add_argument(
        "-s", "--singularity", action="store_true",
        help="Run jobs inside the AQUA Singularity container.",
    )
    parser.add_argument(
        "-w", "--workers", type=int, default=8,
        help="Number of Dask workers per SLURM job (default: 8).",
    )
    parser.add_argument(
        "-p", "--parallel", type=int, default=5,
        help="Max number of simultaneously running SLURM jobs (default: 5)."
             " Every Nth job gets a SLURM dependency on the previous batch.",
    )
    parser.add_argument(
        "--log-dir", type=str, default="log",
        help="Directory for SLURM stdout/stderr log files (default: ./log).",
    )
    return parser.parse_args(arguments)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_arguments(sys.argv[1:])

    config_file  = args.config
    template_file = args.template
    workers      = args.workers
    definitive   = args.definitive
    overwrite    = args.overwrite
    singularity  = args.singularity
    parallel     = args.parallel
    log_dir      = args.log_dir

    print(f"Reading configuration file: {config_file}")
    config = load_yaml(config_file)

    # Global SLURM settings
    slurm_dict = config.get("slurm", {})

    # Global ensemble realizations — can be overridden per source
    global_realizations = to_list(
        config.get("ensemble", {}).get("realizations", ["r1", "r2", "r3", "r4", "r5"])
    )
    print(f"Global ensemble realizations: {global_realizations}")

    if not definitive:
        print("\n*** DRY-RUN MODE — no jobs will be submitted ***\n")

    count       = 0     # total jobs submitted/printed
    jobid       = None
    parent_job  = None  # SLURM dependency chain: every `parallel` jobs → wait for previous batch

    # Iterate: model → exp → source → variable (same depth as drop parallel slurm)
    for model in config["data"]:
        for exp in config["data"][model]:
            for source in config["data"][model][exp]:
                source_cfg = config["data"][model][exp][source]

                # Per-source realization override (falls back to global)
                if "realizations" in source_cfg:
                    realizations = [
                        f"r{r}" if not str(r).startswith("r") else str(r)
                        for r in to_list(source_cfg["realizations"])
                    ]
                else:
                    realizations = global_realizations

                # Per-source worker override
                job_workers = source_cfg.get("workers", workers)

                varnames = to_list(source_cfg.get("vars", []))
                if not varnames:
                    print(f"  [WARN] No vars defined for {model}/{exp}/{source}, skipping.")
                    continue

                print(f"\n{'='*60}")
                print(f"  model={model}  exp={exp}  source={source}")
                print(f"  realizations={realizations}  workers={job_workers}")
                print(f"  variables={varnames}")
                print(f"{'='*60}")

                for varname in varnames:
                    # Every `parallel` jobs, chain a dependency so we don't
                    # flood the SLURM scheduler — mirrors cli_drop_parallel_slurm.py
                    if count > 0 and (count % parallel) == 0:
                        print(f"\n  [CHAIN] Setting dependency on job {jobid}\n")
                        parent_job = str(jobid)

                    count += 1
                    print(f"  Submitting: {model} / {exp} / {source} / {varname}")

                    jobid = submit_sbatch(
                        model=model,
                        exp=exp,
                        source=source,
                        varname=varname,
                        realizations=realizations,
                        slurm_dict=slurm_dict,
                        yaml_file=config_file,
                        template_file=template_file,
                        workers=job_workers,
                        definitive=definitive,
                        overwrite=overwrite,
                        dependency=parent_job,
                        singularity=singularity,
                        log_dir=log_dir,
                    )

    print(f"\n{'='*60}")
    print(f"  Total jobs {'submitted' if definitive else 'prepared (dry-run)'}: {count}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()

