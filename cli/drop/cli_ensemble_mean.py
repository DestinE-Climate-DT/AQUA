#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
cli_ensemble_mean.py
====================
AQUA ``aqua ensemble-mean`` CLI entry point.

This script is called by each SLURM job submitted by cli_ensemble_mean_slurm.py,
one invocation per variable. It:

  1. Loads one realization at a time via Reader (lazy / dask-backed).
  2. Stacks realizations along a synthetic dimension and computes the mean.
  3. Writes output month-by-month using Drop.write_chunk(), compacting to
     yearly files with Drop._concat_var_year().
  4. Registers (or updates) the catalog entry via Drop.create_catalog_entry().

The output path and filename follow exactly the DROP OutputPathBuilder convention
with realization='ensemble', so the result is discoverable via:

    Reader(catalog=..., model=..., exp=..., source='lra-r100-monthly',
           realization='ensemble')

Usage (invoked by SLURM template, can also be run manually)
-----
    # Dry-run for variable 2t
    aqua ensemble-mean -c config.yaml -m IFS-FESOM-10km -e story-nudging-hist \\
                       -s lra-r100-monthly -v 2t --realizations r1 r2 r3 r4 r5

    # Definitive run with 4 workers
    aqua ensemble-mean -c config.yaml -m IFS-FESOM-10km -e story-nudging-hist \\
                       -s lra-r100-monthly -v 2t --realizations r1 r2 r3 r4 r5 \\
                       -d -w 4

    # Overwrite + debug logging
    aqua ensemble-mean -c config.yaml -m IFS-FESOM-10km -e story-nudging-hist \\
                       -s lra-r100-monthly -v 2t --realizations r1 r2 r3 r4 r5 \\
                       -d -o -l DEBUG
"""

import argparse
import os
import sys
from time import time

import xarray as xr

from aqua import Reader
from aqua.core.logger import log_configure
from aqua.core.util import load_yaml, to_list
from aqua.core.drop import CatalogEntryBuilder, Drop
from aqua.core.drop.drop_util import move_tmp_files
from dask.distributed import Client

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_arguments(arguments: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute ensemble mean for a single variable and register the catalog entry.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-c", "--config", type=str, required=True,
        help="AQUA DROP-style YAML configuration file.",
    )
    parser.add_argument(
        "-m", "--model", type=str,
        help="Model name (overrides config file).",
    )
    parser.add_argument(
        "-e", "--exp", type=str,
        help="Experiment name (overrides config file).",
    )
    parser.add_argument(
        "-s", "--source", type=str,
        help="Source name (overrides config file).",
    )
    parser.add_argument(
        "-v", "--varname", type=str, required=True,
        help="Variable name to process (e.g. '2t', 'msl').",
    )
    parser.add_argument(
        "--realizations", type=str, nargs="+",
        help="Space-separated list of realization IDs (e.g. r1 r2 r3 r4 r5). "
             "Overrides config file.",
    )
    parser.add_argument(
        "-d", "--definitive", action="store_true",
        help="Write output files and register catalog entry (default: dry-run).",
    )
    parser.add_argument(
        "-o", "--overwrite", action="store_true",
        help="Overwrite existing output files.",
    )
    parser.add_argument(
        "-w", "--workers", type=int, default=1,
        help="Number of Dask workers (default: 1 = no Dask cluster).",
    )
    parser.add_argument(
        "-l", "--loglevel", type=str, default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO).",
    )
    parser.add_argument(
        "--only-catalog", action="store_true",
        help="Skip data processing; only (re-)create the catalog entry.",
    )
    return parser.parse_args(arguments)


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def resolve_config(args: argparse.Namespace, config: dict) -> dict:
    """
    Merge CLI arguments with the YAML config, CLI taking precedence.

    Returns a flat dict with all resolved parameters.
    """
    target  = config.get("target", {})
    paths   = config.get("paths",  {})
    options = config.get("options", {})
    ensemble_cfg = config.get("ensemble", {})

    # Catalog
    catalog = target.get("catalog", None)

    # Model / exp / source: CLI wins, then config target, then error
    model  = args.model  or target.get("model",  None)
    exp    = args.exp    or target.get("exp",     None)
    source = args.source or target.get("source",  None)

    # Realizations: CLI wins, then per-source block, then global ensemble block
    if args.realizations:
        realizations = args.realizations
    else:
        realizations = to_list(
            ensemble_cfg.get("realizations", ["r1", "r2", "r3", "r4", "r5"])
        )
    # Normalise: allow integer realizations (1 → 'r1')
    realizations = [
        f"r{r}" if not str(r).startswith("r") else str(r)
        for r in realizations
    ]

    return {
        "catalog":      catalog,
        "model":        model,
        "exp":          exp,
        "source":       source,
        "realizations": realizations,
        "resolution":   target.get("resolution", "r100"),
        "frequency":    target.get("frequency",  "monthly"),
        "stat":         target.get("stat",        "mean"),
        "region":       target.get("region",      None),     # None → 'global'
        "outdir":       paths.get("outdir",       None),
        "tmpdir":       paths.get("tmpdir",       "/tmp/aqua_ensmean"),
        "overwrite":    args.overwrite or options.get("overwrite", False),
        "compact":      options.get("compact", "xarray"),
        "loglevel":     args.loglevel,
        "workers":      args.workers,
        "definitive":   args.definitive,
    }


# ---------------------------------------------------------------------------
# Step 1 — ensemble mean (lazy)
# ---------------------------------------------------------------------------

def compute_ensemble_mean(
    catalog: str,
    model: str,
    exp: str,
    source: str,
    realizations: list[str],
    var: str,
    loglevel: str,
    logger,
) -> xr.DataArray:
    """
    Load one variable from each realization, stack, and return the lazy mean.

    Returns an xr.DataArray (not Dataset) so that write_variable() converts
    it to a single-variable Dataset for write_chunk().
    """
    arrays = []
    for r in realizations:
        logger.info("  Loading realization %s, variable '%s' ...", r, var)
        reader = Reader(
            catalog=catalog,
            model=model,
            exp=exp,
            source=source,
            realization=r,
            loglevel=loglevel,
        )
        ds = reader.retrieve(var=var)
        da = ds[var].assign_coords(realization=r)
        arrays.append(da)

    logger.info("  Stacking %d realizations ...", len(arrays))
    stacked = xr.concat(arrays, dim="realization")

    logger.info("  Computing lazy ensemble mean ...")
    ens_mean = stacked.mean(dim="realization", keep_attrs=True).compute()
    ens_mean.attrs["ensemble_realizations"] = ", ".join(realizations)
    ens_mean.attrs["ensemble_method"]       = "arithmetic mean"
    ens_mean.name = var   # preserve variable name for to_dataset(name=var)

    return ens_mean


# ---------------------------------------------------------------------------
# Step 2 — Drop instance
# ---------------------------------------------------------------------------

def build_drop(cfg: dict) -> Drop:
    """
    Build a Drop instance configured for the 'ensemble' pseudo-realization.

    var is a required parameter in Drop.__init__ even though we bypass
    retrieve() and drop_generator(). We pass the actual variable name so
    that get_filename() and write_chunk() produce correctly named files.

    nproc drives the Dask cluster inside write_chunk():
      nproc=1 → synchronous (dask.config scheduler='synchronous')
      nproc>1 → LocalCluster with nproc workers
    """
    if cfg["outdir"] is None:
        raise KeyError(
            "outdir is not set. Specify it in the config file under paths.outdir "
            "or set the LRA_PATH environment variable."
        )

    return Drop(
        catalog=cfg["catalog"],
        model=cfg["model"],
        exp=cfg["exp"],
        source=cfg["source"],
        var=cfg["var"],            # single variable for this job
        resolution=cfg["resolution"],
        frequency=cfg["frequency"],
        stat=cfg["stat"],
        outdir=cfg["outdir"],
        tmpdir=cfg["tmpdir"],
        overwrite=cfg["overwrite"],
        definitive=cfg["definitive"],
        loglevel=cfg["loglevel"],
        nproc=cfg["workers"],
        compact=cfg["compact"],
        realization="ensemble",    # ← pseudo-realization; sets dir + filename
        region=None,               # global
    )

# ---------------------------------------------------------------------------
# Step 3 — write variable month-by-month
# ---------------------------------------------------------------------------

def write_variable(
    drop: Drop,
    var: str,
    da: xr.DataArray,
    overwrite: bool,
    logger,
) -> None:
    """
    Write a DataArray to monthly NetCDF files via Drop.write_chunk(), then
    compact to yearly files via Drop._concat_var_year().

    Mirrors _write_var_catalog() from drop.py but operates on pre-computed
    ensemble data instead of an FDB/intake retrieve pipeline.

    File naming (from OutputPathBuilder.build_filename):
      {var}_{catalog}_{model}_{exp}_ensemble_{resolution}_{frequency}_{stat}_{region}_{YYYY}{MM}.nc
    after compaction:
      {var}_{catalog}_{model}_{exp}_ensemble_{resolution}_{frequency}_{stat}_{region}_{YYYY}.nc
    """
    from aqua.core.util.io_util import file_is_complete

    years = sorted(set(da.time.dt.year.values))
    logger.info("Variable '%s': writing %d year(s): %s – %s",
                var, len(years), years[0], years[-1])

    for year in years:
        # Skip complete yearly file (post-compact) unless --overwrite
        yearfile = drop.get_filename(var, year=year, month=None)
        if os.path.exists(yearfile) and file_is_complete(yearfile, loglevel=drop.loglevel):
            if not overwrite:
                logger.info("  Year %d already complete — skipping.", year)
                continue
            logger.warning("  Year %d exists — overwriting.", year)

        year_data = da.sel(time=da.time.dt.year == year)
        months    = sorted(set(year_data.time.dt.month.values))

        for month in months:
            outfile = drop.get_filename(var, year=year, month=month)
            tmpfile = drop.get_filename(var, year=year, month=month, tmp=True)

            # Skip complete monthly files unless --overwrite
            if os.path.exists(outfile) and file_is_complete(outfile, loglevel=drop.loglevel):
                if not overwrite:
                    logger.info("  %d-%02d already complete — skipping.", year, month)
                    continue

            month_data = year_data.sel(time=year_data.time.dt.month == month)

            # write_chunk() reads data.name for VAR_ENCODING → must be Dataset
            #month_ds = month_data.to_dataset(name=var)
            logger.info("  Writing %d-%02d → %s", year, month, os.path.basename(tmpfile))
            t0 = time()
            drop.write_chunk(month_data, tmpfile)
            logger.info("  Chunk written in %.1f s", time() - t0)

            # Move {var}_..._YYYYMM_tmp.nc → {var}_..._YYYYMM.nc
            move_tmp_files(drop.tmpdir, drop.outdir)

        # Compact 12 monthly → 1 yearly file
        if drop.compact:
            drop._concat_var_year(var, year)


# ---------------------------------------------------------------------------
# Step 4 — catalog registration
# ---------------------------------------------------------------------------

def register_catalog_entry(drop: Drop, cfg: dict, logger) -> None:
    """
    Inject a stub Reader so that _define_source_grid_name() can resolve
    src_grid_name without needing a full FDB retrieve, then call
    create_catalog_entry().

    create_catalog_entry() writes/updates:
      {configdir}/catalogs/{catalog}/catalog/{model}/{exp}.yaml

    CatalogEntryBuilder.create_entry_name() returns 'lra-r100-monthly'
    for resolution='r100' + frequency='monthly'. The 'ensemble' realization
    appears in the urlpath Jinja parameter {{ realization }}, so the new
    entry extends the existing source without creating a separate source name.
    """
    logger.info("Registering catalog entry ...")

    stub = Reader(
        catalog=cfg["catalog"],
        model=cfg["model"],
        exp=cfg["exp"],
        source=cfg["source"],
        realization=cfg["realizations"][0],  # any existing realization
        loglevel="WARNING",
    )
    drop.reader = stub   # inject so _define_source_grid_name() works

    drop.create_catalog_entry()

    entry_name = drop.catbuilder.create_entry_name()
    logger.info("Catalog entry '%s' updated.", entry_name)
    logger.info(
        "Access via: Reader(catalog='%s', model='%s', exp='%s', "
        "source='%s', realization='ensemble')",
        cfg["catalog"], cfg["model"], cfg["exp"], entry_name,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args   = parse_arguments(sys.argv[1:])
    logger = log_configure(args.loglevel, "ensemble-mean")

    # --- load config and resolve all parameters ---
    logger.info("Loading config: %s", args.config)
    raw_config = load_yaml(args.config)
    cfg = resolve_config(args, raw_config)
    cfg["var"] = args.varname   # single variable for this job

    # Validate mandatory parameters
    for key in ("catalog", "model", "exp", "source", "outdir"):
        if not cfg[key]:
            logger.error("Missing required parameter '%s'. "
                         "Set it in the config file or pass via CLI.", key)
            sys.exit(1)

    logger.info("=" * 60)
    logger.info("AQUA ensemble-mean CLI")
    # ... (omitted print statements for brevity) ...
    logger.info("  workers      : %d", cfg["workers"])
    logger.info("  definitive   : %s", cfg["definitive"])
    logger.info("=" * 60)

    # ---> NEW CODE: Start Dask Client <---
    # AQUA's drop.write_chunk expects a distributed client for memory sampling.
    if cfg["workers"] > 1:
        client = Client(n_workers=cfg["workers"])
        logger.info("Started Dask Client with %d workers. Dashboard: %s", 
                    cfg["workers"], client.dashboard_link)
    else:
        # Start a minimal client even for 1 worker to satisfy the MemorySampler dependency
        client = Client(n_workers=1, threads_per_worker=1)
        logger.info("Started minimal Dask Client (1 worker) for memory profiling.")
    # -------------------------------------

    # --- build Drop (always needed for path/catalog machinery) ---
    drop = build_drop(cfg)

    ## Inject stub reader for metadata/history handling
    drop.reader = Reader(
        catalog=cfg["catalog"],
        model=cfg["model"],
        exp=cfg["exp"],
        source=cfg["source"],
        realization=cfg["realizations"][0],
        loglevel="WARNING",
    )

    logger.info("Output directory: %s", drop.outdir)

    # --- only-catalog shortcut ---
    if args.only_catalog:
        if not cfg["definitive"]:
            logger.warning("--only-catalog has no effect without --definitive.")
        register_catalog_entry(drop, cfg, logger)
        return

    # --- Step 1: compute ensemble mean (lazy DataArray) ---
    ens_da = compute_ensemble_mean(
        catalog=cfg["catalog"],
        model=cfg["model"],
        exp=cfg["exp"],
        source=cfg["source"],
        realizations=cfg["realizations"],
        var=cfg["var"],
        loglevel=cfg["loglevel"],
        logger=logger,
    )
    logger.info("Ensemble DataArray: shape=%s, time=%d steps",
                dict(ens_da.sizes), len(ens_da.time))
    

    # --- dry-run: report what would be written ---
    if not cfg["definitive"]:
        logger.warning("DRY-RUN — no files written. Re-run with -d to produce output.")
        for year in sorted(set(ens_da.time.dt.year.values)):
            logger.info("  Would write year %d → %s",
                        year, drop.get_filename(cfg["var"], year=year))
        catbuilder = CatalogEntryBuilder(
            catalog=cfg["catalog"], model=cfg["model"], exp=cfg["exp"],
            resolution=cfg["resolution"], frequency=cfg["frequency"],
            stat=cfg["stat"], loglevel=cfg["loglevel"],
            realization="ensemble",
        )
        logger.info("  Catalog entry name : '%s'", catbuilder.create_entry_name())
        logger.info("  urlpath glob       : %s",
                    catbuilder.opt.build_path(basedir=cfg["outdir"], var="*", year="*"))
        return

    # --- Step 2: write files ---
    write_variable(drop, cfg["var"], ens_da, cfg["overwrite"], logger)

    # --- Step 3: register catalog entry ---
    register_catalog_entry(drop, cfg, logger)

    logger.info("Done.")


if __name__ == "__main__":
    main()

