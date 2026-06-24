"""
Icechunk Writer for DROP

Writes climate data to a single-store Zarr repository via icechunk's git-like versioning.
Each variable is written month by month; every month is committed atomically (failure
boundary).

Differences from NetCDF/Zarr writers:
- One zarr repo for the entire DROP run; all history is tracked as snapshots
- No monthly-to-yearly concatenation step; repo snapshots replace it
- No tmpfiles; writes go directly to icechunk sessions
- Metadata auto-optimized by icechunk (no consolidate_metadata() call needed)

Example::

    from aqua.core.drop.drop_writer_icechunk import IcechunkWriter
    from aqua.core.util import estimate_time_chunk_size

    writer = IcechunkWriter(tmpdir="/tmp/drop", outdir="/data/output", loglevel="INFO")
    writer.time_chunk_size = estimate_time_chunk_size("daily")
    writer.write_variable(data, var="tas", definitive=True)
"""

import os
import time

import icechunk
import icechunk.xarray
import numpy as np
import pandas as pd
import xarray as xr

from .drop_writer_base import BaseWriter


class IcechunkWriter(BaseWriter):
    """
    Writer for icechunk-backed single-store Zarr archives.

    Implements atomic monthly commits with optional garbage collection.
    Multi-variable runs are processed sequentially; all variables share the same repo.

    Differences from NetCDF/Zarr writers:
    - No tmpfiles; writes directly to icechunk sessions
    - No monthly→yearly concatenation; all versions tracked by repo
    - Metadata auto-optimized (no consolidation call needed)
    - Garbage collection optional (removes intermediate snapshots)
    """

    def __init__(
        self,
        tmpdir,
        outdir,
        filename_builder=None,
        repo_path=None,
        garbage_collect_yearly=False,
        loglevel="WARNING",
    ):
        """
        Initialize Icechunk writer.

        Args:
            tmpdir: Temporary directory (unused; kept for interface compatibility)
            outdir: Output directory where single zarr repo will be created
            filename_builder: OutputPathBuilder instance (unused; kept for interface)
            repo_path: Custom repo path (default: outdir/archive.zarr)
            garbage_collect_yearly: Enable yearly garbage collection of snapshots
            loglevel: Logging level
        """
        super().__init__(tmpdir, outdir, filename_builder, loglevel)

        # Icechunk repo configuration
        self.repo_path = repo_path or os.path.join(outdir, "archive.zarr")
        self.repo = None
        self.main_session = None

        # Time chunk configuration (set by Drop.retrieve() based on frequency)
        self.time_chunk_size = None

        # Garbage collection strategy
        self.garbage_collect_yearly = garbage_collect_yearly

        self.logger.info("IcechunkWriter initialized: repo_path=%s", self.repo_path)

    def _init_repo(self):
        """
        Open or create icechunk repository and get writable session.

        Stores repo and main_session for use by write operations.
        """
        try:
            storage = icechunk.local_filesystem_storage(self.repo_path)
            self.repo = icechunk.Repository.open_or_create(storage)
            self.main_session = self.repo.writable_session("main")
            self.logger.info("Opened icechunk repo: %s", self.repo_path)
        except Exception as e:
            self.logger.error("Failed to open/create icechunk repo: %s", e)
            raise

    def get_extension(self):
        """Return file extension for zarr format."""
        return ".zarr"

    def validate(self, store_path):
        """Validate icechunk store: must exist and contain at least one time step."""
        if not os.path.exists(store_path):
            return False
        try:
            ds = self._open_files(store_path)
            return "time" in ds.dims and len(ds.time) > 0
        except Exception:
            return False

    def _get_encoding(self, data, var=None):
        """
        Get zarr v3 encoding configuration with precomputed time chunks.

        ``var`` is present for ABC interface compliance with :class:`BaseWriter`;
        icechunk encodes every variable in ``data.data_vars`` in one pass so
        per-variable dispatch is not needed here.

        Args:
            data: xarray Dataset
            var: Unused; kept for interface compatibility with BaseWriter.

        Returns:
            dict: Encoding configuration or None
        """
        if not self.time_chunk_size:
            self.logger.warning("time_chunk_size not set; icechunk will use default chunk layout")
            return None
        return self._build_zarr_encoding(data, time_chunk=self.time_chunk_size)

    def _write_chunk_to_disk(self, data, tmpfile, encoding):
        """Not used: IcechunkWriter writes via _write_to_icechunk_session(), not tmpfiles."""
        raise NotImplementedError("IcechunkWriter does not write via tmpfiles; use _write_to_icechunk_session()")

    def _write_to_icechunk_session(self, data, session, mode="w", append_dim=None):
        """
        Write data directly to icechunk session.

        Args:
            data: xarray Dataset/DataArray (already computed)
            session: icechunk Session object
            mode: 'w' for initial write, 'a' for append
            append_dim: Dimension to append along (e.g., 'time')

        Returns:
            bool: True if successful
        """
        try:
            # Convert DataArray → Dataset if needed (preserve attrs)
            data = self._to_dataset(data, None)

            # Encoding is only valid on the initial write (mode='w').
            # Passing it on append ('a') raises "variable already exists, but encoding was provided".
            encoding = self._get_encoding(data) if mode == "w" else None

            # Write to session
            icechunk.xarray.to_icechunk(
                data,
                session,
                mode=mode,
                append_dim=append_dim,
                encoding=encoding,
            )

            self.logger.debug("Written to icechunk session (mode=%s)", mode)
            return True

        except Exception as e:
            self.logger.error("Failed to write to icechunk session: %s", e)
            return False

    def _should_concat(self):
        """Not used: IcechunkWriter fully overrides write_variable()."""
        raise NotImplementedError("IcechunkWriter does not use concat; write_variable() is fully overridden")

    def concat_year_files(self, var, year, level=None):
        """
        Icechunk override: no concatenation needed.

        Optionally runs garbage collection after yearly writes.

        Args:
            var: Variable name
            year: Year
            level: Level (optional, for filename generation)

        Returns:
            bool: Always True
        """
        # Yearly garbage collection (optional)
        if self.garbage_collect_yearly:
            self._yearly_garbage_collect()

        return True

    def _yearly_garbage_collect(self):
        """
        Run garbage collection to prune intermediate snapshots.

        Keeps tagged snapshots (e.g., yearly checkpoints) but removes
        intermediate monthly snapshots to reduce repo size.
        """
        try:
            if not self.repo:
                self.logger.warning("Repo not initialized; skipping GC")
                return

            start_gc = time.time()
            self.repo.garbage_collect()
            gc_time = time.time() - start_gc

            self.logger.info("Garbage collection completed in %.2f seconds", gc_time)

        except Exception as e:
            self.logger.warning("Garbage collection failed (non-fatal): %s", e)

    def _open_files(self, filepaths):
        """Open a committed icechunk store as a read-only xarray Dataset.

        Args:
            filepaths: Path or list of paths to the icechunk zarr repository;
                only a single store path is meaningful (the first element of a
                list is used).

        Returns:
            xarray.Dataset: Dataset opened from the latest committed snapshot.

        Raises:
            Exception: Any error from icechunk or xarray (caller decides how to handle).
        """
        store_path = filepaths[0] if isinstance(filepaths, list) else filepaths
        storage = icechunk.local_filesystem_storage(store_path)
        repo = icechunk.Repository.open(storage)
        read_session = repo.readonly_session("main")
        return xr.open_zarr(read_session.store, consolidated=False)

    def check_integrity(self, var, overwrite=False, end_date=None):
        """
        Check variable integrity by querying repo metadata.

        Reads from current session store; very fast (metadata-only).

        Because icechunk holds a single store for all variables, completeness
        must be evaluated against the requested time range: having *some* data
        is not enough if it does not cover up to ``end_date``.

        Args:
            var: Variable name
            overwrite: If True, always report incomplete
            end_date: Optional upper bound of the requested time range
                (numpy datetime64, pandas Timestamp, or any value accepted
                by ``pd.Timestamp``).  When provided, the repo is considered
                complete only if its last committed timestamp is >= end_date.
                More data than requested is fine; less is not.

        Returns:
            dict: {
                'complete': bool,
                'last_record': str (YYYYMMDD format) or None,
                'message': str
            }
        """
        if overwrite:
            return {"complete": False, "last_record": None, "message": "Overwrite mode enabled"}

        # Repo must exist on disk to have any committed data
        if not os.path.exists(self.repo_path):
            return {"complete": False, "last_record": None, "message": "Repo not found on disk"}

        try:
            # Open a fresh read-only session to inspect committed state only.
            # Using self.main_session would expose uncommitted data during an active write.
            ds = self._open_files(self.repo_path)

            if var not in ds:
                return {"complete": False, "last_record": None, "message": f"Variable {var} not found"}

            da = ds[var]
            if len(da.time) == 0:
                return {"complete": False, "last_record": None, "message": "No time data"}

            times = da.time.values

            # Check for duplicates and sorted
            if len(times) != len(set(times)):
                return {"complete": False, "last_record": None, "message": "Duplicate timestamps"}

            if len(times) > 1 and not (np.diff(times) > np.timedelta64(0, "ns")).all():
                return {"complete": False, "last_record": None, "message": "Unsorted timestamps"}

            last_record = pd.to_datetime(times[-1]).strftime("%Y%m%d")

            # When an expected end date is provided, verify coverage.
            # Keep last_record so write_variable can resume from the right point.
            if end_date is not None:
                last_store_ts = pd.Timestamp(times[-1])
                end_ts = pd.Timestamp(end_date)
                if last_store_ts < end_ts:
                    return {
                        "complete": False,
                        "last_record": last_record,
                        "message": (f"Variable {var} covers up to {last_record} but {end_ts.strftime('%Y%m%d')} is requested"),
                    }

            return {
                "complete": True,
                "last_record": last_record,
                "message": f"Variable {var} complete ({len(times)} timesteps)",
            }

        except Exception as e:
            return {"complete": False, "last_record": None, "message": f"Error reading repo: {e}"}

    def write_variable(
        self,
        data,
        var,
        level=None,
        overwrite=False,
        definitive=True,
        dask=False,
        performance_reporting=False,
        stats_file=None,
    ):
        """
        Write complete variable with monthly commits to icechunk repo.

        Orchestrates:
        1. Skip if variable is already complete and ``overwrite=False``
        2. Resume from last committed record when partial data exists and ``overwrite=False``
        3. Split data by year, then by month
        4. Write each month directly to session (mode='w' for first write, 'a' for appends)
        5. Commit per month (atomic failure boundary)
        6. Run yearly GC if enabled

        Args:
            data: xarray DataArray with processed data (history already applied)
            var: Variable name
            level: Optional level information
            overwrite: If True, clobber existing data and restart from scratch.
                       If False, skip completed variables and resume from the last committed record.
            definitive: Actually write files (vs dry-run)
            dask: If True, use Dask for distributed computing
            performance_reporting: Limit to first month only (for benchmarking)

        Returns:
            bool: True if successful
        """
        # Initialize repo on first write
        if not self.repo:
            self._init_repo()

        # Check existing state to decide whether to skip or resume.
        # Pass end_date so check_integrity can verify the repo covers the full requested range.
        integrity = self.check_integrity(var, overwrite=overwrite, end_date=data.time.values[-1])
        if integrity["complete"] and not overwrite:
            self.logger.info("Variable %s already complete in repo; skipping", var)
            return True

        # When not overwriting and partial data exists, resume from last committed record
        last_record = integrity["last_record"] if not overwrite else None
        if last_record:
            resume_after = pd.Timestamp(last_record)
            data = data.sel(time=data.time > resume_after)
            self.logger.info("Resuming %s after %s (%d timesteps remaining)", var, last_record, len(data.time))
            if len(data.time) == 0:
                self.logger.info("No new data for %s after last committed record", var)
                return True

        # mode='w' clobbers; used only for the first write of a fresh/overwrite run.
        # When resuming, all writes must append to the existing store.
        first_session_write = last_record is None

        for year, year_data in self._iter_years(data, performance_reporting):
            self.logger.info("Processing year %s...", str(year))

            for month, month_data in self._iter_months_in_year(year_data, performance_reporting):
                self.logger.info("Processing month %s-%02d...", year, month)

                if definitive:
                    t_start = time.time()

                    # Compute data
                    month_data = self._compute_data(month_data, dask=dask, performance_reporting=performance_reporting)
                    self._last_chunk_size_bytes = month_data.nbytes

                    mode = "w" if first_session_write else "a"
                    append_dim = None if first_session_write else "time"

                    # Write to session
                    try:
                        if not self._write_to_icechunk_session(
                            month_data, self.main_session, mode=mode, append_dim=append_dim
                        ):
                            self.logger.error("Failed to write month %s-%02d; skipping", year, month)
                            self.main_session = self.repo.writable_session("main")
                            continue

                        # Commit per month (atomic unit)
                        try:
                            self.main_session.commit(f"DROP {var} {year}-{month:02d}")
                            self.logger.info("Month %s-%02d committed", year, month)
                            # Create new session for next write (current becomes read-only after commit)
                            self.main_session = self.repo.writable_session("main")
                            first_session_write = False

                        except Exception as e:
                            self.logger.error("Commit failed for month %s-%02d: %s", year, month, e)
                            self.main_session = self.repo.writable_session("main")
                            continue

                        # Post-commit integrity check: verify committed snapshot is readable
                        # and the last timestamp matches what was just written.
                        expected_last = pd.Timestamp(month_data.time.values[-1]).strftime("%Y%m%d")
                        integrity = self.check_integrity(var)
                        if not integrity["complete"] or integrity["last_record"] != expected_last:
                            self.logger.error(
                                "Post-commit integrity check failed for %s-%02d: %s",
                                year,
                                month,
                                integrity["message"],
                            )
                            continue
                        self.logger.debug(
                            "Post-commit check passed for %s-%02d (last_record=%s)",
                            year,
                            month,
                            integrity["last_record"],
                        )

                    except Exception as e:
                        self.logger.error("Write failed for month %s-%02d: %s", year, month, e)
                        self.main_session = self.repo.writable_session("main")
                        continue

                    t_elapsed = time.time() - t_start
                    self.logger.info("Month %s-%02d execution time: %.2f seconds", year, month, t_elapsed)
                    self._record_chunk_stats(var, year, month, t_elapsed, self._last_chunk_size_bytes, stats_file)

            # Yearly checkpoint (optional)
            if definitive:
                self.concat_year_files(var, year)  # Calls optional GC

        return True

    def get_filename(self, var, year=None, month=None, level=None, tmp=False):
        """
        Get filename/store name for variable.

        Args:
            var: Variable name
            year: Year (unused for icechunk; single repo)
            month: Month (unused for icechunk; single repo)
            level: Optional level information (unused for icechunk; single repo)
            tmp: Unused (no tmpfiles in icechunk)

        Returns:
            str: Path to single zarr repo
        """
        return self.repo_path
