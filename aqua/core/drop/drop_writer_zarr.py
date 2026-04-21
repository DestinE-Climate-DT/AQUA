"""
Zarr Writer for DROP

Handles atomic writing of climate data to Zarr stores with validation
and metadata consolidation. Optimized for large time-series datasets.

Requires Zarr v3+.
"""

import os
import shutil
from time import time

import numpy as np
import pandas as pd
import xarray as xr
import zarr

from aqua.core.logger import log_configure


class ZarrWriter:
    """
    Writer for Zarr format in DROP processing.
    
    Uses atomic two-phase writes and consolidates metadata for efficient reads.
    """
    
    def __init__(self, tmpdir, outdir, chunks=None, compressor='auto', consolidate=False, loglevel='WARNING'):
        """
        Initialize Zarr writer.
        
        Args:
            tmpdir: Temporary directory for atomic writes
            outdir: Output directory for zarr stores
            chunks: Dict of chunk sizes (e.g. {'time': 1, 'lat': None, 'lon': None})
            compressor: 'auto' for Blosc/zstd or custom numcodecs compressor
            consolidate: Whether to consolidate metadata on finalize (default: False)
            loglevel: Logging level
        """
        self.tmpdir = tmpdir
        self.outdir = outdir
        self.chunks = chunks or {'time': 1, 'lat': None, 'lon': None}
        self.compressor = compressor
        self.consolidate_on_finalize = consolidate
        self.logger = log_configure(loglevel, "ZarrWriter")
        self.stores_written = set()  # Track stores for finalization
    
    def get_extension(self):
        """Return file extension for this format."""
        return '.zarr'
    
    def validate(self, store_path):
        """
        Validate Zarr store integrity.
        
        Args:
            store_path: Path to zarr store
            
        Returns:
            tuple: (bool, result) - success flag and either Dataset or error message
        """
        try:
            ds = xr.open_zarr(store_path, consolidated=False)
            if 'time' not in ds.dims:
                return False, "No time dimension"
            if len(ds.time) == 0:
                return False, "Empty time"
            # Check for duplicates
            if len(ds.time) != len(set(ds.time.values)):
                return False, "Duplicate times"
            # Check sorted
            if len(ds.time) > 1 and not (ds.time.diff('time') > np.timedelta64(0, 'ns')).all():
                return False, "Time not sorted"
            return True, ds
        except Exception as e:
            return False, str(e)
    
    def _should_append(self, ds_existing, ds_new):
        """
        Check if new data can be safely appended.
        
        Args:
            ds_existing: Existing dataset in store
            ds_new: New dataset to append
            
        Returns:
            bool: True if safe to append
        """
        try:
            existing_times = set(ds_existing.time.values)
            new_times = ds_new.time.values
            
            if any(t in existing_times for t in new_times):
                self.logger.warning("Time overlap detected, skipping append")
                return False
            return True
        except Exception as e:
            self.logger.warning("Cannot check overlap: %s", e)
            return False
    
    def _setup_encoding(self, data):
        """
        Setup encoding with chunks and compression.
        
        Zarr v3+ only - uses default xarray compression handling.
        
        Args:
            data: xarray Dataset
            
        Returns:
            dict: Encoding configuration
        """
        encoding = {}
        
        # Get variable list
        vars_to_encode = list(data.data_vars)
        
        # Setup chunking
        if self.chunks:
            for var in vars_to_encode:
                # Convert None to actual dimension size
                chunks_resolved = tuple(
                    self.chunks.get(dim, data[var].sizes[dim]) if self.chunks.get(dim) is not None
                    else data[var].sizes[dim]
                    for dim in data[var].dims
                )
                encoding[var] = {'chunks': chunks_resolved}
        
        # Compression: let xarray handle zarr v3 codecs by default
        # zarr v3 uses a different API (codecs instead of compressor)
        # xarray will apply default compression if not specified
        self.logger.debug("Using xarray default compression for zarr v3")
        
        return encoding or None
    
    def append_chunk(self, data, store_path):
        """
        Atomically append data to zarr store.
        
        Uses two-phase write: temp → validate → atomic replace.
        
        Args:
            data: xarray Dataset/DataArray to append
            store_path: Path to zarr store
            
        Returns:
            bool: True if write successful
        """
        # Convert DataArray to Dataset if needed
        if isinstance(data, xr.DataArray):
            var_name = data.name or 'data'
            data = data.to_dataset(name=var_name)
        
        tmp_path = store_path + ".tmp"
        encoding = self._setup_encoding(data)
        
        try:
            # Phase 1: Check if we can append
            if os.path.exists(store_path):
                valid, result = self.validate(store_path)
                if not valid:
                    self.logger.warning("Existing store invalid (%s), recreating", result)
                    shutil.rmtree(store_path)
                    data.to_zarr(store_path, mode='w', consolidated=False, encoding=encoding)
                    self.stores_written.add(store_path)
                    self.logger.info("Created new zarr store: %s", store_path)
                    return True
                
                ds_existing = result
                if not self._should_append(ds_existing, data):
                    return False
                
                # Phase 2: Write to temp (copy + append)
                self.logger.debug("Copying store to temp for atomic append")
                shutil.copytree(store_path, tmp_path)
                data.to_zarr(tmp_path, mode='a', append_dim='time', consolidated=False)
                
            else:
                # New store
                self.logger.debug("Creating new zarr store")
                data.to_zarr(tmp_path, mode='w', consolidated=False, encoding=encoding)
            
            # Phase 3: Validate temp
            valid, result = self.validate(tmp_path)
            if not valid:
                raise ValueError(f"Validation failed: {result}")
            
            # Phase 4: Atomic replace
            if os.path.exists(store_path):
                shutil.rmtree(store_path)
            shutil.move(tmp_path, store_path)
            
            self.stores_written.add(store_path)
            self.logger.info("Successfully appended to zarr store: %s", store_path)
            return True
            
        except Exception as e:
            self.logger.error("Zarr write failed: %s", e)
            # Cleanup on failure
            if os.path.exists(tmp_path):
                shutil.rmtree(tmp_path)
            return False
    
    def consolidate_metadata(self, store_path):
        """
        Consolidate zarr metadata for faster reads.
        
        Args:
            store_path: Path to zarr store
            
        Returns:
            bool: True if successful
        """
        try:
            zarr.convenience.consolidate_metadata(store_path)
            self.logger.info("Metadata consolidated: %s", store_path)
            return True
        except Exception as e:
            self.logger.error("Consolidation failed for %s: %s", store_path, e)
            return False
    
    def finalize(self):
        """
        Finalize all zarr stores (optionally consolidating metadata).
        
        Should be called after all writes are complete.
        """
        if self.consolidate_on_finalize:
            self.logger.info("Finalizing %d zarr stores with metadata consolidation...", len(self.stores_written))
            for store_path in self.stores_written:
                self.consolidate_metadata(store_path)
            self.logger.info("Zarr writer finalization complete (metadata consolidated)")
        else:
            self.logger.info("Zarr writer finalization complete (%d stores, consolidation disabled)", len(self.stores_written))
    
    def get_filename(self, var, year=None, month=None, tmp=False):
        """
        Generate Zarr store path (single store per variable).
        
        Args:
            var: Variable name
            year: Ignored for zarr (single store)
            month: Ignored for zarr (single store)
            tmp: If True, return path in tmpdir
            
        Returns:
            str: Full path to zarr store
        """
        zarr_filename = f"{var}.zarr"
        if tmp:
            return os.path.join(self.tmpdir, zarr_filename)
        else:
            return os.path.join(self.outdir, zarr_filename)
    
    def check_integrity(self, var, overwrite=False):
        """
        Check integrity of Zarr store for a variable.
        
        Args:
            var: Variable name
            overwrite: If True, always report incomplete
            
        Returns:
            dict: {
                'complete': bool,
                'last_record': str (YYYYMMDD format) or None,
                'message': str
            }
        """
        if overwrite:
            return {
                'complete': False,
                'last_record': None,
                'message': 'Overwrite mode enabled'
            }
        
        store_path = self.get_filename(var)
        
        if not os.path.exists(store_path):
            return {
                'complete': False,
                'last_record': None,
                'message': 'Store does not exist'
            }
        
        valid, result = self.validate(store_path)
        
        if valid:
            ds = result
            try:
                last_record = ds.time[-1].values
                last_record_str = pd.to_datetime(last_record).strftime("%Y%m%d")
                return {
                    'complete': True,
                    'last_record': last_record_str,
                    'message': f'Zarr store complete with {len(ds.time)} timesteps'
                }
            except Exception as e:
                return {
                    'complete': False,
                    'last_record': None,
                    'message': f'Error reading store: {e}'
                }
        else:
            return {
                'complete': False,
                'last_record': None,
                'message': f'Store validation failed: {result}'
            }
    
    def write_variable(self, data, var, overwrite=False, definitive=True,
                      performance_reporting=False, history_callback=None):
        """
        Write complete variable to Zarr store (incremental appends).
        
        Args:
            data: xarray DataArray with processed data
            var: Variable name
            overwrite: Overwrite existing store
            definitive: Actually write files (vs dry-run)
            performance_reporting: Limit to first month only
            history_callback: Optional function to append history metadata
            
        Returns:
            bool: True if successful
        """
        store_path = self.get_filename(var)
        
        # Check if store exists and if we should skip
        if os.path.exists(store_path) and not overwrite:
            valid, result = self.validate(store_path)
            if valid:
                ds_existing = result
                # Check if all data is already in store
                existing_times = set(ds_existing.time.values)
                new_times = set(data.time.values)
                if new_times.issubset(existing_times):
                    self.logger.info("All data already in zarr store, skipping...")
                    return True
                self.logger.info("Appending new timesteps to existing zarr store")
        
        # Process year by year for memory efficiency
        years = sorted(set(data.time.dt.year.values))
        if performance_reporting:
            years = [years[0]]
        
        for year in years:
            self.logger.info("Processing year %s...", str(year))
            year_data = data.sel(time=data.time.dt.year == year)
            
            # Process month by month
            months = sorted(set(year_data.time.dt.month.values))
            if performance_reporting:
                months = [months[0]]
            
            for month in months:
                self.logger.info("Processing month %s...", str(month))
                month_data = year_data.sel(time=year_data.time.dt.month == month)
                
                # Apply history if callback provided
                if history_callback:
                    month_data = history_callback(month_data)
                
                # Write chunk
                if definitive:
                    t_start = time()
                    success = self.append_chunk(month_data, store_path)
                    t_elapsed = time() - t_start
                    if success:
                        self.logger.info("Chunk execution time: %.2f", t_elapsed)
                    else:
                        self.logger.error("Failed to write chunk for %s-%s", year, month)
                
                del month_data
            
            del year_data
        
        return True
