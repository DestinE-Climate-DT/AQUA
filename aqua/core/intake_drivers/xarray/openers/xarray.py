"""Functional entry points for NetCDF and Zarr access through xarray.

``open_netcdf`` and ``open_zarr`` are the stable, intake2-friendly way to open
file-based data as xarray datasets. They are used both by the intake ``netcdf``
and ``zarr`` drivers (:mod:`aqua.core.intake_drivers.xarray.readers`) and
directly (e.g. from tests or notebooks without a catalog), mirroring the layout
of the ``fdb`` and ``icechunk`` drivers. Local glob patterns are expanded here,
and the single-file / multi-file xarray entry point is selected accordingly.
"""

from glob import glob
from urllib.parse import urlparse

import fsspec
import xarray as xr

from aqua.core.util import to_list

DEFAULT_NETCDF_ENGINE = "netcdf4"

# kwargs accepted by xr.open_mfdataset only: dropped when a single file
# falls back to xr.open_dataset.
MFDATASET_ONLY_KWARGS = (
    "combine",
    "concat_dim",
    "preprocess",
    "parallel",
    "join",
    "compat",
    "coords",
    "data_vars",
    "combine_attrs",
)


def _is_local(url) -> bool:
    """Check whether a url points to the local filesystem."""
    return urlparse(str(url)).scheme in ("file", "")


def expand_urls(url) -> list:
    """
    Normalize a url (or list of urls) into an explicit list of paths.

    Glob patterns in local paths are expanded (sorted); remote urls are kept as-is.

    Args:
        url (str | list): Path(s) or url(s) to the data, possibly containing globs.

    Returns:
        list: The explicit list of paths/urls.
    """
    expanded = []
    for u in to_list(url):
        if _is_local(u) and any(char in str(u) for char in "*?["):
            expanded.extend(sorted(glob(str(u))))
        else:
            expanded.append(u)
    return expanded


def open_netcdf(url, storage_options=None, metadata=None, engine=DEFAULT_NETCDF_ENGINE, **kwargs) -> xr.Dataset:
    """
    Open one or more NetCDF files as an xarray Dataset.

    A single file is opened with ``xr.open_dataset`` (eager-capable: no dask
    unless ``chunks`` is passed), multiple files with ``xr.open_mfdataset``.
    Remote urls are opened through fsspec file-like objects.

    Args:
        url (str | list): Path(s) to the source file(s). May include globs.
        storage_options (dict, optional): Parameters for the fsspec file-system (e.g. s3).
        metadata (dict, optional): Catalog metadata for this source (unused, accepted
                                   for compatibility with the datatype dict).
        engine (str, optional): xarray backend engine. Defaults to "netcdf4",
                                since AQUA netcdf data may be in any netCDF flavour.
        kwargs: Further parameters forwarded to the xarray open call
                (e.g. chunks, decode_times, combine).

    Returns:
        xr.Dataset: The opened dataset.

    Raises:
        FileNotFoundError: When no file matches the given url(s).
    """
    urls = expand_urls(url)
    if not urls:
        raise FileNotFoundError(f"No NetCDF files match {url}")

    if len(urls) == 1:
        # single file: open_dataset supports eager reading (open_mfdataset would
        # always return a dask-backed dataset); drop the mfdataset-only kwargs.
        kwargs = {key: value for key, value in kwargs.items() if key not in MFDATASET_ONLY_KWARGS}
        target = urls[0]
        if not _is_local(target):
            target = fsspec.open(target, **(storage_options or {})).open()
        return xr.open_dataset(target, engine=engine, **kwargs)

    if not _is_local(urls[0]):
        urls = [of.open() for of in fsspec.open_files(urls, **(storage_options or {}))]

    return xr.open_mfdataset(urls, engine=engine, **kwargs)


def open_zarr(url, storage_options=None, metadata=None, root="", **kwargs) -> xr.Dataset:
    """
    Open one or more Zarr stores as an xarray Dataset.

    A single store is opened with ``xr.open_dataset`` (zarr engine), multiple
    stores (e.g. DROP-generated entries with glob urlpaths) with
    ``xr.open_mfdataset``. Remote urls are handled by the zarr engine itself
    through ``storage_options``.

    Args:
        url (str | list): Path(s) or url(s) to the zarr store(s). May include globs.
        storage_options (dict, optional): Parameters for the fsspec file-system.
        metadata (dict, optional): Catalog metadata for this source (unused, accepted
                                   for compatibility with the datatype dict).
        root (str, optional): Group within the store, as set in the intake Zarr datatype.
        kwargs: Further parameters forwarded to the xarray open call
                (e.g. chunks, decode_times, consolidated).

    Returns:
        xr.Dataset: The opened dataset.

    Raises:
        FileNotFoundError: When no store matches the given url(s).
    """
    urls = expand_urls(url)
    if not urls:
        raise FileNotFoundError(f"No Zarr stores match {url}")

    kwargs.setdefault("engine", "zarr")
    if root and "group" not in kwargs:
        kwargs["group"] = root
    if storage_options:
        kwargs.setdefault("backend_kwargs", {})["storage_options"] = storage_options

    if len(urls) == 1:
        kwargs = {key: value for key, value in kwargs.items() if key not in MFDATASET_ONLY_KWARGS}
        return xr.open_dataset(urls[0], **kwargs)

    return xr.open_mfdataset(urls, **kwargs)
