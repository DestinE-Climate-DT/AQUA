"""AQUA-provided intake source for STAC item data."""

import json
import os
from urllib.parse import urljoin

import fsspec
import xarray as xr
from intake import readers

from aqua.core.intake_drivers.xarray.base import IntakeXarraySourceAdapter
from aqua.core.intake_drivers.xarray.readers import TolerantXArrayDatasetReader
from aqua.core.logger import log_configure

xr.set_options(keep_attrs=True)


def _is_fsspec_urlpath(urlpath):
    """Return True if urlpath (str or list of str) carries an fsspec protocol (e.g. ``s3://``)."""
    paths = urlpath if isinstance(urlpath, (list, tuple, set)) else [urlpath]
    return any(isinstance(p, str) and "://" in p for p in paths)


def _resolve_href(base_urlpath: str, href: str) -> str:
    """Resolve asset href relative to the STAC item urlpath if href is relative.

    Args:
        base_urlpath (str): The path or URL to the STAC item JSON.
        href (str): The asset href, which may be relative, absolute, or a URL.

    Returns:
        str: Resolved asset path or URL.
    """
    if not base_urlpath or "://" in href or os.path.isabs(href):
        return href
    if "://" in base_urlpath:
        return urljoin(base_urlpath, href)
    base_dir = os.path.dirname(os.path.abspath(base_urlpath))
    return os.path.normpath(os.path.join(base_dir, href))


def _detect_format(media_type: str | None, href: str) -> str:
    """Detect whether asset is NetCDF or Zarr based on media type and href.

    Args:
        media_type (str | None): The MIME type specified in the STAC asset.
        href (str): The resolved asset href.

    Returns:
        str: 'zarr' or 'netcdf'.
    """
    media_type = (media_type or "").lower()
    href_lower = href.lower()

    if "zarr" in media_type or href_lower.endswith(".zarr") or ".zarr/" in href_lower:
        return "zarr"
    if "netcdf" in media_type or "hdf" in media_type or href_lower.endswith((".nc", ".nc4", ".cdf")):
        return "netcdf"
    if "zarr" in href_lower:
        return "zarr"
    return "netcdf"


class IntakeSTACSource(IntakeXarraySourceAdapter):
    """Intake driver to read a STAC item JSON and open an asset with xarray.

    Registered as the ``stac`` driver. Reads a STAC item (via local path or URL),
    extracts the item's metadata and properties, resolves the target asset (defaulting
    to ``data`` or the first asset), and loads it into an xarray Dataset via NetCDF or Zarr.

    Example usage in an Intake YAML catalog::

        sources:
          ifs_stac_regridded:
            description: IFS regridded test data via STAC item
            driver: stac
            args:
              urlpath: "{{ CATALOG_DIR }}/items/ifs/ifs-long-regridded-r18x9.json"
              asset: "data"
              chunks:
                time: 12
              xarray_kwargs:
                engine: netcdf4

    Example programmatic usage::

        source = IntakeSTACSource("/path/to/item.json", asset="data")
        ds = source.to_dask()
        print(source.metadata)

    Args:
        urlpath (str, optional): Path or URL to the STAC item JSON file.
        path (str, optional): Alias for ``urlpath``.
        asset (str, optional): Key of the asset to open (e.g. 'data'). If None,
            defaults to 'data' if present, or the first available asset.
        format (str, optional): Data format ('netcdf' or 'zarr'). If None,
            inferred from the asset's type or href.
        storage_options (dict, optional): Parameters passed to fsspec for reading.
        metadata (dict, optional): Extra catalog metadata to merge with STAC metadata.
        xarray_kwargs (dict, optional): Additional kwargs for xarray open calls.
        loglevel (str, optional): Logging level. Defaults to 'WARNING'.
        **kwargs: Further parameters forwarded to the reader (e.g. chunks).
    """

    name = "stac"

    def __init__(
        self,
        urlpath: str | None = None,
        path: str | None = None,
        asset: str | None = None,
        format: str | None = None,
        storage_options: dict | None = None,
        metadata: dict | None = None,
        xarray_kwargs: dict | None = None,
        loglevel: str = "WARNING",
        **kwargs,
    ):
        self.logger = log_configure(log_level=loglevel, log_name="IntakeSTACSource")
        self.loglevel = loglevel

        target_url = urlpath or path
        if not target_url:
            raise ValueError("Must provide 'urlpath' (or 'path') pointing to a STAC item JSON.")

        # Read the STAC item JSON
        if isinstance(target_url, dict):
            item_dict = target_url
            base_urlpath = ""
        else:
            base_urlpath = str(target_url)
            self.logger.debug("Loading STAC item JSON from %s", base_urlpath)
            with fsspec.open(base_urlpath, "r", **(storage_options or {})) as f:
                item_dict = json.load(f)

        # Validate and select asset
        assets = item_dict.get("assets", {})
        if not assets:
            raise ValueError(f"STAC item '{item_dict.get('id', base_urlpath)}' contains no assets.")

        if asset is not None:
            if asset not in assets:
                raise KeyError(
                    f"Asset '{asset}' not found in STAC item '{item_dict.get('id', base_urlpath)}'. "
                    f"Available assets: {list(assets.keys())}"
                )
            asset_key = asset
        elif "data" in assets:
            asset_key = "data"
        else:
            asset_key = next(iter(assets.keys()))

        asset_info = assets[asset_key]
        raw_href = asset_info.get("href")
        if not raw_href:
            raise ValueError(f"Asset '{asset_key}' in STAC item '{item_dict.get('id', base_urlpath)}' has no 'href'.")

        asset_href = _resolve_href(base_urlpath, raw_href)
        self.logger.info("Selected asset '%s' with href: %s", asset_key, asset_href)

        # Build combined metadata exposing STAC item properties
        combined_metadata = {}
        properties = item_dict.get("properties")
        if isinstance(properties, dict):
            combined_metadata.update(properties)

        for field in ("id", "collection", "geometry", "bbox", "stac_version", "stac_extensions"):
            if field in item_dict:
                combined_metadata[field] = item_dict[field]

        combined_metadata["asset"] = asset_info
        combined_metadata["asset_name"] = asset_key
        combined_metadata["stac_item"] = {k: v for k, v in item_dict.items() if k != "links"}

        # User-supplied catalog metadata takes precedence if specified
        if metadata:
            combined_metadata.update(metadata)

        # Detect format and configure reader
        fmt = format.lower() if format else _detect_format(asset_info.get("type"), asset_href)
        xarray_kwargs = dict(xarray_kwargs or {})

        if fmt == "zarr":
            if "chunks" not in xarray_kwargs:
                kwargs.setdefault("chunks", {})
            asset_storage = storage_options if (storage_options and _is_fsspec_urlpath(asset_href)) else None
            data = readers.datatypes.Zarr(asset_href, storage_options=asset_storage, metadata=combined_metadata)
            reader = TolerantXArrayDatasetReader(data, **xarray_kwargs, metadata=combined_metadata, **kwargs)
        else:  # netcdf
            xarray_kwargs.setdefault("engine", "netcdf4")
            if "chunks" not in xarray_kwargs:
                kwargs.setdefault("chunks", {})
            data = readers.datatypes.NetCDF3(asset_href, storage_options=storage_options, metadata=combined_metadata)
            reader = TolerantXArrayDatasetReader(data, **xarray_kwargs, metadata=combined_metadata, **kwargs)

        self.reader = reader
        self.stac_item = item_dict
        self.asset_name = asset_key
        self.asset_info = asset_info

        super().__init__(data, xarray_kwargs, metadata=combined_metadata)
