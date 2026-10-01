"""AQUA-provided intake source for STAC item and collection data."""

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
    """Resolve asset href relative to the STAC item/collection urlpath if href is relative.

    Args:
        base_urlpath (str): The path or URL to the STAC item or collection JSON.
        href (str): The asset or item href, which may be relative, absolute, or a URL.

    Returns:
        str: Resolved path or URL.
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


def _find_item_in_collection(
    collection_dict: dict,
    item_id: str,
    base_urlpath: str,
    storage_options: dict | None = None,
) -> tuple[dict, str]:
    """Find and load a STAC item from a STAC Collection JSON.

    Args:
        collection_dict (dict): The parsed collection JSON dictionary.
        item_id (str): The requested item identifier, title, filename, or relative path.
        base_urlpath (str): The URL or path to the collection JSON.
        storage_options (dict, optional): Storage options for reading the item.

    Returns:
        tuple[dict, str]: (loaded item dictionary, resolved item URL/path).

    Raises:
        KeyError: If the item cannot be found in the collection.
    """
    links = collection_dict.get("links", [])
    item_links = [l for l in links if l.get("rel") == "item"]

    target_href = None
    available_items = []

    for l in item_links:
        href = l.get("href", "")
        title = l.get("title", "")
        lid = l.get("id", "")
        base_name = os.path.basename(href)
        stem = base_name.removesuffix(".json")

        label = lid or stem or title or href
        if label:
            available_items.append(label)

        if item_id in (lid, title, href, base_name, stem):
            target_href = href
            break

    # If not matched directly on rel="item" links, check for rel="items" (STAC API collection)
    if not target_href:
        items_link = next((l for l in links if l.get("rel") == "items"), None)
        if items_link and items_link.get("href"):
            items_base = _resolve_href(base_urlpath, items_link["href"])
            candidate = f"{items_base.rstrip('/')}/{item_id}"
            try:
                with fsspec.open(candidate, "r", **(storage_options or {})) as f:
                    item_dict = json.load(f)
                return item_dict, candidate
            except Exception:
                pass

    # If target_href was found from item links, load it
    if target_href:
        resolved_item_path = _resolve_href(base_urlpath, target_href)
        with fsspec.open(resolved_item_path, "r", **(storage_options or {})) as f:
            item_dict = json.load(f)
        return item_dict, resolved_item_path

    # Fallback: check if item_id can be resolved directly relative to collection path
    for candidate in (item_id, f"{item_id}.json"):
        resolved = _resolve_href(base_urlpath, candidate)
        try:
            with fsspec.open(resolved, "r", **(storage_options or {})) as f:
                item_dict = json.load(f)
            return item_dict, resolved
        except Exception:
            continue

    coll_id = collection_dict.get("id", base_urlpath)
    err_msg = f"Item '{item_id}' not found in STAC collection '{coll_id}'."
    if available_items:
        err_msg += f" Available items: {available_items}"
    raise KeyError(err_msg)


class IntakeSTACSource(IntakeXarraySourceAdapter):
    """Intake driver to read a STAC item or collection JSON and open an asset with xarray.

    Registered as the ``stac`` driver. Reads a STAC item JSON directly, or reads a
    STAC collection JSON and selects the item specified via the ``item`` argument.
    It extracts metadata and properties, resolves the target asset (defaulting to
    ``data`` or the first asset), and loads it into an xarray Dataset via NetCDF or Zarr.

    Example usage in an Intake YAML catalog for a STAC Item::

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

    Example usage in an Intake YAML catalog for a STAC Collection::

        sources:
          ifs_stac_collection:
            description: IFS regridded test data via STAC collection
            driver: stac
            args:
              urlpath: "{{ CATALOG_DIR }}/collections/ifs.json"
              item: "ifs-long-regridded-r18x9"
              asset: "data"
              chunks:
                time: 12

    Example programmatic usage::

        # Direct STAC Item:
        source = IntakeSTACSource("/path/to/item.json", asset="data")
        ds = source.to_dask()

        # Via STAC Collection:
        source = IntakeSTACSource("/path/to/collection.json", item="ifs-long-regridded-r18x9", asset="data")
        ds = source.to_dask()
        print(source.metadata)

    Args:
        urlpath (str, optional): Path or URL to the STAC item or collection JSON file.
        path (str, optional): Alias for ``urlpath``.
        item (str, optional): Identifier, filename, or title of the item to load if
            ``urlpath`` points to a STAC Collection. Required when reading a collection.
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
        item: str | None = None,
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
            raise ValueError("Must provide 'urlpath' (or 'path') pointing to a STAC JSON.")

        # Read the STAC JSON
        if isinstance(target_url, dict):
            raw_dict = target_url
            base_urlpath = ""
        else:
            base_urlpath = str(target_url)
            self.logger.debug("Loading STAC JSON from %s", base_urlpath)
            with fsspec.open(base_urlpath, "r", **(storage_options or {})) as f:
                raw_dict = json.load(f)

        # Determine whether this is a STAC Collection or Item
        is_collection = raw_dict.get("type") == "Collection" or (
            "extent" in raw_dict and "links" in raw_dict and "assets" not in raw_dict
        )

        collection_info = None
        if is_collection:
            if not item:
                coll_id = raw_dict.get("id", base_urlpath)
                raise ValueError(
                    f"The STAC JSON at '{base_urlpath}' is a Collection (id: '{coll_id}'). "
                    "Please provide the 'item' argument to specify which item to load."
                )
            self.logger.info("Resolving item '%s' from STAC collection '%s'", item, raw_dict.get("id"))
            collection_info = {k: v for k, v in raw_dict.items() if k != "links"}
            item_dict, item_base_urlpath = _find_item_in_collection(
                raw_dict, item, base_urlpath, storage_options=storage_options
            )
        else:
            item_dict = raw_dict
            item_base_urlpath = base_urlpath

        # Validate and select asset
        assets = item_dict.get("assets", {})
        if not assets:
            raise ValueError(f"STAC item '{item_dict.get('id', item_base_urlpath)}' contains no assets.")

        if asset is not None:
            if asset not in assets:
                raise KeyError(
                    f"Asset '{asset}' not found in STAC item '{item_dict.get('id', item_base_urlpath)}'. "
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
            raise ValueError(f"Asset '{asset_key}' in STAC item '{item_dict.get('id', item_base_urlpath)}' has no 'href'.")

        asset_href = _resolve_href(item_base_urlpath, raw_href)
        self.logger.info("Selected asset '%s' with href: %s", asset_key, asset_href)

        # Build combined metadata exposing STAC properties and collection details
        combined_metadata = {}

        if collection_info:
            combined_metadata["collection_info"] = collection_info
            if "title" in collection_info:
                combined_metadata["collection_title"] = collection_info["title"]
            if "description" in collection_info:
                combined_metadata["collection_description"] = collection_info["description"]

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
        self.collection_info = collection_info
        self.item_name = item or item_dict.get("id")
        self.asset_name = asset_key
        self.asset_info = asset_info

        super().__init__(data, xarray_kwargs, metadata=combined_metadata)
