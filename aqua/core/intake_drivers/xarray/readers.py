"""AQUA specialization of the intake 2 xarray reader."""

import xarray as xr
from intake.readers.readers import XArrayDatasetReader


def fold_use_cftime(xarray_kwargs):
    """
    Fold the deprecated ``use_cftime`` xarray kwarg into a CFDatetimeCoder.

    Legacy catalog entries may still carry ``use_cftime`` in their ``xarray_kwargs``:
    since xarray 2025 the kwarg is deprecated as such (and a TypeError when combined
    with a ``decode_times`` coder), so it is translated into the coder here.

    Args:
        xarray_kwargs (dict): The xarray kwargs, modified in place.

    Returns:
        dict: The same dict, with ``use_cftime`` folded into ``decode_times``.
    """
    if "use_cftime" in xarray_kwargs:
        xarray_kwargs["decode_times"] = xr.coders.CFDatetimeCoder(use_cftime=xarray_kwargs.pop("use_cftime"))
    return xarray_kwargs


# kwargs accepted by xr.open_mfdataset only: they must not reach xr.open_dataset,
# which is what XArrayDatasetReader calls when the url resolves to a single file
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


class NetCDFZarrDatasetReader(XArrayDatasetReader):
    """XArrayDatasetReader tolerating multi-file kwargs on single-file reads.

    AQUA catalog entries routinely carry ``xr.open_mfdataset``-only kwargs
    (e.g. ``combine: by_coords``) while their url may resolve to a single file
    (glob expansion, date filtering): the stock reader routes such reads to
    ``xr.open_dataset``, which rejects those kwargs, so they are dropped here.
    """

    def _read(self, data, **kw):
        url = data.url
        single = (isinstance(url, str) and "*" not in url) or (isinstance(url, (list, tuple, set)) and len(url) == 1)
        if single:
            kw = {k: v for k, v in kw.items() if k not in MFDATASET_ONLY_KWARGS}
        return super()._read(data, **kw)
