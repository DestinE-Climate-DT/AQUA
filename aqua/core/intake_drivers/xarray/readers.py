"""AQUA specialization of the intake 2 xarray reader."""

from intake.readers.readers import XArrayDatasetReader

# kwargs accepted by xr.open_mfdataset but not by xr.open_dataset, which is what
# XArrayDatasetReader calls when the url resolves to a single file. They all control
# how *multiple* datasets are combined, so they are meaningless (and safely dropped)
# for a single one — there is nothing to combine. Only these actually appear in the
# AQUA catalogs: combine (75 entries) and compat (6); the rest are listed for safety.
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
    ``xr.open_dataset``, which rejects those kwargs and raises. This subclass only
    strips those kwargs and delegates the actual open to ``super()._read`` — the
    ``xr.open_dataset`` / ``xr.open_mfdataset`` dispatch stays entirely in intake.
    """

    def _read(self, data, **kw):
        url = data.url
        single = (isinstance(url, str) and "*" not in url) or (isinstance(url, (list, tuple, set)) and len(url) == 1)
        if single:
            kw = {k: v for k, v in kw.items() if k not in MFDATASET_ONLY_KWARGS}
        return super()._read(data, **kw)
