"""Inspect the AQUA metadata of data retrieved by a Reader and the Reader settings that produced it."""

import xarray as xr


def aqua_attrs(attrs):
    """Return only the ``AQUA_*`` attributes."""
    return {k: v for k, v in attrs.items() if k.startswith("AQUA_")}


def reader_info(reader, data):
    """Log and return the AQUA metadata of the data and the Reader settings that produced it.

    Args:
        reader (Reader): The Reader that retrieved the data.
        data (xr.Dataset or xr.DataArray): The input dataset or dataarray.

    Returns:
        AquaInfo: A dict, printed as an indented block, with three entries.
        ``"reader"``: the Reader settings, i.e. ``fix`` (whether the fixer is on), ``fixer`` and
        ``convention`` names, ``datamodel`` name (False if disabled), ``src_grid`` and ``tgt_grid``
        (None if regridding is not available).
        ``"data"``: the ``AQUA_*`` attributes shared by all variables, with ``AQUA_regridded`` set to
        False if not regridded, plus the ``history`` of the data.
        ``"variables"``: for each variable whose ``AQUA_*`` attributes differ from the shared ones
        (e.g. ``AQUA_formula``), only those that differ.

    Example:
        >>> reader = Reader(model="IFS", exp="test-tco79", source="short")
        >>> data = reader.retrieve()
        >>> reader.info(data)  # or data.aqua.info()
    """
    arrays = [data] if isinstance(data, xr.DataArray) else [data[var] for var in data.data_vars]
    per_var = {da.name: aqua_attrs(da.attrs) for da in arrays}
    for attrs in per_var.values():
        attrs.setdefault("AQUA_regridded", False)
    first, *others = per_var.values() if per_var else [{}]
    common = {k: v for k, v in first.items() if all(k in other and other[k] == v for other in others)}
    if "history" in data.attrs:
        common["history"] = data.attrs["history"]

    info = AquaInfo(
        reader={
            "fix": bool(reader.fix),
            "fixer": reader.fixer_name,
            "convention": reader.convention,
            "datamodel": reader.datamodel.name if reader.datamodel else False,
            "src_grid": reader.src_grid_name,
            "tgt_grid": reader.tgt_grid_name if reader.tgt_fldstat else None,
        },
        data={**aqua_attrs(data.attrs), **common},
        variables={},
    )
    for name, attrs in per_var.items():
        extra = {k: v for k, v in attrs.items() if k not in common or common[k] != v}
        if extra:
            info["variables"][name] = extra
    reader.logger.info("AQUA info:\n%s", info)
    return info


class AquaInfo(dict):
    """Dictionary returned by ``Reader.info()``, printed as an indented block."""

    def __repr__(self):
        lines = []
        for section, content in self.items():
            lines.append(section)
            if not content:
                lines.append("    (none)")
            for key, value in content.items():
                if isinstance(value, dict):  # one entry per variable
                    lines.append(f"    {key}")
                    lines.extend(f"        {k}: {v}" for k, v in value.items())
                elif key == "history":
                    lines.append("    history:")
                    lines.extend(f"        {line.strip()}" for line in str(value).splitlines() if line.strip())
                else:
                    lines.append(f"    {key}: {value}")
        return "\n".join(lines)
