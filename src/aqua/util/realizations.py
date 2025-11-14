"""utilities for formatting realizations."""
from typing import Optional, Union
import xarray as xr

DEFAULT_REALIZATION = 'r1'  # Default realization if not specified

def format_realization(realization: Optional[str | int | list | None] = None) -> Union[str, list]:
    """
    Format the realization string by prepending 'r' if it is a digit.

    Args:
        realization (str | int | list | None): The realization value. Can be:
            - str/int: Single realization value
            - list: List of realization values
            - None: Returns default realization

    Returns:
        str | list: Formatted realization string or list of formatted strings.
    """
    if not realization:
        return DEFAULT_REALIZATION
    if isinstance(realization, list):
        for i, r in enumerate(realization):
            if r is None:
                realization[i] = DEFAULT_REALIZATION
            else:
                realization[i] = f'r{r}' if str(r).isdigit() else str(r)
        return realization
    if isinstance(realization, (int, str)):
        return f'r{realization}' if str(realization).isdigit() else str(realization)


def get_realizations(datasets):
    """
    Extract the 'AQUA_realization' attribute from one or more datasets.

    Parameters
    ----------
    datasets : xr.Dataset or list of xr.Dataset
        A single dataset or a list of datasets from which to extract the realization.

    Returns
    -------
    str or list of str
        The realization if the input is a single dataset or a list with one element,
        or a list of realizations if the input list has more than one element.
    """
    # Convert to list if not already
    if not isinstance(datasets, list):
        datasets = [datasets]

    realizations = []
    for d in datasets:
        if hasattr(d, "attrs"):
            val = getattr(d, "AQUA_realization", d.attrs.get("AQUA_realization", DEFAULT_REALIZATION))
        else:
            val = getattr(d, "AQUA_realization", DEFAULT_REALIZATION)
        realizations.append(val)

    # Return single value if list has only one element
    return realizations[0] if len(realizations) == 1 else realizations