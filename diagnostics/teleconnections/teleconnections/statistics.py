"""Module for computing regression maps."""
import xarray as xr
import sacpy as scp

from aqua.logger import log_configure


def reg_evaluation(indx, data, dim="time", loglevel="WARNING"):
    """Compute regression maps.

    Args:
        indx (xarray.DataArray): Index to regress against.
        data (xarray.DataArray): Data to regress.
        dim (str, optional): Dimension to regress over. Defaults to "time".
        loglevel (str, optional): Logging level. Defaults to "WARNING".

    Returns:
        xarray.DataArray: Regression maps.
    """

    log = log_configure(loglevel, "Regression")

    log.info("Computing regression")

    # Compute regression
    linreg = scp.LinReg(indx, data)

    return linreg.slope

def cor_evaluation(indx, data, dim="time", loglevel="WARNING"):
    """Compute correlation maps.

    Args:
        indx (xarray.DataArray): Index to regress against.
        data (xarray.DataArray): Data to regress.
        dim (str, optional): Dimension to regress over. Defaults to "time".
        loglevel (str, optional): Logging level. Defaults to "WARNING".

    Returns:
        xarray.DataArray: Correlation maps.
    """

    log = log_configure(loglevel, "Correlation")

    log.info("Computing correlation")

    # Compute correlation
    linreg = scp.LinReg(indx, data)

    return linreg.corr
