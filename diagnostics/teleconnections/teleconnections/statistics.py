"""Module for computing regression maps."""
import xarray as xr

from teleconnections.tools import _check_dim


def reg_evaluation(indx: xr.DataArray,
                   data: xr.DataArray,
                   dim='time'):
    """
    Evaluate regression map of a teleconnection index
    and a DataArray field

    Args:
        indx (xarray.DataArray): teleconnection index
        data (xarray.DataArray): data field

    Returns:
        (xarray.DataArray):  Regression map
    """
    _check_dim(indx, dim)
    _check_dim(data, dim)

    reg = xr.cov(indx, data, dim=dim)/indx.var(dim=dim,
                                               skipna=True).values

    return reg


def cor_evaluation(indx: xr.DataArray,
                   data: xr.DataArray,
                   dim='time'):
    """
    Evaluate correlation map of a teleconnection index
    and a DataArray field

    Args:
        indx (xarray.DataArray):  teleconnection index
        data (xarray.DataArray):  data field
        dim (str,opt):            dimension along which to compute
                                  correlation. Default is 'time'

    Returns:
        (xarray.DataArray):  Correlation map
    """
    _check_dim(indx, dim)
    _check_dim(data, dim)

    cor = xr.corr(indx, data, dim=dim)

    return cor
