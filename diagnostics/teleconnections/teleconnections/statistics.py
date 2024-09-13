"""Module for computing regression maps."""
import xarray as xr

from teleconnections.tools import _check_dim, select_season

xr.set_options(keep_attrs=True)


def reg_evaluation(indx,
                   data,
                   dim: str = 'time',
                   season=None):
    """
    Evaluate regression map of a teleconnection index
    and a DataArray field

    Args:
        indx: teleconnection index
        data: data field
        dim (str,opt):           dimension along which to compute
                                 regression. Default is 'time'
        season (str,opt):        season to be selected. Default is None

    Returns:
        (xarray.DataArray):  Regression map
    """
    _check_dim(indx, dim)
    _check_dim(data, dim)

    if season:
        indx = select_season(indx, season)
        data = select_season(data, season)

    reg = xr.cov(indx, data, dim=dim)/indx.var(dim=dim,
                                               skipna=True).values

    return reg


def cor_evaluation(indx,
                   data,
                   dim: str = 'time',
                   season=None):
    """
    Evaluate correlation map of a teleconnection index
    and a DataArray field

    Args:
        indx:  teleconnection index
        data:  data field
        dim (str,opt):            dimension along which to compute
                                  correlation. Default is 'time'
        season (str,opt):         season to be selected. Default is None

    Returns:
        (xarray.DataArray):  Correlation map
    """
    _check_dim(indx, dim)
    _check_dim(data, dim)

    if season:
        indx = select_season(indx, season)
        data = select_season(data, season)

    cor = xr.corr(indx, data, dim=dim)

    return cor
