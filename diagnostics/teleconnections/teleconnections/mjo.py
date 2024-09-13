"""
Module to evaluate the MJO teleconnection.
"""
import xarray as xr

from aqua.logger import log_configure
from aqua.util import area_selection
from teleconnections.tools import TeleconnectionsConfig

# set default options for xarray
xr.set_options(keep_attrs=True)


def mjo_hovmoller(data=None,
                  var: str = None,
                  loglevel: str = "WARNING",
                  telecname: str = "MJO",
                  namelist: dict = None,
                  day_window: int = None,
                  flip_sign: bool = False,
                  **kwargs) -> xr.DataArray:
    """
    Prepare the data for a MJO Hovmoller plot.

    Args:
        data: (xr.DataArray or xr.Dataset): Data to be used in the Hovmoller plot.
        var (str, opt): Name of the variable to be used in the Hovmoller plot.
                        It is only required if data is a dataset.
                        If not provided, it will be extracted from the namelist
        loglevel (str):   Log level to be used. Default is "WARNING".
        telecname (str):  Name of the teleconnection to be evaluated.
                          Default is "MJO".
        namelist (dict):  Namelist with the teleconnection informations.
        day_window (int): Number of days to be used in the smoothing window.
                          Default is not performed.
        flip_sign (bool): If True, it will flip the sign of the variable.
                          Will be extracted from the namelist if not provided. Default is False.

    KwArgs:
        configdir (str):  Path to the configuration directory.

    Returns:
        xr.DataArray: DataArray with the data to be used in the Hovmoller plot.
    """
    logger = log_configure(log_level=loglevel, log_name='MJO')

    if data is None:
        raise ValueError("No data provided.")

    if namelist is None:
        logger.info("No namelist provided. Trying to load default namelist.")
        config = TeleconnectionsConfig(**kwargs)
        namelist = config.load_namelist()

    # Subselect field if data is a dataset
    if var is None:
        var = namelist[telecname]['field']
    if isinstance(data, xr.Dataset):
        logger.info("Subselecting var " + var)
        data = data[var]

    flip_sign = namelist[telecname].get('flip_sign', flip_sign)
    if flip_sign:
        logger.info("Flipping the sign of the variable.")
        data = -data

    # Acquiring MJO box
    lat = [namelist[telecname]['latS'], namelist[telecname]['latN']]
    lon = [namelist[telecname]['lonW'], namelist[telecname]['lonE']]

    # Selecting the MJO box
    data_sel = area_selection(data, lat=lat, lon=lon, drop=True)

    # Evaluating anomalies
    data_mean = data_sel.mean(dim='time')
    data_anom = data_sel - data_mean

    # Smoothing the data
    if day_window:
        logger.info("Smoothing the data with a window of " + str(day_window) + " days.")
        data_anom_smooth = data_anom.rolling(time=day_window, center=True).mean()
    else:
        data_anom_smooth = data_anom

    return data_anom_smooth
