"""
Global Ocean module
"""

import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import xarray as xr
import numpy as np
import dask.array as da
from scipy.stats import t as statt
from aqua.diagnostics.ocean3d import weighted_area_mean
from aqua.diagnostics.ocean3d import area_selection
from aqua.diagnostics.ocean3d import weighted_zonal_mean
from aqua.diagnostics.ocean3d import file_naming
from aqua.diagnostics.ocean3d import custom_region
from aqua.diagnostics.ocean3d import write_data
from aqua.diagnostics.ocean3d import export_fig
from aqua.logger import log_configure



def data_process_by_type(**kwargs):
    """
    Selects the type of timeseries and colormap based on the given parameters.

    Args:
        data (DataArray): Input data containing temperature (avg_thetao) and salinity (avg_so).
        anomaly (bool, optional): Specifies whether to compute anomalies. Defaults to False.
        standardise (bool, optional): Specifies whether to standardize the data. Defaults to False.
        anomaly_ref (str, optional): Reference for the anomaly computation. Valid options: "t0", "tmean". Defaults to None.

    Returns:
        process_data (Dataset): Processed data based on the selected preprocessing approach.
        type (str): Type of preprocessing approach applied
        cmap (str): Colormap to be used for the plot.
    """
    loglevel= kwargs.get('loglevel',"WARNING")
    logger = log_configure(loglevel, 'data_process_by_type')
    data = kwargs["data"]
    anomaly = kwargs["anomaly"]
    anomaly_ref = kwargs["anomaly_ref"]
    standardise = kwargs["standardise"]
    
    process_data = xr.Dataset()

    
    if anomaly:
        anomaly_ref = anomaly_ref.lower().replace(" ", "").replace("_", "")
        if not standardise:
            if anomaly_ref in ['tmean', "meantime", "timemean"]:
                cmap = "coolwarm" #"PuOr"
                for var in list(data.data_vars.keys()):
                    process_data[var] = data[var] - data[var].mean(dim='time')
                type = "anomaly wrt temporal mean"
            elif anomaly_ref in ['t0', "intialtime", "firsttime"]:
                cmap = "coolwarm" #"PuOr"
                for var in list(data.data_vars.keys()):
                    process_data[var] = data[var] - data[var].isel(time=0)
                type = "anomaly wrt initial time"
            else:
                raise ValueError(
                    "Select proper value of anomaly_ref: t0 or tmean, when anomaly = True ")
            logger.debug(f"Data processed for {type}")
        if standardise:
            if anomaly_ref in ['t0', "intialtime", "firsttime"]:
                cmap = "coolwarm" #"PuOr"
                for var in list(data.data_vars.keys()):
                    var_data = data[var] - data[var].isel(time=0)
                    var_data.attrs['units'] = 'Stand. Units'
                    # Calculate the standard anomaly by dividing the anomaly by its standard deviation along the time dimension
                    process_data[var] = var_data / var_data.std(dim="time")
                type = "Std. anomaly wrt initial time"
            elif anomaly_ref in ['tmean', "meantime", "timemean"]:
                cmap = "coolwarm" #"PuOr"
                for var in list(data.data_vars.keys()):
                    var_data = data[var] - data[var].mean(dim='time')
                    var_data.attrs['units'] = 'Stand. Units'
                    # Calculate the standard anomaly by dividing the anomaly by its standard deviation along the time dimension
                    process_data[var] = var_data / var_data.std(dim="time")
                type = "Std. anomaly wrt temporal mean"
            else:
                raise ValueError(
                    "Select proper value of type: t0 or tmean, when anomaly = True ")
            logger.debug(
                f"Data processed for {type}")

    else:
        cmap = 'jet'
        logger.debug("Data processed for Full values as anomaly = False")
        type = "Full values"

        process_data = data
    # logger.debug(f"Data processed for {type}")
    return process_data, type, cmap
    
    


def reg_mean(data, region=None, lat_s=None, lat_n=None, lon_w=None, lon_e=None, loglevel= "WARNING"):
    """
    Computes the weighted box mean for some predefined or customized region

    Parameters:
        data (xarray.Dataset): Input data.

        region (str, optional): Predefined region name. If provided, latitude and longitude bounds will be fetched from predefined regions.

        lat_s (float, optional): Southern latitude bound. Required if region is not provided or None.

        lat_n (float, optional): Northern latitude bound. Required if region is not provided or None.

        lon_w (float, optional): Western longitude bound. Required if region is not provided or None.

        lon_e (float, optional): Eastern longitude bound. Required if region is not provided or None.

    Returns:
        xarray.Dataset: Standardised anomaly with respect to the initial time step of the input data.
    """
    logger = log_configure(loglevel, 'split_ocean3d_req')
    data = weighted_area_mean(data, region, lat_s, lat_n, lon_w, lon_e)

    return data

    
