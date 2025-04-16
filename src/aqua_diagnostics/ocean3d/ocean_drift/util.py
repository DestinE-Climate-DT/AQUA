import xarray as xr
import numpy as np
from aqua.logger import log_configure
from itertools import product
from aqua.util import find_vert_coord, load_yaml, add_pdf_metadata
import os


def process_region_config(regions_yaml):
    """
    Converts keys at the top level of the dictionary to lowercase and removes spaces, underscores, and dashes.
    """
    if isinstance(regions_yaml, dict):
        return {
            key.replace(" ", "").replace("_", "").replace("-", "").lower(): v
            for key, v in regions_yaml.items()
        }
    else:
        return regions_yaml


def predefined_regions(region, loglevel="WARNING"):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    processed_region = region.replace(" ", "").replace("_", "").replace("-", "").lower()
    regions_yaml = f"{current_dir}/../../../../config/diagnostics/ocean3d/regions.yaml"
    regions_dict = load_yaml(regions_yaml)
    try:
        regions_dict = process_region_config(regions_dict["regions"])
        region_boundary = regions_dict[processed_region]
        lat_n = region_boundary.get("LatN")
        lat_s = region_boundary.get("LatS")
        lon_e = region_boundary.get("LonE")
        lon_w = region_boundary.get("LonW")
    except KeyError:
        raise ValueError(
            f"Invalid region name: {region}. Check the region name in config file or update it: {regions_yaml}"
        )

    return lat_s, lat_n, lon_w, lon_e


def _data_process_by_type(**kwargs):
    """
    Selects the type of timeseries and colormap based on the given parameters.

    Args:
        data (DataArray): Input data containing temperature (thetao) and salinity (so).
        anomaly (bool, optional): Specifies whether to compute anomalies. Defaults to False.
        standardise (bool, optional): Specifies whether to standardize the data. Defaults to False.
        anomaly_ref (str, optional): Reference for the anomaly computation. Valid options: "t0", "tmean". Defaults to None.

    Returns:
        process_data (Dataset): Processed data based on the selected preprocessing approach.
        type (str): Type of preprocessing approach applied
        cmap (str): Colormap to be used for the plot.
    """
    loglevel = kwargs.get("loglevel", "WARNING")
    logger = log_configure(loglevel, "data_process_by_type")
    data = kwargs["data"]
    anomaly = kwargs["anomaly"]
    anomaly_ref = kwargs["anomaly_ref"]
    standardise = kwargs["standardise"]

    process_data = xr.Dataset()

    if anomaly:
        anomaly_ref = anomaly_ref.lower().replace(" ", "").replace("_", "")
        if not standardise:
            if anomaly_ref in ["tmean", "meantime", "timemean"]:
                cmap = "coolwarm"  # "PuOr"
                for var in list(data.data_vars.keys()):
                    process_data[var] = data[var] - data[var].mean(dim="time")
                type = "anomaly wrt temporal mean"
            elif anomaly_ref in ["t0", "intialtime", "firsttime"]:
                cmap = "coolwarm"  # "PuOr"
                for var in list(data.data_vars.keys()):
                    process_data[var] = data[var] - data[var].isel(time=0)
                type = "anomaly wrt initial time"
            else:
                raise ValueError(
                    "Select proper value of anomaly_ref: t0 or tmean, when anomaly = True "
                )
            logger.debug(f"Data processed for {type}")
        if standardise:
            if anomaly_ref in ["t0", "intialtime", "firsttime"]:
                cmap = "coolwarm"  # "PuOr"
                for var in list(data.data_vars.keys()):
                    var_data = data[var] - data[var].isel(time=0)
                    var_data.attrs["units"] = "Stand. Units"
                    # Calculate the standard anomaly by dividing the anomaly by its standard deviation along the time dimension
                    process_data[var] = var_data / var_data.std(dim="time")
                type = "Std. anomaly wrt initial time"
            elif anomaly_ref in ["tmean", "meantime", "timemean"]:
                cmap = "coolwarm"  # "PuOr"
                for var in list(data.data_vars.keys()):
                    var_data = data[var] - data[var].mean(dim="time")
                    var_data.attrs["units"] = "Stand. Units"
                    # Calculate the standard anomaly by dividing the anomaly by its standard deviation along the time dimension
                    process_data[var] = var_data / var_data.std(dim="time")
                type = "Std. anomaly wrt temporal mean"
            else:
                raise ValueError(
                    "Select proper value of type: t0 or tmean, when anomaly = True "
                )
            logger.debug(f"Data processed for {type}")

    else:
        cmap = "jet"
        logger.debug("Data processed for Full values as anomaly = False")
        type = "Full values"

        process_data = data
    # logger.debug(f"Data processed for {type}")
    return process_data, type, cmap


def _data_process_for_drift(data, dim_mean: None, loglevel="WARNING"):

    if dim_mean is not None:
        data = data.mean(dim=dim_mean)
    data_list = []

    for anomaly, standardise, anomaly_ref in product(
        [False, True], [False, True], ["t0", "tmean"]
    ):
        data_proc, type, cmap = _data_process_by_type(
            data=data,
            anomaly=anomaly,
            standardise=standardise,
            anomaly_ref=anomaly_ref,
            loglevel=loglevel,
        )

        data_proc.attrs["cmap"] = cmap
        data_proc = data_proc.expand_dims(dim={"type": [type]})

        data_list.append(data_proc)

    stacked_data = xr.concat(data_list, dim="type")
    return stacked_data
