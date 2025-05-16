import xarray as xr
import matplotlib.pyplot as plt

from aqua.logger import log_configure
from .util_timeseries import plot_monthly_data, plot_annual_data
from .styles import ConfigStyle

def plot_lat_lon_profiles(mean_type: str, 
                          monthly_data: xr.DataArray = None,
                          annual_data: xr.DataArray = None,
                          data_list: list = None,
                          data_labels: list = None,
                          style: str = None,
                          fig: plt.Figure = None, 
                          ax: plt.Axes = None,
                          loglevel='WARNING',
                          **kwargs):


    logger = log_configure(loglevel, 'plot_lat_lon_profiles')
    ConfigStyle(style=style, loglevel=loglevel)

    def data_coordinate_means(data, mean_type : str):
        """
        Calculate the mean of the data along the latitude or longitude axis.
        """
        logger.debug(f"{mean_type} mean calculation")
        if mean_type == 'zonal':
            return data.mean(dim='lon')
        elif mean_type == 'meridional':
            return data.mean(dim='lat')
        else:
            raise ValueError("mean_type must be 'zonal' or 'meridional'")
        

    if data_list is not None:
        if fig is None and ax is None:
            fig_size = kwargs.get('figsize', (10, 5))
            fig, ax = plt.subplots(1, 1, figsize=fig_size)
        for i, data in enumerate(data_list):
            averaged_data = data_coordinate_means(data, mean_type)
            if data_labels is not None and i < len(data_labels):
                label = data_labels[i]
            else:
                label = data.attrs.get("long_name", f"Data {i+1}")
            ax.plot(averaged_data[averaged_data.dims[0]], averaged_data, label=label, lw=3)
        ax.legend(fontsize='small')
    else:
        if monthly_data is not None:
            data = monthly_data
        elif annual_data is not None:
            data = annual_data
        else:
            raise ValueError("No data provided for plotting")
        

    if fig is None and ax is None:
        fig_size = kwargs.get('figsize', (10, 5))
        fig, ax = plt.subplots(1, 1, figsize=fig_size)

    averaged_data = data_coordinate_means(data, mean_type)

    if monthly_data is not None:
        plot_monthly_data(ax, averaged_data, data_labels, logger, lw=3)

    if annual_data is not None:
        plot_annual_data(ax, averaged_data, data_labels, logger, lw=3)

    ax.grid(True, axis="y", linestyle='-', color='silver', alpha=0.8)

    title = kwargs.get('title', None)
    if title:
        ax.set_title(title, fontsize=13, fontweight='bold')

    return fig, ax