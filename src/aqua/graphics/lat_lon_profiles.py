import xarray as xr
import matplotlib.pyplot as plt

from aqua.logger import log_configure
from aqua.util import to_list
from .util_timeseries import plot_timeseries_data
from .styles import ConfigStyle

def plot_lat_lon_profiles(mean_type: str, 
                          monthly_data: xr.DataArray = None,
                          annual_data: xr.DataArray = None,
                          data_labels: list = None,
                          style: str = None,
                          fig: plt.Figure = None, 
                          ax: plt.Axes = None,
                          loglevel='WARNING',
                          **kwargs):
    """
    Plot latitude or longitude profiles of data, averaging over the specified axis.

    Args:
        mean_type (str): Type of mean to calculate ('zonal' for latitude, 'meridional' for longitude).
        monthly_data (xr.DataArray or list, optional): Monthly data to plot.
        annual_data (xr.DataArray or list, optional): Annual data to plot.
        data_labels (list, optional): Labels for the data.
        style (str, optional): Style for the plot.
        fig (plt.Figure, optional): Matplotlib figure object.
        ax (plt.Axes, optional): Matplotlib axes object.
        loglevel (str, optional): Logging level.
        **kwargs: Additional keyword arguments for customization.

    Returns:
        tuple: Matplotlib figure and axes objects.
    """

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

    # Convert inputs to lists
    monthly_list = to_list(monthly_data)
    annual_list = to_list(annual_data)
    labels_list = to_list(data_labels)

    if monthly_list:
        data_to_plot = [data_coordinate_means(d, mean_type).load() for d in monthly_list]
        kind = 'monthly'
    elif annual_list:
        data_to_plot = [data_coordinate_means(d, mean_type).load() for d in annual_list]
        kind = 'annual'
    else:
        raise ValueError("No data provided for plotting")

    if fig is None and ax is None:
        fig_size = kwargs.get('figsize', (10, 5))
        fig, ax = plt.subplots(1, 1, figsize=fig_size)

    logger.debug(f"Plotting {len(data_to_plot)} data arrays")


    # prepare labels if not provided
    if not labels_list or len(labels_list) < len(data_to_plot):
        labels_list = [
            (d.attrs.get("long_name", f"Data {i+1}")) for i, d in enumerate(data_to_plot)
        ]

    plot_timeseries_data(
        ax=ax,
        data=data_to_plot,
        data_labels=labels_list,
        lw=3,
        kind=kind
    )

    ax.legend(fontsize='small')
    ax.grid(True, axis="y", linestyle='-', color='silver', alpha=0.8)

    title = kwargs.get('title', None)
    if title:
        ax.set_title(title, fontsize=13, fontweight='bold')

    return fig, ax