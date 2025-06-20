import xarray as xr
import matplotlib.pyplot as plt

from aqua.logger import log_configure
from .styles import ConfigStyle
from .lat_lon_profiles import plot_lat_lon_profiles

def get_seasonal_and_annual_means(da):
    """
    Returns a list of seasonal and annual means from a DataArray.

    Args:
        da (xr.DataArray): Input DataArray with a 'time' dimension.
    Returns:
        list: A list containing seasonal means for DJF, MAM, JJA, SON and an annual mean.
    """

    seasons = {
        "DJF": [11, 0, 1],
        "MAM": [2, 3, 4],
        "JJA": [5, 6, 7],
        "SON": [8, 9, 10]
    }
    season_means = []
    for months in seasons.values():
        season_means.append(da.isel(time=months).mean(dim='time'))
    annual_mean = da.mean(dim='time')
    return season_means + [annual_mean]

def plot_lines(maps,
               plot_type: str = 'seasonal',
               style: str = None,
               loglevel='WARNING',
               data_labels: list = None,
               **kwargs):
    """
    Plots multiple lines for seasonal or annual means from a DataArray or a list of DataArrays.

    Args:
        maps (xr.DataArray or list of xr.DataArray): Input data to plot.
        plot_type (str): Type of plot, either 'seasonal' or 'annual'.
        style (str): Style for the plot.
        loglevel (str): Logging level.
        data_labels (list): Labels for the data series.
        **kwargs: Additional keyword arguments for plotting.
    Returns:
        fig, axs: Matplotlib figure and axes objects.
    """

    logger = log_configure(loglevel, 'plot_lines')
    ConfigStyle(style=style, loglevel=loglevel)

    # If maps is a single DataArray, calculate seasonal and annual means
    if isinstance(maps, xr.DataArray):
        da = maps
        logger.info("getting seasonal and annual means from a single DataArray")
        seasonal_and_annual = get_seasonal_and_annual_means(da)

        maps = [[seasonal_and_annual[i]] for i in range(4)] + [[seasonal_and_annual[4]]]
        if data_labels is None:
            data_labels = [da.attrs.get("long_name", "Data")]
        plot_type = 'seasonal'

    # If maps is a list of DataArrays, calculate seasonal and annual means for each
    elif isinstance(maps, list) and all(isinstance(m, xr.DataArray) for m in maps):
        logger.debug("Getting seasonal and annual means from a list of DataArrays")
        seasonal_and_annual = [get_seasonal_and_annual_means(m) for m in maps]

        # List of lists for each season/annual
        maps = [
            [seasonal_and_annual[j][i] for j in range(len(maps))]  # For each season/annual
            for i in range(5)
        ]
        if data_labels is None:
            data_labels = [
                m.attrs.get("long_name", f"Data {i+1}") for i, m in enumerate(maps[0])
            ]
        plot_type = 'seasonal'


    # "Classical" case: list of lists already structured as [[DJF], [MAM], [JJA], [SON], [Annual]]
    if plot_type == 'seasonal':
        fig = plt.figure(figsize=(14, 10), constrained_layout=True)
        gs = fig.add_gridspec(3, 2)
        axs = [
            fig.add_subplot(gs[0, 0]),  # DJF
            fig.add_subplot(gs[0, 1]),  # MAM
            fig.add_subplot(gs[1, 0]),  # JJA
            fig.add_subplot(gs[1, 1]),  # SON
            fig.add_subplot(gs[2, :])   # Annual (big subplot)
        ]
        season_names = ["DJF", "MAM", "JJA", "SON"]
        for i, ax in enumerate(axs[:4]):
            for j, data in enumerate(maps[i]):
                plot_lat_lon_profiles(mean_type='zonal',
                                      monthly_data=data, 
                                      fig=fig, ax=ax,
                                      data_labels=[data_labels[j]] if data_labels else None
                                      )
            ax.set_title(season_names[i])
            ax.grid(True, linestyle='--', alpha=0.7)
            ax.legend_.remove() if ax.legend_ else None

        # Annual mean
        for j, data in enumerate(maps[4]):
            plot_lat_lon_profiles(mean_type='zonal',
                                  monthly_data=data, 
                                  fig=fig, ax=axs[4],
                                  data_labels=[data_labels[j]] if data_labels else None
                                  )
        axs[4].set_title("Annual Mean")
        if len(maps[4]) > 1:
            axs[4].legend(fontsize='small', loc='upper right')
        axs[4].grid(True, linestyle='--', alpha=0.7)
        return fig, axs
    else:
        raise NotImplementedError("only 'seasonal' plot type is implemented.")