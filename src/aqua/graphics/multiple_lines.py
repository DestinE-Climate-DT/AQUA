import xarray as xr
import matplotlib.pyplot as plt

from aqua.logger import log_configure
from .styles import ConfigStyle
from .lat_lon_profiles import plot_lat_lon_profiles

def plot_lines(maps,
               plot_type: str = 'seasonal',
               style: str = None,
               loglevel='WARNING',
               data_labels: list = None,
               **kwargs):
    """
    Plots multiple lines for seasonal or annual means from pre-calculated data.
    
    This function is purely for graphics and expects pre-calculated seasonal/annual data.
    For data calculation, use the appropriate diagnostic classes (e.g., LatLonProfiles).

    Args:
        maps (list of lists): Pre-calculated data structured as [[DJF], [MAM], [JJA], [SON], [Annual]].
                             Each inner list contains DataArrays for each data series.
        plot_type (str): Type of plot, currently only 'seasonal' is supported.
        style (str): Style for the plot.
        loglevel (str): Logging level.
        data_labels (list): Labels for the data series.
        **kwargs: Additional keyword arguments for plotting.
    Returns:
        fig, axs: Matplotlib figure and axes objects.
    """

    logger = log_configure(loglevel, 'plot_lines')
    ConfigStyle(style=style, loglevel=loglevel)
    
    # Validate input data structure
    if not isinstance(maps, list) or len(maps) != 5:
        raise ValueError("maps must be a list of 5 elements: [DJF, MAM, JJA, SON, Annual]")
    
    for i, season_data in enumerate(maps):
        if not isinstance(season_data, list):
            raise ValueError(f"Season data {i} must be a list of DataArrays")

    # Create the seasonal plot layout
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
        
        # Plot seasonal means (first 4 panels)
        for i, ax in enumerate(axs[:4]):
            for j, data in enumerate(maps[i]):
                plot_lat_lon_profiles(mean_type='zonal',
                                      monthly_data=data, 
                                      fig=fig, ax=ax,
                                      data_labels=[data_labels[j]] if data_labels else None
                                      )
            ax.set_title(season_names[i])
            ax.grid(True, linestyle='--', alpha=0.7)
            if ax.legend_:
                ax.legend_.remove()

        # Annual mean (bottom panel)
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