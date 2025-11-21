import xarray as xr
import matplotlib.pyplot as plt

from aqua.core.logger import log_configure
from aqua.core.util import to_list, coord_names
from .styles import ConfigStyle

def plot_lat_lon_profiles(data: xr.DataArray | list[xr.DataArray],
                          ref_data: xr.DataArray | None = None,
                          ref_std_data: xr.DataArray | None = None,
                          data_labels: list | None = None,
                          ref_label: str | None = None,
                          style: str | None = None,
                          fig: plt.Figure | None = None,
                          ax: plt.Axes | None = None,
                          figsize: tuple = (10, 5),
                          title: str | None = None,
                          loglevel: str = 'WARNING'):
    """
    Plot latitude or longitude profiles of data, averaging over the specified axis.

    Args:
        data (xr.DataArray | list[xr.DataArray] | None): Data to plot. Must be xarray DataArrays 
            with 'lat', 'lon', 'latitude', or 'longitude' dimensions. Can be a single DataArray 
            or a list of DataArrays.
        ref_data (xr.DataArray, optional): Reference data to plot.
        ref_std_data (xr.DataArray | None, optional): Standard deviation of the reference data.
        data_labels (list | None, optional): Labels for the data.
        ref_label (str | None, optional): Label for the reference data.
        style (str | None, optional): Style for the plot.
        fig (plt.Figure | None, optional): Matplotlib figure object.
        ax (plt.Axes | None, optional): Matplotlib axes object.
        figsize (tuple, optional): Figure size if a new figure is created.
        title (str | None, optional): Title for the plot.
        loglevel (str, optional): Logging level.

    Returns:
        tuple: Matplotlib figure and axes objects.
    """
    logger = log_configure(loglevel, 'plot_lat_lon_profiles')
    ConfigStyle(style=style, loglevel=loglevel)
    
    # Convert data to list, handling both single DataArrays and lists
    data_list = to_list(data)

    # Create figure if needed
    if fig is None:
        fig = plt.figure(figsize=figsize)
    if ax is None:
        ax = fig.add_subplot(1, 1, 1)

    logger.debug(f"Plotting {len(data_list)} data arrays")

    # Plot data (higher zorder = foreground)
    for i, d in enumerate(data_list):
        # Determine coordinate name based on 'lat' or 'lon'
        lon_name, lat_name = coord_names(d)
        coord_name = lat_name if lat_name is not None else lon_name

        if coord_name is None:
            logger.warning(f"Data {i} has no spatial coordinates, skipping")
            continue
            
        label = data_labels[i] if data_labels else None
        d.plot(ax=ax, label=label, linewidth=3, zorder=3)

    # Handle reference data (lower zorder = background)
    if ref_data is not None:
        
        # Find coordinate for ref_data
        lon_name, lat_name = coord_names(ref_data)
        coord_name = lat_name if lat_name is not None else lon_name
        
        if coord_name is not None:
            ref_x_coord = ref_data[coord_name].values
            
            # Plot reference std if available
            if ref_std_data is not None:
                if hasattr(ref_std_data, 'compute'):
                    ref_std_data = ref_std_data.compute()
                ax.fill_between(ref_x_coord,
                                ref_data.values - 2.*ref_std_data.values,
                                ref_data.values + 2.*ref_std_data.values,
                                facecolor='grey', alpha=0.5, zorder=1)

            # Plot reference data
            ref_data.plot(ax=ax, label=ref_label if ref_label else 'Reference',
                          color='black', linestyle='-', linewidth=3, alpha=1.0,
                          zorder=2)

    # Finalize plot
    if data_labels:
        ax.legend(fontsize='small')
    ax.grid(True, axis="y", linestyle='-', color='silver', alpha=0.8)

    # Set x-label based on the first valid coordinate found
    first_data = data_list[0]
    lon_name, lat_name = coord_names(first_data)
    coord_name = lat_name if lat_name is not None else lon_name

    if coord_name and 'lat' in coord_name:
        ax.set_xlabel('Latitude')
    elif coord_name and 'lon' in coord_name:
        ax.set_xlabel('Longitude')

    # Set title if provided
    if title:
        ax.set_title(title, fontsize=13, fontweight='bold')

    return fig, ax