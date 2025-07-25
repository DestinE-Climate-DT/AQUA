import xarray as xr
import matplotlib.pyplot as plt

from aqua.logger import log_configure
from aqua.util import to_list
from .util_timeseries import plot_timeseries_data
from .styles import ConfigStyle

def plot_lat_lon_profiles(data: xr.DataArray = None,
                          ref_data: xr.DataArray = None,
                          std_data: xr.DataArray = None,
                          ref_std_data: xr.DataArray = None,
                          data_labels: list = None,
                          ref_label: str = None,
                          style: str = None,
                          fig: plt.Figure = None, 
                          ax: plt.Axes = None,
                          loglevel='WARNING',
                          **kwargs):
    """
    Plot latitude or longitude profiles of data, averaging over the specified axis.

    Args:
        data (xr.DataArray or list): Data to plot.
        ref_data (xr.DataArray, optional): Reference data to plot.
        std_data (xr.DataArray or list, optional): Standard deviation of the main data.
        ref_std_data (xr.DataArray, optional): Standard deviation of the reference data.
        data_labels (list, optional): Labels for the data.
        ref_label (str, optional): Label for the reference data.
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

    # Handle the input data - convert to list safely
    if data is None:
        raise ValueError("No data provided for plotting")
    
    # Convert data to list, handling both single DataArrays and lists
    if isinstance(data, list):
        data_list = data
    else:
        data_list = [data]
    
    # Validate that we have actual data
    if not data_list or len(data_list) == 0:
        raise ValueError("No data provided for plotting")
    
    # Validate each data array
    valid_data_list = []
    for i, d in enumerate(data_list):
        if d is None:
            logger.warning(f"Data array {i} is None, skipping")
            continue
        if not isinstance(d, xr.DataArray):
            logger.warning(f"Data array {i} is not an xarray DataArray, skipping")
            continue
        if d.size == 0:
            logger.warning(f"Data array {i} is empty, skipping")
            continue
        # Check if data has spatial dimensions for profiling
        if not any(dim in d.dims for dim in ['lat', 'lon', 'latitude', 'longitude']):
            logger.warning(f"Data array {i} has no spatial dimensions (lat/lon), skipping")
            continue
        valid_data_list.append(d)
    
    if not valid_data_list:
        raise ValueError("No valid data arrays found for plotting")
    
    data_list = valid_data_list
    
    # Handle labels
    if data_labels is None or len(data_labels) < len(data_list):
        labels_list = [
            d.attrs.get("long_name", f"Data {i+1}") for i, d in enumerate(data_list)
        ]
    else:
        labels_list = data_labels[:len(data_list)]

    # Create figure if needed
    if fig is None and ax is None:
        fig_size = kwargs.get('figsize', (10, 5))
        fig, ax = plt.subplots(1, 1, figsize=fig_size)

    logger.debug(f"Plotting {len(data_list)} data arrays")

    # Plot main data using direct matplotlib instead of plot_timeseries_data
    for i, d in enumerate(data_list):
        if 'lat' in d.dims:
            x_coord = d.lat.values
            x_label = 'Latitude'
        elif 'lon' in d.dims:
            x_coord = d.lon.values  
            x_label = 'Longitude'
        else:
            logger.warning(f"Data {i} has no lat/lon dimension, skipping")
            continue
            
        label = labels_list[i] if i < len(labels_list) else f"Data {i+1}"
        ax.plot(x_coord, d.values, label=label, linewidth=3)
    
    # Plot standard deviation for main data
    if std_data is not None:
        if isinstance(std_data, list):
            std_data_list = std_data
        else:
            std_data_list = [std_data]
        
        for i, (d, std_d) in enumerate(zip(data_list, std_data_list)):
            if std_d is not None and std_d.size > 0:
                try:
                    if 'lat' in d.dims:
                        x_coord = d.lat.values
                    elif 'lon' in d.dims:
                        x_coord = d.lon.values
                    else:
                        continue
                    
                    # Get the color of the corresponding line
                    line_color = ax.lines[i].get_color()
                    
                    # Plot fill_between for 2*std
                    ax.fill_between(x_coord,
                                d.values - 2.*std_d.values,
                                d.values + 2.*std_d.values,
                                facecolor=line_color, 
                                alpha=0.3)
                except Exception as e:
                    logger.warning(f"Failed to plot std for data {i}: {e}")
    
    # Handle reference data with std
    if ref_data is not None:
        if isinstance(ref_data, xr.DataArray) and ref_data.size > 0:
            if any(dim in ref_data.dims for dim in ['lat', 'lon', 'latitude', 'longitude']):
                ref_label_final = ref_label if ref_label is not None else "Reference"
                
                try:
                    if 'lat' in ref_data.dims:
                        ref_x_coord = ref_data.lat.values
                    elif 'lon' in ref_data.dims:
                        ref_x_coord = ref_data.lon.values
                    else:
                        ref_x_coord = None
                        
                    if ref_x_coord is not None:
                        # Plot reference data
                        ax.plot(ref_x_coord, ref_data.values, 
                            label=ref_label_final, 
                            color='black',
                            linestyle='-',
                            linewidth=3,
                            alpha=1.0)
                        
                        # Plot reference std if available
                        if ref_std_data is not None and ref_std_data.size > 0:
                            ax.fill_between(ref_x_coord,
                                        ref_data.values - 2.*ref_std_data.values,
                                        ref_data.values + 2.*ref_std_data.values,
                                        facecolor='grey', 
                                        alpha=0.5)
                        
                except Exception as e:
                    logger.warning(f"Failed to plot reference data: {e}")
            else:
                logger.warning("Reference data has no spatial dimensions, skipping")
        else:
            logger.warning("Reference data is invalid or empty, skipping")

    # Finalize plot
    ax.legend(fontsize='small')
    ax.grid(True, axis="y", linestyle='-', color='silver', alpha=0.8)
    
    # Set x-label based on the dimension
    first_data = data_list[0]
    if 'lat' in first_data.dims:
        ax.set_xlabel('Latitude')
    elif 'lon' in first_data.dims:
        ax.set_xlabel('Longitude')

    title = kwargs.get('title', None)
    if title:
        ax.set_title(title, fontsize=13, fontweight='bold')

    return fig, ax