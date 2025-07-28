import xarray as xr
import matplotlib.pyplot as plt

from aqua.logger import log_configure
from aqua.util import to_list, find_spatial_coord
from .styles import ConfigStyle

def plot_lat_lon_profiles(data: xr.DataArray | list[xr.DataArray] | None = None,
                          ref_data: xr.DataArray = None,
                          std_data: xr.DataArray | list[xr.DataArray] | None = None,
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
        data (xr.DataArray | list[xr.DataArray] | None): Data to plot. Must be xarray DataArrays 
            with 'lat', 'lon', 'latitude', or 'longitude' dimensions. Can be a single DataArray 
            or a list of DataArrays.
        ref_data (xr.DataArray, optional): Reference data to plot.
        std_data (xr.DataArray | list[xr.DataArray] | None, optional): Standard deviation 
            of the data.
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
    
    # Convert data to list, handling both single DataArrays and lists
    data_list = to_list(data)
    """
    # Validate main data
    for i, d in enumerate(data_list):
        validate_spatial_data(d, f"data[{i}]")
    
    # Validate reference data if provided
    if ref_data is not None:
        validate_spatial_data(ref_data, "ref_data")
    
    # Validate std data if provided  
    if std_data is not None:
        std_data_list = to_list(std_data)
        for i, std_d in enumerate(std_data_list):
            if std_d is not None:
                validate_spatial_data(std_d, f"std_data[{i}]")
    """
    # Handle labels
    if data_labels is None:
        labels_list = []
        for i, d in enumerate(data_list):
            # AQUA-specific attributes first
            if 'AQUA_model' in d.attrs:
                model = d.attrs['AQUA_model']
                exp = d.attrs.get('AQUA_exp', '')
                label = f"{model} {exp}".strip() if exp else model
            else:
                # Fall back to standard attributes
                label = d.attrs.get("long_name", f"Data {i+1}")
            labels_list.append(label)
    elif len(data_labels) == len(data_list):
        labels_list = data_labels
    else:
        raise ValueError(f"data_labels length ({len(data_labels)}) must match data length ({len(data_list)})")

    # Create figure if needed
    if fig is None:
        fig_size = kwargs.get('figsize', (10, 5))
        fig = plt.figure(figsize=fig_size)
    if ax is None:
        ax = fig.add_subplot(1, 1, 1)

    logger.debug(f"Plotting {len(data_list)} data arrays")

    # Plot
    for i, d in enumerate(data_list):
        # Determine coordinate name based on 'lat' or 'lon' - FIX: add None default
        coord_name = find_spatial_coord(d)
        if coord_name is None:
            logger.warning(f"Data {i} has no spatial coordinates, skipping")
            continue
            
        x_coord = d[coord_name].values
        label = labels_list[i]
        ax.plot(x_coord, d.values, label=label, linewidth=3)

    # Plot standard deviation for main data
    if std_data is not None:
        std_data_list = to_list(std_data)
        for i, (d, std_d) in enumerate(zip(data_list, std_data_list)):
            if std_d is not None:
                coord_name = find_spatial_coord(d)
                if coord_name is None:
                    continue

                x_coord = d[coord_name].values
                line_color = ax.lines[i].get_color()
                ax.fill_between(x_coord,
                            d.values - 2.*std_d.values,
                            d.values + 2.*std_d.values,
                            facecolor=line_color, alpha=0.3)

    # Handle reference data
    if ref_data is not None:
        # AQUA-specific label extraction
        if 'AQUA_model' in ref_data.attrs:
            model = ref_data.attrs['AQUA_model']
            exp = ref_data.attrs.get('AQUA_exp', '')
            ref_label_final = ref_label or (f"{model} {exp}".strip() if exp else model)
        else:
            ref_label_final = ref_label or ref_data.attrs.get("long_name", "Reference")
        
        # Find coordinate for ref_data
        coord_name = find_spatial_coord(ref_data)
        
        if coord_name is not None:
            ref_x_coord = ref_data[coord_name].values
            
            # Plot reference data
            ax.plot(ref_x_coord, ref_data.values, 
                label=ref_label_final, 
                color='black', linestyle='-', linewidth=3, alpha=1.0)
            
            # Plot reference std if available
            if ref_std_data is not None:
                ax.fill_between(ref_x_coord,
                            ref_data.values - 2.*ref_std_data.values,
                            ref_data.values + 2.*ref_std_data.values,
                            facecolor='grey', alpha=0.5)

    # Finalize plot
    ax.legend(fontsize='small')
    ax.grid(True, axis="y", linestyle='-', color='silver', alpha=0.8)

    # Set x-label based on the first valid coordinate found
    first_data = data_list[0]
    coord_name = find_spatial_coord(first_data)

    if coord_name and 'lat' in coord_name:
        ax.set_xlabel('Latitude')
    elif coord_name and 'lon' in coord_name:
        ax.set_xlabel('Longitude')

    title = kwargs.get('title', None)
    if title:
        ax.set_title(title, fontsize=13, fontweight='bold')

    return fig, ax