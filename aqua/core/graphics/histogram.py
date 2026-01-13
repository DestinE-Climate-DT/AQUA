import xarray as xr
import matplotlib.pyplot as plt
import numpy as np

from aqua.core.logger import log_configure
from aqua.core.util import to_list, unit_to_latex
from .styles import ConfigStyle


def plot_histogram(data: xr.DataArray | list[xr.DataArray],
                   ref_data: xr.DataArray | None = None,
                   data_labels: list | None = None,
                   ref_label: str | None = None,
                   style: str | None = None,
                   fig: plt.Figure | None = None,
                   ax: plt.Axes | None = None,
                   figsize: tuple = (10, 6),
                   title: str | None = None,
                   xlogscale: bool = False,
                   ylogscale: bool = True,
                   xmax: float | None = None,
                   xmin: float | None = None,
                   ymax: float | None = None,
                   ymin: float | None = None,
                   smooth: bool = False,
                   smooth_window: int = 5,
                   loglevel: str = 'WARNING'):
    """
    Plot histogram or PDF data.

    Args:
        data (xr.DataArray | list[xr.DataArray]): Histogram data to plot. 
            Must be xarray DataArrays with 'center_of_bin' dimension.
            Can be a single DataArray or a list of DataArrays.
        ref_data (xr.DataArray, optional): Reference histogram data to plot.
        data_labels (list | None, optional): Labels for the data.
        ref_label (str | None, optional): Label for the reference data.
        style (str | None, optional): Style for the plot.
        fig (plt.Figure | None, optional): Matplotlib figure object.
        ax (plt.Axes | None, optional): Matplotlib axes object.
        figsize (tuple, optional): Figure size if a new figure is created.
        title (str | None, optional): Title for the plot.
        xlogscale (bool, optional): Use logarithmic scale for x-axis.
        ylogscale (bool, optional): Use logarithmic scale for y-axis.
        xmax (float | None, optional): Maximum value for x-axis.
        xmin (float | None, optional): Minimum value for x-axis.
        ymax (float | None, optional): Maximum value for y-axis.
        ymin (float | None, optional): Minimum value for y-axis.
        smooth (bool, optional): Apply smoothing to the data.
        smooth_window (int, optional): Window size for smoothing.
        loglevel (str, optional): Logging level.

    Returns:
        tuple: Matplotlib figure and axes objects.
    """
    logger = log_configure(loglevel, 'plot_histogram')
    ConfigStyle(style=style, loglevel=loglevel)
    
    # Convert data to list
    data_list = to_list(data)

    # Create figure if needed
    if fig is None:
        fig = plt.figure(figsize=figsize)
    if ax is None:
        ax = fig.add_subplot(1, 1, 1)

    logger.debug(f"Plotting {len(data_list)} histograms")

    # Plot data
    for i, d in enumerate(data_list):
        if 'center_of_bin' not in d.dims:
            logger.warning(f"Data {i} has no 'center_of_bin' dimension, skipping")
            continue
        
        x = d.center_of_bin.values
        y = d.values
        
        # Apply smoothing if requested
        if smooth:
            y = _smooth_data(y, window_size=smooth_window)
        
        label = data_labels[i] if data_labels and i < len(data_labels) else None
        ax.plot(x, y, label=label, linewidth=2, zorder=3)

    # Handle reference data
    if ref_data is not None:
        if 'center_of_bin' in ref_data.dims:
            x_ref = ref_data.center_of_bin.values
            y_ref = ref_data.values
            
            # Apply smoothing if requested
            if smooth:
                y_ref = _smooth_data(y_ref, window_size=smooth_window)
            
            ax.plot(x_ref, y_ref, 
                   label=ref_label if ref_label else 'Reference',
                   color='black', linestyle='-', linewidth=2, alpha=1.0, zorder=1)

    # Set scales
    if xlogscale:
        ax.set_xscale('log')
    if ylogscale:
        ax.set_yscale('log')

    # Set limits
    if xmin is not None or xmax is not None:
        ax.set_xlim(left=xmin, right=xmax)
    if ymin is not None or ymax is not None:
        ax.set_ylim(bottom=ymin, top=ymax)

    # Finalize plot
    if data_labels or ref_label:
        ax.legend(fontsize='small', loc='upper right')
    
    ax.grid(True, linestyle='-', alpha=0.3)
    
    # Set labels
    first_data = data_list[0]
    xlabel = "Value"
    if hasattr(first_data, 'center_of_bin') and hasattr(first_data.center_of_bin, 'units'):
        xlabel += f" [{unit_to_latex(first_data.center_of_bin.units)}]"
    ax.set_xlabel(xlabel)
    
    ylabel = "Frequency"
    if hasattr(first_data, 'units'):
        ylabel += f" [{unit_to_latex(first_data.units)}]"
    ax.set_ylabel(ylabel)

    # Set title if provided
    if title:
        ax.set_title(title, fontsize=13, fontweight='bold')

    return fig, ax


def _smooth_data(data, window_size=5):
    """
    Apply moving average smoothing to data.
    
    Args:
        data (array): Data to smooth.
        window_size (int): Size of smoothing window.
    
    Returns:
        array: Smoothed data.
    """
    if window_size < 2:
        return data
    
    # Simple moving average
    kernel = np.ones(window_size) / window_size
    smoothed = np.convolve(data, kernel, mode='same')
    
    # Fix edges
    for i in range(window_size // 2):
        smoothed[i] = np.mean(data[:i+window_size//2+1])
        smoothed[-(i+1)] = np.mean(data[-(i+window_size//2+1):])
    
    return smoothed