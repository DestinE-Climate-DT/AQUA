from typing import Optional, Tuple
import matplotlib.pyplot as plt
import xarray as xr
from aqua.core.util import to_list, unit_to_latex
from aqua.core.logger import log_configure
from .styles import ConfigStyle


def plot_vertical_lines(data: xr.DataArray | list[xr.DataArray],
                        ref_data: xr.DataArray = None,
                        lev_name: str = "plev",
                        lev_min: Optional[float] = None, lev_max: Optional[float] = None,
                        labels: Optional[list[str]] = None,
                        ref_label: Optional[str] = None,
                        label_size: Optional[int] = 14,
                        axis_label_size: Optional[int] = 14,
                        title: Optional[str] = None, title_size: Optional[int] = 18,
                        style: Optional[str] = None,
                        logscale: bool = False,
                        invert_yaxis: bool = True,
                        return_fig: bool = True, figsize: Tuple[int, int] = (6, 10),
                        fig: Optional[plt.Figure] = None, ax: Optional[plt.Axes] = None,
                        ax_pos: Tuple[int, int, int] = (1, 1, 1),
                        loglevel: str = "WARNING"):
    """
    Plots a vertical line plot.

    Args:
        data (xr.DataArray or list of xr.DataArray): DataArray(s) to plot.
        ref_data (xr.DataArray, optional): Reference DataArray to plot as dashed line.
        lev_name (str): Name of the vertical levels (default 'plev').
        lev_min (float, optional): Minimum vertical level to plot.
        lev_max (float, optional): Maximum vertical level to plot.
        labels (list of str, optional): Labels for each DataArray.
        ref_label (str, optional): reference data label.
        label_size (int, optional): Legend font size. Default is 14.
        axis_label_size (int, optional): Axis label font size. Default is 14.
        title (str, optional): Plot title.
        title_size (int, optional): Title font size. Default is 16.
        style (str, optional): Plot style (default aqua style).
        logscale (bool, optional): Use log scale for y-axis if True.
        invert_yaxis (bool, optional): Invert y-axis if True (default True).
        return_fig (bool, optional): If True, return (fig, ax).
        figsize (Tuple[int, int], optional): Figure size.
        fig (plt.Figure, optional): Optional figure to plot on.
        ax (plt.Axes, optional): Optional axes to plot on.
        ax_pos (Tuple[int, int, int], optional): Position of subplot.
        loglevel (str, optional): Logging level.
    """
    logger = log_configure(loglevel, "plot_vertical_line")
    ConfigStyle(style=style, loglevel=loglevel)

    data = to_list(data)
    labels = to_list(labels)

    # Prepare figure and axis
    fig = fig or plt.figure(figsize=figsize)
    ax = ax or fig.add_subplot(*ax_pos)

    # Select vertical levels
    if lev_min is None or lev_max is None:
        lev_min = min(data[lev_name].min().item() for data in data)
        lev_max = max(data[lev_name].max().item() for data in data)
        if ref_data is not None:
            lev_min = min(lev_min, ref_data[lev_name].min().item())
            lev_max = max(lev_max, ref_data[lev_name].max().item())
    else: # If at least one is provided, limit to the provided range
        ax.set_ylim(lev_max, lev_min)

    logger.debug(f"Plotting vertical line from {lev_min} to {lev_max} {lev_name}")

    if logscale:
        ax.set_yscale("log")

    units = data[0][lev_name].attrs.get("units", "")
    var_name = data[0].long_name or data[0].short_name
    var_units = data[0].attrs.get("units", "")

    units_latex = unit_to_latex(units) if units else ""

    if var_name and var_units:
        var_units_latex = unit_to_latex(var_units)
        xlabel = f"{var_name} ({var_units_latex})"
    else:
        xlabel = "Unknown variable"
        
    ylabel = f"{lev_name} ({units_latex})" if units_latex else lev_name  # Replace 'units' with actual units if available

    ax.set_ylabel(ylabel, fontsize=axis_label_size)
    ax.set_xlabel(xlabel, fontsize=axis_label_size)

    for i, d in enumerate(data):
        mask = (d[lev_name] >= lev_min) & (d[lev_name] <= lev_max)
        label = labels[i] if labels else None
        ax.plot(d.where(mask), d[lev_name].where(mask), label=label)

    logger.debug("Plotting reference data" if ref_data is not None else "No reference data to plot")
    
    if ref_data is not None:
        mask = (ref_data[lev_name] >= lev_min) & (ref_data[lev_name] <= lev_max)
        ax.plot(ref_data.where(mask), ref_data[lev_name].where(mask), label=ref_label, linestyle='--', color='black')

    ax.legend(fontsize=label_size)
    ax.grid(True, axis='y')

    if invert_yaxis:
        ax.invert_yaxis()

    if title:
        ax.set_title(title, fontsize=title_size)

    if return_fig:
        logger.debug("Returning figure and axes")
        return fig, ax
