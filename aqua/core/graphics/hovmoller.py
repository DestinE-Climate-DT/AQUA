"""
This module contains simple functions for hovmoller plots.
Their main purpose is to provide a simple interface for plotting
hovmoller diagrams with xarray DataArrays.
"""

import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
from matplotlib.colors import BoundaryNorm

from aqua.core.logger import log_configure
from aqua.core.util import evaluate_colorbar_limits, unit_to_latex

from .styles import ConfigStyle

# set default options for xarray
xr.set_options(keep_attrs=True)


def plot_hovmoller(
    data: xr.DataArray,
    invert_axis=False,
    invert_time=False,
    invert_space_coord=False,
    space_limits: tuple = None,
    sym=False,
    style=None,
    contour=True,
    dim="lon",
    figsize=(8, 13),
    vmin=None,
    vmax=None,
    cmap="PuOr_r",  # defaul map for MJO
    title=None,
    box_text=True,
    cbar: bool = True,
    text: list | str = None,
    nlevels=8,
    levels: list = None,
    cbar_label=None,
    cbar_orientation: str = "horizontal",
    return_fig=False,
    fig: plt.Figure = None,
    ax: plt.Axes = None,
    ax_pos: tuple = (1, 1, 1),
    loglevel: str = "WARNING",
):
    """
    Plot a hovmoller diagram for a given xarray DataArray.

    Args:
        data (xr.DataArray): The data to be plotted.
        invert_axis (bool): If True, the x-axis will be inverted.
        invert_time (bool): If True, the direction time axis will be inverted.
        invert_space_coord (bool): If True, the space coordinate axis will be inverted.
        space_limits (tuple): (min, max) range of the space coordinate to show.
            If None, the bands where the data are NaN at every time are left out.
        sym (bool): If True, the color limits will be symmetric around zero.
        style (str): The style to be used for the plot. Default is None, which uses the default AQUA style.
        contour (bool): If True, contours will be plotted. If False, pcolormesh will be used.
        dim (str): The dimension to be averaged over. Default is 'lon'.
        figsize (tuple): Size of the figure. Default is (8, 13).
        vmin (float): Minimum value for the color limits. If None, it will be evaluated from the data.
        vmax (float): Maximum value for the color limits. If None, it will be evaluated from the data.
        cmap (str): Colormap to be used for the plot. Default is 'PuOr_r'.
        title (str): Title for the plot. If None, no title will be set.
        box_text (bool): If True, a box with the min and max values of the dimension will be added to the plot.
        cbar (bool): If True, a colorbar will be added to the plot.
        text (list | str): Text to be added to the plot. If None, no text will be added.
        nlevels (int): Number of contour levels. Default is 8.
        levels (list): Contour levels, also unevenly spaced ones. If given, they replace vmin, vmax and nlevels,
            and each interval between them gets its own color of the colormap.
        cbar_label (str): Label for the colorbar. If None, it will be generated from the data attributes.
        cbar_orientation (str): Orientation of the colorbar. Default is 'horizontal'.
        return_fig (bool): If True, the figure and axes will be returned. Default is False.
        fig (plt.Figure): Matplotlib figure object to plot on. If None, a new figure will be created.
        ax (plt.Axes): Matplotlib axes object to plot on. If None, a new axes will be created.
        ax_pos (tuple): Position of the axes in the figure. Default is (1, 1, 1), which means a single subplot.
        loglevel (str): Logging level. Default is "WARNING".

    Returns:
        plt.Figure: The matplotlib figure object containing the hovmoller plot.
        plt.Axes: The matplotlib axes object containing the hovmoller plot.
    """
    logger = log_configure(log_level=loglevel, log_name="Hovmoller")
    ConfigStyle(style=style, loglevel=loglevel)

    # Check if data is a DataArray
    if not isinstance(data, xr.DataArray):
        raise TypeError("Data is not a DataArray")

    dim_min = None
    dim_max = None

    if dim:
        # Evaluate the mean over the dimension to be averaged over
        logger.info("Averaging over dimension: {}".format(dim))
        dim_min = data.coords[dim].min()
        dim_max = data.coords[dim].max()
        dim_min = np.round(dim_min, 0)
        dim_max = np.round(dim_max, 0)

        data_mean = data.mean(dim=dim)
    else:
        data_mean = data

    # Create figure and axes
    if fig is None:
        fig = plt.figure(figsize=figsize)
    if ax is None:
        ax = fig.add_subplot(ax_pos[0], ax_pos[1], ax_pos[2])

    # Label the space axis with the units of its coordinate, if available
    space_dim = data_mean.dims[-1]
    space_units = data_mean[space_dim].attrs.get("units")
    space_label = f"{space_dim} ({unit_to_latex(space_units)})" if space_units else space_dim

    # Plot the data
    if invert_axis:
        logger.debug("Inverting axis for plot")
        x = data_mean.coords[space_dim]
        y = data_mean.coords["time"]
        ax.set_xlabel(space_label)
        ax.set_ylabel("time")
        if invert_time:
            plt.gca().invert_yaxis()
        if invert_space_coord:
            plt.gca().invert_xaxis()
    else:
        x = data_mean.coords["time"]
        y = data_mean.coords[space_dim]
        ax.set_xlabel("time")
        ax.set_ylabel(space_label)
        if invert_time:
            plt.gca().invert_xaxis()
        if invert_space_coord:
            plt.gca().invert_yaxis()

    if levels is not None:
        vmin, vmax = levels[0], levels[-1]
    elif vmin is None or vmax is None:
        vmin, vmax = evaluate_colorbar_limits(maps=[data], sym=sym)
    else:
        if sym:
            logger.warning("sym=True, vmin and vmax given will be ignored")
            vmin, vmax = evaluate_colorbar_limits(maps=[data], sym=sym)
    logger.debug("Setting vmin to %s, vmax to %s", vmin, vmax)

    # Extend the colorbar only on the sides where the data exceed the color limits
    below, above = bool(data_mean.min() < vmin), bool(data_mean.max() > vmax)
    extend = "both" if below and above else "min" if below else "max" if above else "neither"
    logger.debug("Colorbar extension: %s", extend)

    if contour:
        try:
            if levels is None:
                levels = np.linspace(vmin, vmax, nlevels)
                color_kwargs = {"vmin": vmin, "vmax": vmax}
            else:
                # Each interval of the given levels, even uneven ones, gets its own color of the colormap
                color_kwargs = {"norm": BoundaryNorm(levels, ncolors=plt.get_cmap(cmap).N, extend=extend)}
            if invert_axis:
                im = ax.contourf(x, y, data_mean, levels=levels, cmap=cmap, extend=extend, **color_kwargs)
            else:
                im = ax.contourf(x, y, data_mean.T, levels=levels, cmap=cmap, extend=extend, **color_kwargs)
        except TypeError as e:
            logger.warning("Could not evaluate levels: {}".format(e))
            if invert_axis:
                im = ax.contourf(x, y, data_mean, cmap=cmap, vmin=vmin, vmax=vmax, extend=extend)
            else:
                im = ax.contourf(x, y, data_mean.T, cmap=cmap, vmin=vmin, vmax=vmax, extend=extend)
    else:  # pcolormesh
        if invert_axis:
            im = ax.pcolormesh(x, y, data_mean, cmap=cmap, vmin=vmin, vmax=vmax)
        else:
            im = ax.pcolormesh(x, y, data_mean.T, cmap=cmap, vmin=vmin, vmax=vmax)

    if space_limits is None:
        # Leave out the bands where the data are NaN at every time (e.g. below the sea floor)
        has_data = data_mean.notnull().any(dim="time").values
        if has_data.any() and not has_data.all():
            valid_coords = data_mean[space_dim].values[has_data]
            space_limits = (valid_coords.min(), valid_coords.max())
    if space_limits is not None:
        low, high = sorted(space_limits)
        limits = (high, low) if invert_space_coord else (low, high)
        logger.debug("Setting space coordinate limits to %s", limits)
        if invert_axis:
            ax.set_xlim(limits)
        else:
            ax.set_ylim(limits)

    if cbar is True:
        # Adjust the location of the subplots on the page to make room for the colorbar
        fig.subplots_adjust(bottom=0.25, top=0.9, left=0.05, right=0.95, wspace=0.1, hspace=0.5)
        box = ax.get_position()
        cbar_ax = fig.add_axes(
            [
                box.x0 - 0.01 + box.width + 0.01,  # a small gap to the right
                box.y0,  # same vertical start
                0.015,  # narrow width
                box.height,  # same height
            ]
        )
        # cbar label
        if cbar_label is None:
            try:
                var_name = data_mean.short_name
            except AttributeError:
                try:
                    var_name = data_mean.long_name
                except AttributeError:
                    var_name = None
            # Add units
            try:
                units = data_mean.units
            except AttributeError:
                units = None
            if var_name is not None and units is not None:
                cbar_label = "{} [{}]".format(var_name, unit_to_latex(units))
            elif var_name is not None:
                cbar_label = var_name
            else:
                cbar_label = None
                logger.warning("Could not find a label for the colorbar")

        fig.colorbar(im, cax=cbar_ax, orientation=cbar_orientation, label=cbar_label)

    if box_text and dim is not None and dim_min is not None and dim_max is not None:
        # Add min and max values of the dim on the top right corner
        ax.text(
            0.99,
            0.99,
            f"{dim} = {dim_min:.2f} to {dim_max:.2f}",
            verticalalignment="top",
            horizontalalignment="right",
            transform=ax.transAxes,
            fontsize="medium",
            bbox=dict(facecolor="white", alpha=0.5),
        )
    if text:
        logger.debug("Adding text in the plot: %s", text)
        ax.text(-0.3, 0.33, text, fontsize="large", color="dimgray", rotation=90, transform=ax.transAxes, ha="center")

    if title:
        ax.set_title(title)

    if return_fig:
        logger.debug("Returning figure and axes")
        return fig, ax
