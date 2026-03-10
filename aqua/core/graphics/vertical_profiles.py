from typing import Optional, Tuple
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr

from aqua.core.logger import log_configure
from aqua.core.util import evaluate_colorbar_limits, unit_to_latex
from .styles import ConfigStyle


def plot_vertical_profile(data: xr.DataArray, var: str= None,
                          lev_name: str = "plev", x_coord: str = "lat",
                          lev_min: Optional[float] = None,lev_max: Optional[float] = None,
                          vmin: Optional[float] = None, vmax: Optional[float] = None,
                          nlevels: int = 18, 
                          title: Optional[str] = None, title_size: Optional[int] = 16,
                          style: Optional[str] = None,
                          logscale: bool = False,
                          grid: bool = True,
                          add_land: bool = False,
                          cbar: bool = True,
                          cmap: str = "RdBu_r",
                          cbar_label: Optional[str] = None,
                          return_fig: bool = False, figsize: Tuple[int, int] = (10, 8),
                          fig: Optional[plt.Figure] = None, ax: Optional[plt.Axes] = None,
                          ax_pos: Tuple[int, int, int] = (1, 1, 1),
                          loglevel: str = "WARNING"):
    """
    Plots a zonal mean vertical profile.

    Args:
        data: DataArray to plot.
        var (str): Variable name for labeling purposes.
        lev_name (str): Name of the vertical levels (default 'plev').
        x_coord (str): Name of the horizontal coordinate (default 'lat').
        lev_min (float, optional): Minimum vertical level to plot.
        lev_max (float, optional): Maximum vertical level to plot.
        vmin (float, optional): Minimum colorbar limit.
        vmax (float, optional): Maximum colorbar limit.
        nlevels (int): Number of contour levels. Default is 18.
        title (str, optional): Plot title.
        title_size (int, optional): Title font size. Default is 16.
        style (str, optional): Plot style (default aqua style).
        logscale (bool, optional): Use log scale for y-axis if True.
        grid (bool, optional): If True, display grid lines on the plot.
        add_land (bool, optional): If True, shade land areas in the background.
        cbar (bool, optional): If True, display colorbar.
        cmap (str, optional): Colormap to use (default 'RdBu_r').
        cbar_label (str, optional): Label for the colorbar. Generated from data if None.
        return_fig (bool, optional): If True, return (fig, ax).
        figsize (Tuple[int, int], optional): Figure size.
        fig (plt.Figure, optional): Optional figure to plot on.
        ax (plt.Axes, optional): Optional axes to plot on.
        ax_pos (Tuple[int, int, int], optional): Position of subplot.
        loglevel (str, optional): Logging level.
    """
    logger = log_configure(loglevel, "plot_vertical_profile")
    ConfigStyle(style=style, loglevel=loglevel)

    # Select vertical levels
    lev_min = lev_min or data[lev_name].min().item()
    lev_max = lev_max or data[lev_name].max().item()
    mask = (data[lev_name] >= lev_min) & (data[lev_name] <= lev_max)
    data = data.sel({lev_name: data[lev_name].where(mask, drop=True)})

    # Ensure reasonable number of levels
    nlevels = max(2, int(nlevels))

    # Auto colorbar limits if not provided
    if vmin is None or vmax is None:
        vmin, vmax = float(data.min()), float(data.max())
        if vmin * vmax < 0:  # symmetric around 0
            vmax = max(abs(vmin), abs(vmax))
            vmin = -vmax

    levels = np.linspace(vmin, vmax, nlevels)

    # Prepare figure and axis
    fig = fig or plt.figure(figsize=figsize)
    ax = ax or fig.add_subplot(*ax_pos)

    # Plot
    cax = ax.contourf(data[x_coord], data[lev_name], data,
                      cmap=cmap, levels=levels, extend="both")
    if logscale:
        ax.set_yscale("log")
    ax.set_xlabel("Latitude") if x_coord == "lat" else x_coord
    ax.set_ylabel("Pressure Level (Pa)" if lev_name == "plev" else lev_name)
    ax.invert_yaxis()
    if cbar:
        if cbar_label is None:
            units = data.attrs.get('units', '')
            units_latex = unit_to_latex(units) if units else ''
            cbar_label = f"{var} [{units_latex}]" if units_latex else f"{var}"
        fig.colorbar(cax, ax=ax, label=cbar_label)
    if grid:
        ax.grid(True)

    if add_land:
        logger.debug("Adding land")
        ax.set_facecolor(color='grey')

    if title:
        logger.debug("Setting title to %s", title)
        ax.set_title(title, fontsize=title_size)

    if return_fig:
        logger.debug("Returning figure and axes")
        return fig, ax


def plot_vertical_profile_diff(data: xr.DataArray, data_ref: xr.DataArray,
                               var: str, 
                               lev_name: str = "plev", x_coord: str = "lat",
                               lev_min: Optional[float] = None, lev_max: Optional[float] = None,
                               vmin: Optional[float] = None, vmax: Optional[float] = None,
                               vmin_contour: Optional[float] = None, vmax_contour: Optional[float] = None,
                               sym_contour: bool = False, add_contour: bool = False,
                               nlevels: int = 18,
                               title: Optional[str] = None, title_size: Optional[int] = 16,
                               style: Optional[str] = None,
                               return_fig: bool = False, fig: Optional[plt.Figure] = None,
                               ax: Optional[plt.Axes] = None, ax_pos: Tuple[int, int, int] = (1, 1, 1),
                               loglevel: str = "WARNING",
                                **kwargs):
    """
    Plot the difference (data - data_ref) as vertical profile.
    Optionally add contour lines of the reference data.

    Args:
        data (xr.DataArray): Dataset to plot.
        data_ref (xr.DataArray): DataArrays to compare. A contour of this will be added if add_contour is True.
        var (str): Variable name for labeling purposes.
        lev_name (str): Name of the vertical levels. Default is 'plev'.
        x_coord (str): Name of the horizontal coordinate.
        lev_min (float, optional): Minimum vertical level to plot.
        lev_max (float, optional): Maximum vertical level to plot.
        vmin (float, optional): Minimum colorbar limit.
        vmax (float, optional): Maximum colorbar limit.
        vmin_contour (float, optional): Minimum contour limit.
        vmax_contour (float, optional): Maximum contour limit.
        sym_contour (bool, optional): If True, contour limits symmetric around zero.
        add_contour (bool, optional): If True, overlay contour lines from reference data.
        nlevels (int, optional): Number of contour levels.
        title (str, optional): Plot title.
        title_size (int, optional): Title font size. Default is 16.
        style (str, optional): Plot style (default aqua style).
        return_fig (bool, optional): If True, return (fig, ax).
        fig (plt.Figure, optional): Optional figure to plot on.
        ax (plt.Axes, optional): Optional axes to plot on.
        ax_pos (Tuple[int, int, int], optional): Position of subplot.
        loglevel (str, optional): Logging level.
        **kwargs: Additional arguments passed to plot_vertical_profile.
    """
    logger = log_configure(loglevel, "plot_vertical_profile_diff")
    ConfigStyle(style=style, loglevel=loglevel)

    # Difference
    diff = data - data_ref

    fig, ax = plot_vertical_profile(
        diff, var=var, lev_min=lev_min, lev_max=lev_max,
        vmin=vmin, vmax=vmax, nlevels=nlevels,
        style=style, return_fig=True, fig=fig, ax=ax, ax_pos=ax_pos,
        loglevel=loglevel, **kwargs)

    # Add contours of reference data
    if add_contour:
        # Contour limits
        if vmin_contour is None or vmax_contour is None:
            vmin_contour, vmax_contour = evaluate_colorbar_limits([data], sym=sym_contour)

        levels = np.linspace(vmin_contour, vmax_contour, max(2, int(nlevels)))
        data_common = data.sel({lev_name: diff[lev_name]})

        cs = ax.contour(data_common[x_coord], data_common[lev_name], data_common,
                        levels=levels, colors="k", linewidths=0.5)
        fmt = {lvl: f"{lvl:.1e}" if (abs(lvl) < 0.1 or abs(lvl) > 1000)
               else f"{lvl:.1f}" for lvl in cs.levels}
        ax.clabel(cs, fmt=fmt, fontsize=6, inline=True)

    if title:
        logger.debug("Setting title to %s", title)
        ax.set_title(title, fontsize=title_size)

    if return_fig:
        logger.debug("Returning figure and axes")
        return fig, ax
