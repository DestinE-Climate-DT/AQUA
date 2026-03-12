from pyproj import transform
from aqua.graphics.single_map import plot_single_map
import xarray as xr
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import numpy as np
from aqua.graphics import ConfigStyle
from aqua.logger import log_configure
from aqua.util import (
    evaluate_colorbar_limits,
    generate_colorbar_ticks,
)
from mpl_toolkits.axes_grid1 import make_axes_locatable


def plot_maps(maps: list[xr.DataArray],
              style=None,
              title: str = None,
              titles: list = None,
              proj: ccrs.Projection = ccrs.PlateCarree(),
              extent: list = None,
              vmin: list = None,
              vmax: list = None,
              cmap: str = "RdBu_r",
              cbar_labels: list = None,
              ytext: list = None,
              nrows: int = 6,
              ncols: int = 2,
              transform_first: bool = False,
              cyclic_lon: bool = True,
              loglevel: str = "WARNING",
              return_fig: bool = True,
              nlevels: int = 12,
              **kwargs
              ):
    """
    Plot multiple 2D maps (xarray DataArrays) in a grid layout.

    Parameters
    ----------
    maps : list[xr.DataArray]
        List of xarray DataArrays to plot.
    style : str, optional
        Plot style preset or name. If None, uses the default AQUA style.
    title : str, optional
        Overall figure title.
    titles : list[str], optional
        List of subplot titles corresponding to each DataArray in `maps`.
    proj : cartopy.crs.Projection, optional
        Map projection for plotting (default: PlateCarree).
    extent : list[float], optional
        Geographic extent as [lon_min, lon_max, lat_min, lat_max].
    cmap : str, optional
        Colormap name to use (default: "RdBu_r").
    cbar_labels : list[str], optional
        List of colorbar labels for each subplot.
    ytext : list[str], optional
        Text annotations to place on the y-axis of each subplot.
    nrows : int, optional
        Number of rows in the subplot grid (default: 6).
    ncols : int, optional
        Number of columns in the subplot grid (default: 2).
    transform_first : bool, optional
        If True, apply coordinate transformation before plotting.
    cyclic_lon : bool, optional
        Whether to make longitude cyclic for continuous global maps.
    loglevel : str, optional
        Logging verbosity level (default: "WARNING").
    return_fig : bool, optional
        If True, return the Matplotlib figure object (default: True).
    nlevels : int, optional
        Number of discrete color levels in the colormap (default: 12).
    **kwargs
        Additional keyword arguments passed to the underlying contour or pcolormesh function.

    Returns
    -------
    matplotlib.figure.Figure or None
        The Matplotlib figure if `return_fig=True`, otherwise None.

    Notes
    -----
    This function provides a convenient way to visualize multiple geospatial fields
    using Cartopy. Handles projection setup, cyclic longitude wrapping, and optional
    labeling automatically.
    """
    logger = log_configure(loglevel, "plot_maps")
    ConfigStyle(style=style, loglevel=loglevel)

    if maps is None or any(not isinstance(data_map, xr.DataArray) for data_map in maps):
        raise ValueError("Maps should be a list of xarray.DataArray")
    logger.debug("Loading maps")
    maps = [data_map.load(keep_attrs=True) for data_map in maps]

    figsize = (ncols * 4.5, nrows * 2.5)
    fig, axs = plt.subplots(
        nrows=nrows, ncols=ncols,
        figsize=figsize,
        subplot_kw={'projection': ccrs.PlateCarree()},
    )
    axs = axs.flatten()

    for i in range(len(maps)):
        try:
            maps[i] = add_cyclic_lon(maps[i])
        except Exception as e:
            logger.warning(f"Could not add cyclic longitude to map {i}: {e}")

        vmin, vmax = evaluate_colorbar_limits(maps=[maps[i]], sym=True)
        ticks = np.linspace(vmin, vmax, int(nlevels/2) + 1)
        if len(ticks) < 3:  # ensure at least 3 ticks for colorbar
            ticks = np.linspace(vmin, vmax, 3)
        logger.debug(f"Colorbar limits for map {i}: vmin={vmin}, vmax={vmax}")

        row = i // ncols
        col = i % ncols
        
        logger.debug("Plotting map %d", i)
        
        if col == 0:
            cmap = "RdBu_r"
        else:
            cmap = "coolwarm" 
        fig, ax = plot_single_map(
            data=maps[i],
            contour=True,
            proj=proj,
            extent=extent,
            # vmin=vmin,
            # vmax=vmax,
            nlevels=nlevels,
            title=titles[i] if titles is not None else None,
            cmap=cmap,
            cbar=False,
            transform_first=transform_first,
            add_land=True,
            return_fig=True,
            cyclic_lon=cyclic_lon,
            fig=fig,
            gridlines=False,
            loglevel=loglevel,
            ax_pos=(nrows, ncols, i + 1),
            ticks_rounding=0,
            **kwargs,
        )
        ax.set_aspect("auto")  # NEW: stretch plot to fill subplot
        ax.coastlines()

        gl = ax.gridlines(draw_labels=True, linewidth=0.5, color='gray', alpha=0.3)

        gl.xlabel_style = {'color': 'gray'}
        gl.ylabel_style = {'color': 'gray'}

        gl.top_labels = False
        gl.right_labels = False
        
            
        if row == nrows - 1:
            gl.bottom_labels = True
        else:
            gl.bottom_labels = False

        if col == ncols - 1:
            gl.left_labels = False
        else:
            gl.left_labels = True

        if ytext:
            ax.text(-0.3, 0.33, ytext[i], fontsize=15, color='dimgray',
                    rotation=90, transform=ax.transAxes, ha='center')


        if row == nrows - 1:
            if ax.collections:
                mappable = ax.collections[-1]
            elif ax.images:
                mappable = ax.images[-1]
            else:
                logger.warning("No mappable object found for subplot %d", i)
                continue

            # Update mappable normalization and cmap
            mappable.set_norm(plt.Normalize(vmin=vmin, vmax=vmax))
            mappable.set_cmap(cmap)

            pos = ax.get_position()
            cax = fig.add_axes([pos.x0, pos.y0 - 0.05, pos.width, 0.02])
            cbar = fig.colorbar(mappable, cax=cax, orientation="horizontal")
            cbar.set_label(cbar_labels[i])
            
            cbar_ticks_rounding = kwargs.get("cbar_ticks_rounding", 3)
            cbar_ticks = generate_colorbar_ticks(
                vmin=vmin,
                vmax=vmax,
                sym=True,
                nlevels=5,
                ticks_rounding=cbar_ticks_rounding,
                loglevel=loglevel,
            )
            cbar.set_ticks(cbar_ticks)
            
        
        
        if titles and i < len(titles):
            ax.set_title(titles[i], fontsize=12)

    if title:
        plt.suptitle(title, fontsize=ncols*9, y=0.95)

    if return_fig:
        return fig