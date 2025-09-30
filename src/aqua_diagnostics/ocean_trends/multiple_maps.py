import xarray as xr
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import numpy as np
from aqua.graphics import ConfigStyle
from aqua.logger import log_configure
from aqua.util import evaluate_colorbar_limits, add_cyclic_lon


def plot_maps(maps: list[xr.DataArray],
              style=None,
              title: str = None,
              titles: list = None,
              cmap: str = "RdBu_r",
              cbar_labels: list = None,
              ytext: list = None,
              nrows: int = 6,
              ncols: int = 2,
              loglevel: str = "WARNING",
              return_fig: bool = True,
              nlevels: int = 12,
              **kwargs):
    """
    Plot multiple maps.
    """
    logger = log_configure(loglevel, "plot_maps")
    ConfigStyle(style=style, loglevel=loglevel)

    if maps is None or any(not isinstance(data_map, xr.DataArray) for data_map in maps):
        raise ValueError("Maps should be a list of xarray.DataArray")
    logger.debug("Loading maps")
    maps = [data_map.load(keep_attrs=True) for data_map in maps]

    figsize = (ncols * 6.5, nrows * 3.5)
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
        logger.warning(f"Colorbar limits for map {i}: vmin={vmin}, vmax={vmax}")

        #logger.warning("Plotting map %d, %d", i, maps[i])
        ax = axs[i]
        _ = maps[i].plot.contourf(
            ax=ax,
            transform=ccrs.PlateCarree(),
            cmap=cmap,
            vmin=vmin,
            vmax=vmax,
            levels=nlevels,
            extend='both',
            cbar_kwargs={'label': cbar_labels[i] if cbar_labels and i < len(cbar_labels) else None,
                 'ticks': ticks}  # Format to show tick levels
        )
        ax.set_aspect("auto")  # NEW: stretch plot to fill subplot
        ax.coastlines()

        if ytext:
            ax.text(-0.3, 0.33, ytext[i], fontsize=15, color='dimgray',
                    rotation=90, transform=ax.transAxes, ha='center')
        
        if titles and i < len(titles):
            ax.set_title(titles[i], fontsize=12)

    if title:
        plt.suptitle(title, fontsize=ncols*12, y=0.95)

    if return_fig:
        return fig