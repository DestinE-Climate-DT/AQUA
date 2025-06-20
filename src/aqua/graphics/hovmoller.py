'''
This module contains simple functions for hovmoller plots.
Their main purpose is to provide a simple interface for plotting
hovmoller diagrams with xarray DataArrays.
'''
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr

from aqua.util import create_folder, evaluate_colorbar_limits
from aqua.logger import log_configure
from .styles import ConfigStyle

# set default options for xarray
xr.set_options(keep_attrs=True)


def plot_hovmoller(data: xr.DataArray,
                   invert_axis=False,
                   invert_time=False,
                   invert_space_coord=False,
                   sym=False,
                   style=None,
                   contour=True,
                   dim='lon', figsize=(8, 13),
                   vmin=None, vmax=None, cmap='PuOr_r', # defaul map for MJO
                   title = None,
                   box_text=True,
                   cbar: bool = True,
                   text: list | str = None,
                   nlevels=8,
                   cbar_label=None,
                   cbar_pos: list = [0.2, 0.15, 0.6, 0.02],
                   cbar_orientation: str = 'horizontal',
                   return_fig=False,
                   fig: plt.Figure = None, ax: plt.Axes = None,
                   ax_pos: tuple = (1, 1, 1),
                   loglevel: str = "WARNING",
                   ):
    
    logger = log_configure(log_level=loglevel, log_name='Hovmoller')
    ConfigStyle(style=style, loglevel=loglevel)

    # Check if data is a DataArray
    if not isinstance(data, xr.DataArray):
        raise TypeError('Data is not a DataArray')

    # Evaluate the mean over the dimension to be averaged over
    logger.info('Averaging over dimension: {}'.format(dim))
    dim_min = data.coords[dim].min()
    dim_max = data.coords[dim].max()
    dim_min = np.round(dim_min, 0)
    dim_max = np.round(dim_max, 0)

    data_mean = data.mean(dim=dim)

    # Create figure and axes
    if fig is None:
        fig = plt.figure(figsize=figsize)
    if ax is None:
        ax = fig.add_subplot(ax_pos[0], ax_pos[1], ax_pos[2])

    # Plot the data
    if invert_axis:
        logger.debug('Inverting axis for plot')
        x = data_mean.coords[data_mean.dims[-1]]
        y = data_mean.coords['time']
        ax.set_xlabel(data_mean.dims[-1])
        ax.set_ylabel('time')
        if invert_time:
            plt.gca().invert_yaxis()
        if invert_space_coord:
            plt.gca().invert_xaxis()
    else:
        x = data_mean.coords['time']
        y = data_mean.coords[data_mean.dims[-1]]
        ax.set_xlabel('time')
        ax.set_ylabel(data_mean.dims[-1])
        if invert_time:
            plt.gca().invert_xaxis()
        if invert_space_coord:
            plt.gca().invert_yaxis()

    if vmin is None or vmax is None:
        vmin, vmax = evaluate_colorbar_limits(maps=[data], sym=sym)
    else:
        if sym:
            logger.warning("sym=True, vmin and vmax given will be ignored")
            vmin, vmax = evaluate_colorbar_limits(maps=[data], sym=sym)
    logger.debug("Setting vmin to %s, vmax to %s", vmin, vmax)

    if contour:
        try:
            levels = np.linspace(vmin, vmax, nlevels)
            if invert_axis:
                im = ax.contourf(x, y, data_mean, levels=levels, cmap=cmap,
                                 vmin=vmin, vmax=vmax, extend='both')
            else:
                im = ax.contourf(x, y, data_mean.T, levels=levels, cmap=cmap,
                                 vmin=vmin, vmax=vmax, extend='both')
        except TypeError as e:
            logger.warning('Could not evaluate levels: {}'.format(e))
            if invert_axis:
                im = ax.contourf(x, y, data_mean, cmap=cmap,
                                 vmin=vmin, vmax=vmax, extend='both')
            else:
                im = ax.contourf(x, y, data_mean.T, cmap=cmap,
                                 vmin=vmin, vmax=vmax, extend='both')
    else:  # pcolormesh
        if invert_axis:
            im = ax.pcolormesh(x, y, data_mean, cmap=cmap,
                               vmin=vmin, vmax=vmax)
        else:
            im = ax.pcolormesh(x, y, data_mean.T, cmap=cmap,
                               vmin=vmin, vmax=vmax)

    # Adjust the location of the subplots on the page to make room for the colorbar
    fig.subplots_adjust(bottom=0.25, top=0.9, left=0.05, right=0.95,
                        wspace=0.1, hspace=0.5)

    if cbar:
        # if cbar_pos is None:
        box = ax.get_position()
        cbar_ax = fig.add_axes([
            box.x0 - 0.01 + box.width + 0.01,  # a small gap to the right
            box.y0,                     # same vertical start
            0.015,                      # narrow width
            box.height                  # same height
        ])
        # else:
        #     cbar_ax = fig.add_axes(cbar_pos)


    if box_text:
        # Add min and max values of the dim on the top right corner
        ax.text(0.99, 0.99, f'{dim} = {dim_min:.2f} to {dim_max:.2f}',
                verticalalignment='top', horizontalalignment='right',
                transform=ax.transAxes, fontsize=12,
                bbox=dict(facecolor='white', alpha=0.5))
    if text:
        logger.debug("Adding text in the plot: %s", text)
        ax.text(-0.3, 0.33, text, fontsize=15, color='dimgray', rotation=90, transform=ax.transAxes, ha='center')

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
            cbar_label = '{} [{}]'.format(var_name, units)
        elif var_name is not None:
            cbar_label = var_name
        else:
            cbar_label = None
            logger.warning('Could not find a label for the colorbar')

    fig.colorbar(im, cax=cbar_ax, orientation=cbar_orientation,
                 label=cbar_label)
    ax.set_title(title, fontsize=20)

    if return_fig:
        logger.debug("Returning figure and axes")
        return fig, ax
