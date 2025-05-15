'''
This module contains simple functions for hovmoller plots.
Their main purpose is to provide a simple interface for plotting
hovmoller diagrams with xarray DataArrays.
'''
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr

from aqua.util import create_folder, evaluate_colorbar_limits, to_list
from aqua.logger import log_configure

# set default options for xarray
xr.set_options(keep_attrs=True)


def plot_hovmoller(data: xr.DataArray,
                   invert_axis=False,
                   invert_time=False,
                   sym=False,
                   contour=True, save=False,
                   dim='lon', figsize=(8, 13),
                   vmin=None, vmax=None, cmap='PuOr_r', # defaul map for MJO
                   nlevels=8, cbar_label=None,
                   outputdir='.', filename='hovmoller.pdf',
                   display=True, return_fig=False,
                   loglevel: str = "WARNING",
                   **kwargs):
    """"
    Plot a hovmoller diagram of a DataArray.

    Args:
        data (xr.DataArray):    xr.DataArray to be plot
        invert_axis (bool,opt): enable or disable axis inversion,
                                default is False
        invert_time (bool,opt): enable or disable time inversion,
                                if False, time will increase with
                                the increasing axis direction.
        sym (bool,opt):         center the cbar around zero,
                                default is False
        contour (bool,opt):     True for contour plot, False for pcolormesh,
                                default is True
        save (bool,opt):        save the figure, default is False
        dim (str|list,opt):     dimensions to be averaged over,
                                default is 'lon'
        figsize (tuple,opt):    figure size, default is (11, 8.5)
        vmin (float,opt):       minimum value for the colorbar
        vmax (float,opt):       maximum value for the colorbar
        cmap (str,opt):         colormap, default is 'RdBu_r'
        nlevels (int,opt):      number of contour levels, default is 8
        cbar_label (str,opt):   colorbar label, default is None
        outputdir (str,opt):    output directory, default is '.'
        filename (str,opt):     output filename, default is 'hovmoller.png'
        show_dim_values (bool,opt): show the values of the dimension
                                    over which the mean was taken (round them to int)
                                    Default is True
        display (bool, optional):   If True, display the figure. Defaults to True.
        return_fig (bool, optional): If True, return the figure (fig, ax). Defaults to False.
        loglevel (str,opt):     log level for the logger,
                                default is 'WARNING'

    Keyword Args:
        format (str, optional):      Format of the figure. Defaults to 'pdf'.
        dpi (int, optional):         Dots per inch. Defaults to 100 for pcolormesh
                                     and 300 for contour plots.
        xlog (bool, optional):       Use log scale for the x-axis. Defaults to False.
        ylog (bool, optional):       Use log scale for the y-axis. Defaults to False.
        xinvert (bool, optional):    Invert the x-axis. Defaults to False.
        yinvert (bool, optional):    Invert the y-axis. Defaults to False.
        title (str, optional):       Title of the plot. Defaults to None.

    Returns:
        fig, ax: tuple with the figure and axes
    """
    logger = log_configure(log_level=loglevel, log_name='plot_hovmoller')

    # Check if data is a DataArray
    if not isinstance(data, xr.DataArray):
        raise TypeError('Data is not a DataArray')

    # Evaluate the mean over the dimension to be averaged over
    dim = to_list(dim)
    logger.info('Averaging over dimensions: {}'.format(dim))
    label_text = ''
    for d in dim:
        dim_min = data.coords[d].min()
        dim_max = data.coords[d].max()
        dim_min = np.round(dim_min, 0)
        dim_max = np.round(dim_max, 0)
    
        data = data.mean(dim=d)

        label_text += f'{d} = {dim_min:.2f} to {dim_max:.2f} '
    
    # Check that we have a 2D array
    if len(data.dims) != 2:
        raise ValueError('DataArray is not 2D')

    # Create figure and axes
    fig, ax = plt.subplots(figsize=figsize)

    # Plot the data
    if invert_axis:
        logger.debug('Inverting axis for plot')
        x = data.coords[data.dims[-1]]
        y = data.coords['time']
        ax.set_xlabel(data.dims[-1])
        ax.set_ylabel('time')
        if invert_time:
            plt.gca().invert_yaxis()
    else:
        x = data.coords['time']
        y = data.coords[data.dims[-1]]
        ax.set_xlabel('time')
        ax.set_ylabel(data.dims[-1])
        if invert_time:
            plt.gca().invert_xaxis()

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
                im = ax.contourf(x, y, data, levels=levels, cmap=cmap,
                                 vmin=vmin, vmax=vmax, extend='both')
            else:
                im = ax.contourf(x, y, data.T, levels=levels, cmap=cmap,
                                 vmin=vmin, vmax=vmax, extend='both')
        except TypeError as e:
            logger.warning('Could not evaluate levels: {}'.format(e))
            if invert_axis:
                im = ax.contourf(x, y, data, cmap=cmap,
                                 vmin=vmin, vmax=vmax, extend='both')
            else:
                im = ax.contourf(x, y, data.T, cmap=cmap,
                                 vmin=vmin, vmax=vmax, extend='both')
    else:  # pcolormesh
        if invert_axis:
            im = ax.pcolormesh(x, y, data, cmap=cmap,
                               vmin=vmin, vmax=vmax, extend='both')
        else:
            im = ax.pcolormesh(x, y, data.T, cmap=cmap,
                               vmin=vmin, vmax=vmax, extend='both')

    # Adjust the location of the subplots on the page to make room for the colorbar
    fig.subplots_adjust(bottom=0.25, top=0.9, left=0.05, right=0.95,
                        wspace=0.1, hspace=0.5)

    # Add a colorbar axis at the bottom of the graph
    cbar_ax = fig.add_axes([0.2, 0.15, 0.6, 0.02])

    # Add min and max values of the dim on the top right corner
    if dim is not None:
        ax.text(0.99, 0.99, label_text,
                verticalalignment='top', horizontalalignment='right',
                transform=ax.transAxes, fontsize=12,
                bbox=dict(facecolor='white', alpha=0.5))

    # cbar label
    if cbar_label is None:
        try:
            var_name = data.short_name
        except AttributeError:
            try:
                var_name = data.long_name
            except AttributeError:
                var_name = None
        # Add units
        try:
            units = data.units
        except AttributeError:
            units = None
        if var_name is not None and units is not None:
            cbar_label = '{} [{}]'.format(var_name, units)
        elif var_name is not None:
            cbar_label = var_name
        else:
            cbar_label = None
            logger.warning('Could not find a label for the colorbar')

    fig.colorbar(im, cax=cbar_ax, orientation='horizontal',
                label=cbar_label)
    
    # Set log scale:
    if kwargs.get('ylog', False):
        ax.set_yscale('log')
    if kwargs.get('xlog', False):
        ax.set_xscale('log')

    # Set axis inversion
    if kwargs.get('xinvert', False):
        ax.invert_xaxis()
    if kwargs.get('yinvert', False):
        ax.invert_yaxis()

    # Set title
    title = kwargs.get('title', None)
    if title is not None:
        ax.set_title(title)

    # Save the figure
    if save:
        logger.debug("Saving figure to %s", outputdir)
        create_folder(outputdir, loglevel=loglevel)
        plot_format = kwargs.get('format', 'pdf')
        if filename.endswith(plot_format):
            logger.debug("Format already set in the filename")
        else:
            filename = f"{filename}.{plot_format}"
        logger.debug("Setting filename to %s", filename)

        logger.info("Saving figure as %s/%s", outputdir, filename)
        if contour:
            dpi = kwargs.get('dpi', 300)
        else:
            dpi = kwargs.get('dpi', 100)
            if dpi == 100:
                logger.info("Setting dpi to 100 by default, use dpi kwarg to change it")

        logger.info('Saving figure to {}/{}'.format(outputdir, filename))
        fig.savefig('{}/{}'.format(outputdir, filename),
                    dpi=dpi, bbox_inches='tight')

        if display is False and return_fig is False:
            logger.debug("Display is set to False, closing figure")
            plt.close(fig)
        logger.info('Saving figure to {}/{}'.format(outputdir, filename))

    if return_fig:
        logger.debug("Returning figure and axes")
        return fig, ax
