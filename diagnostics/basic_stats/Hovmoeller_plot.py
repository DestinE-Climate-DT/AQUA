# Function to plot Hovmoller diagram by matteonurisso

import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import xarray as xr

def set_layout(fig, ax, title=None, xlabel=None, ylabel=None, xlog=False,
               ylog=False, xlim=None, ylim=None):
    """
    Set the layout of the plot

    Args:
        fig (Figure):           Figure object
        ax (Axes):              Axes object
        title (str,opt):        title of the plot
        xlabel (str,opt):       label of the x axis
        ylabel (str,opt):       label of the y axis
        xlog (bool,opt):        enable or disable x axis log scale,
                                default is False
        ylog (bool,opt):        enable or disable y axis log scale,
                                default is False
        xlim (tuple,opt):       x axis limits
        ylim (tuple,opt):       y axis limits

    Returns:
        fig (Figure):           Figure object
        ax (Axes):              Axes object
    """
    if title:
        ax.set_title(title)
    if xlabel:
        ax.set_xlabel(xlabel)
    if ylabel:
        ax.set_ylabel(ylabel)
    if xlog:
        ax.set_xscale('symlog')
    if ylog:
        ax.set_yscale('symlog')
    if xlim:
        ax.set_xlim(xlim)
    if ylim:
        ax.set_ylim(ylim)

    return fig, ax

def hovmoller_plot(infile, dim='lon', contour=True, levels=8,
                   xlabel=None, ylabel=None, title=None,
                   invert_axis=False, save=False, outputdir='./',
                   filename='hovmoller.png', **kwargs):
    '''
    Args:
        infile (DataArray):     DataArray to be plot
        dim (str,opt):          dimension to be averaged over, default is 'lon'
        contour (bool,opt):     enable or disable contour plot, default is True
        levels (int,opt):       number of contour levels, default is 8
        xlabel (str,opt):       label of the x axis
        ylabel (str,opt):       label of the y axis
        title (str,opt):        title of the plot
        invert_axis (bool,opt): enable or disable axis inversion,
                                default is False
        save (bool,opt):        enable or disable saving the figure,
                                default is False
        outputdir (str,opt):    output directory for the figure,
                                default is './'
        filename (str,opt):     filename for the figure,
                                default is 'hovmoller.png'
        **kwargs:               additional arguments for set_layout

    Returns:
        fig (Figure):           Figure object
        ax (Axes):              Axes object
    '''
    infile_mean = infile.mean(dim=dim, keep_attrs=True)

    fig, ax = plt.subplots(figsize=(12, 8))

    # Contour or pcolormesh plot
    if contour:
        if invert_axis:
            im = ax.contourf(infile_mean.coords[infile_mean.dims[-1]],
                             infile_mean.coords['time'], infile_mean,
                             levels=levels)
        else:
            im = ax.contourf(infile_mean.coords['time'],
                             infile_mean.coords[infile_mean.dims[-1]],
                             infile_mean.T, levels=levels)
    else:
        if invert_axis:
            im = ax.pcolormesh(infile_mean.coords[infile_mean.dims[-1]],
                               infile_mean.coords['time'], infile_mean)
        else:
            im = ax.pcolormesh(infile_mean.coords['time'],
                               infile_mean.coords[infile_mean.dims[-1]],
                               infile_mean.T)

    # Colorbar
    try:
        cbar_label = infile_mean.name
        cbar_label += ' [' + infile_mean.units + ']'
        print(cbar_label)
        plt.colorbar(im, ax=ax, label=cbar_label)
    except AttributeError:
        plt.colorbar(im, ax=ax, label=infile_mean.name)

    set_layout(fig, ax, xlabel=xlabel, ylabel=ylabel,
               title=title, **kwargs)

    # Custom labels if provided
    if xlabel is None:
        if invert_axis:
            ax.set_xlabel(infile_mean.dims[-1])
        else:
            ax.set_xlabel('time')
    if ylabel is None:
        if invert_axis:
            ax.set_ylabel('time')
        else:
            ax.set_ylabel(infile_mean.dims[-1])
    if title is None:
        ax.set_title(f'Hovmoller Plot ({dim} mean)')
    if save:
        fig.savefig(outputdir + filename)

    return fig, ax
