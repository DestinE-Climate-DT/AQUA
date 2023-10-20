import matplotlib.pyplot as plt
import matplotlib.colors as colors
# import boost_histogram as bh  # pip
from matplotlib.gridspec import GridSpec
import seaborn as sns

from aqua.util import create_folder
from aqua.logger import log_configure
from .tropical_rainfall_func import extract_directory_path

import cartopy.crs as ccrs
import cartopy.mpl.ticker as cticker
from cartopy.util import add_cyclic_point

import numpy as np
import xarray as xr

def savefig(path_to_pdf=None, pdf_format=True):
        """
        Save the current figure to a file in either PDF or PNG format.

        Args:
            path_to_pdf (str): The file path where the figure will be saved. If None, the figure will not be saved.
            pdf_format (bool): If True, the figure will be saved in PDF format; otherwise, it will be saved in PNG format.

        Returns:
            None

        Note:
            The function first checks the `path_to_pdf` to determine the format of the saved figure. If `pdf_format` is set to True, the figure will be saved in PDF format with the specified path. If `pdf_format` is False, the function replaces the '.pdf' extension in the `path_to_pdf` with '.png' and saves the figure in PNG format.

        Example:
            savefig(path_to_pdf='example.pdf', pdf_format=True)
            # This will save the current figure in PDF format as 'example.pdf'.

        """
        create_folder(folder=extract_directory_path(
                    path_to_pdf), loglevel='WARNING')
        
        if pdf_format:
            plt.savefig(path_to_pdf, format="pdf", bbox_inches="tight", pad_inches=1, transparent=True,
                        facecolor="w", edgecolor='w', orientation='landscape')
        else:
            path_to_pdf = path_to_pdf.replace('.pdf', '.png')
            plt.savefig(path_to_pdf, bbox_inches="tight", pad_inches=1,
                        transparent=True, facecolor="w", edgecolor='w', orientation='landscape')

def histogram_plot(x, data,      positive=True,  xlabel = '',        ylabel='',
                    weights=None,      smooth=True,       step=False,           color_map=False,
                    ls='-',            ylogscale=True,       xlogscale=False,
                    color='tab:blue',  figsize=1,            legend='_Hidden',
                    plot_title=None,   loc='upper right',    
                    add=None,          fig=None,             path_to_pdf=None,
                    pdf_format=True,      xmax=None,
                    linewidth=3.0,     fontsize=14):
    """ Function to generate a histogram figure based on the provided data.

    Args:
        data:                           The data for the histogram.
        weights (optional):             An array of weights for the data.       Default is None.
        frequency (bool, optional):     Whether to plot frequency.              Default is False.
        pdf (bool, optional):           Whether to plot the probability density function (PDF). Default is True.
        smooth (bool, optional):        Whether to plot a smooth line.          Default is True.
        step (bool, optional):          Whether to plot a step line.            Default is False.
        color_map (bool or str, optional): Whether to apply a color map to the histogram bars.
            If True, uses the 'viridis' color map. If a string, uses the specified color map. Default is False.
        ls (str, optional):             The line style for the plot.            Default is '-'.
        ylogscale (bool, optional):     Whether to use a logarithmic scale for the y-axis. Default is True.
        xlogscale (bool, optional):     Whether to use a logarithmic scale for the x-axis. Default is False.
        color (str, optional):          The color of the plot.                  Default is 'tab:blue'.
        figsize (float, optional):      The size of the figure.                 Default is 1.
        legend (str, optional):         The legend label for the plot.          Default is '_Hidden'.
        varname (str, optional):        The name of the variable for the x-axis label. Default is 'Precipitation'.
        plot_title (str, optional):     The title of the plot.                  Default is None.
        loc(str, optional):             The location of the legend.             Default to 'upper right'.
        add (tuple, optional):          Tuple of (fig, ax) to add the plot to an existing figure.
        fig (object, optional):         The figure object to plot on. If provided, ignores the 'add' argument.
        path_to_pdf (str, optional): The path to save the figure. If provided, saves the figure at the specified path.


    Returns:
        A tuple (fig, ax) containing the figure and axes objects.
    """
    if fig is not None:
        fig, ax = fig
    elif add is None and fig is None:
        fig, ax = plt.subplots(figsize=(8*figsize, 5*figsize))
    elif add is not None:
        fig, ax = add

    if positive:
        data = data.where(data > 0)
    if smooth:
        plt.plot(x, data,
                    linewidth=linewidth, ls=ls, color=color, label=legend)
        plt.grid(True)
    elif step:
        plt.step(x, data,
                    linewidth=3.0, ls=ls, color=color, label=legend)
        plt.grid(True)
    elif color_map:
        if weights is None:
            N, _, patches = plt.hist(
                x=x, bins=x, weights=data,    label=legend)
        else:
            N, bins, patches = plt.hist(
                x=x, bins=x, weights=weights, label=legend)

        fracs = ((N**(1 / 5)) / N.max())
        norm = colors.Normalize(fracs.min(), fracs.max())

        for thisfrac, thispatch in zip(fracs, patches):
            if color_map is True:
                color = plt.cm.get_cmap('viridis')(norm(thisfrac))
            elif isinstance(color_map, str):
                color = plt.cm.get_cmap(color_map)(norm(thisfrac))
            thispatch.set_facecolor(color)
    plt.xlabel(xlabel, fontsize=fontsize)
    if ylogscale:
        plt.yscale('log')
    if xlogscale:
        plt.xscale('log')

    plt.ylabel(ylabel,       fontsize=fontsize)
    plt.title(plot_title,       fontsize=16)

    if legend != '_Hidden':
        plt.legend(loc=loc,     fontsize=10)

    if xmax is not None:
        plt.xlim([0, xmax])
    if isinstance(path_to_pdf, str):
        savefig(path_to_pdf, pdf_format)
    return {fig, ax}

def plot_of_average(data=None, trop_lat=None, ylabel='', coord=None, fontsize=15, pad=15, y_lim_max=None,
                    legend='_Hidden', figsize=1, ls='-', maxticknum=12, color='tab:blue', ylogscale=False, 
                    xlogscale=False, loc='upper right', add=None, fig=None, plot_title=None, path_to_pdf=None, 
                    pdf_format=True):
    """ Function to plot the mean or median value of variable in Dataset.

    Args:
        data (xarray):                  The Dataset
        model_variable (str, optional): The variable of the Dataset.            Defaults to 'tprate'.
        coord (str, optional):          The coordinate of the Dataset.          Defaults to 'time'.
        trop_lat (float, optional):     The maximumal and minimal tropical latitude values in Dataset.  Defaults to None.
        get_mean (bool, optional):      The flag to calculate the mean of the variable.  Defaults to True.
        get_median (bool, optional):    The flag to calculate the median of the variable.  Defaults to False.
        s_time (str, optional):         The starting time of the Dataset.       Defaults to None.
        f_time (str, optional):         The ending time of the Dataset.         Defaults to None.
        s_year (str, optional):         The starting year of the Dataset.       Defaults to None.
        f_year (str, optional):         The ending year of the Dataset.         Defaults to None.
        s_month (str, optional):        The starting month of the Dataset.      Defaults to None.
        f_month (str, optional):        The ending month of the Dataset.        Defaults to None.
        legend (str, optional):         The legend of the plot.                 Defaults to '_Hidden'.
        figsize (int, optional):        The size of the plot.                   Defaults to 1.
        ls (str, optional):             The line style of the plot.             Defaults to '-'.
        maxticknum (int, optional):     The maximum number of ticks on the x-axis.  Defaults to 12.
        color (str, optional):          The color of the plot.                  Defaults to 'tab:blue'.
        varname (str, optional):        The name of the variable.               Defaults to 'Precipitation'.
        loc (str, optional):            The location of the legend.             Defaults to 'upper right'.
        add (matplotlib.figure.Figure, optional): The add previously created figure to plot.  Defaults to None.
        fig (matplotlib.figure.Figure, optional): The add previously created figure to plot.     Defaults to None.
        plot_title (str, optional):     The title of the plot.                  Defaults to None.
        path_to_pdf (str, optional):    The path to the pdf file.               Defaults to None.
        new_unit (str, optional):       The unit of the model variable.         Defaults to None.
        name_of_file (str, optional):   The name of the file.                   Defaults to None.
        seasons (bool, optional):       The flag to calculate the seasonal mean.  Defaults to True.
        pdf_format (bool, optional):    The flag to save the plot in pdf format. Defaults to True.
    Example:

    Returns:
        None.
    """


    # make a plot with different y-axis using second axis object
    labels_int = data[coord].values

    if 'Dataset' in str(type(data)):
        if fig is not None:
            ax1, ax2, ax3, ax4, ax5, ax_twin_5 = fig[1], fig[2], fig[3], fig[4], fig[5], fig[6]
            fig = fig[0]
            axs = [ax1, ax2, ax3, ax4, ax5]

        elif add is None and fig is None:
            fig = plt.figure(figsize=(11*figsize, 10*figsize), layout='constrained')
            gs = fig.add_gridspec(3, 2, height_ratios=[1, 1, 2.5])
            if coord == 'lon':
                ax1 = fig.add_subplot(gs[0, 0], projection=ccrs.PlateCarree())
                ax2 = fig.add_subplot(gs[0, 1], projection=ccrs.PlateCarree())
                ax3 = fig.add_subplot(gs[1, 0], projection=ccrs.PlateCarree())
                ax4 = fig.add_subplot(gs[1, 1], projection=ccrs.PlateCarree())
                ax5 = fig.add_subplot(gs[2, :], projection=ccrs.PlateCarree())
                ax_twin_5 = ax5.twinx()
            else:
                ax1 = fig.add_subplot(gs[0, 0])
                ax2 = fig.add_subplot(gs[0, 1])
                ax3 = fig.add_subplot(gs[1, 0])
                ax4 = fig.add_subplot(gs[1, 1])
                ax5 = fig.add_subplot(gs[2, :])
                ax_twin_5 = None
            axs = [ax1, ax2, ax3, ax4, ax5, ax_twin_5]
        elif add is not None:
            fig = add
            ax1, ax2, ax3, ax4, ax5, ax_twin_5 = add
            axs = [ax1, ax2, ax3, ax4, ax5]
        titles = ["DJF", "MAM", "JJA", "SON", "Yearly"]
        i = -1
        for one_season in [data.DJF, data.MAM, data.JJA, data.SON, data.Yearly]:
            i += 1
            axs[i].set_title(titles[i], fontsize=fontsize+1)
            # Latitude labels
            if coord == 'lon':
                axs[i].set_xlabel('Longitude',
                                    fontsize=12)
            elif coord == 'lat':
                axs[i].set_xlabel('Latitude',
                                    fontsize=12)

            if ylogscale:
                axs[i].set_yscale('log')
            if xlogscale:
                axs[i].set_xscale('log')

            if coord == 'lon':
                # twin object for two different y-axis on the sample plot
                ax_span = axs[i].twinx()
                ax_span.axhspan(-trop_lat, trop_lat, alpha=0.05, color='tab:red')
                ax_span.set_ylim([-90, 90])
                ax_span.set_xticks([])
                ax_span.set_yticks([])
                axs[i].coastlines(alpha=0.5, color='grey')
                axs[i].set_xticks(np.arange(-180, 181, 60), crs=ccrs.PlateCarree())
                axs[i].xaxis.set_major_formatter(cticker.LongitudeFormatter())

                # Latitude labels
                axs[i].set_yticks(np.arange(-90, 91, 30), crs=ccrs.PlateCarree())
                axs[i].yaxis.set_major_formatter(cticker.LatitudeFormatter())

                if i < 4:
                    ax_twin = axs[i].twinx()
                    ax_twin.set_frame_on(True)
                    ax_twin.plot(one_season.lon - 180, one_season, color=color, label=legend, ls=ls)
                    ax_twin.set_ylim([0, y_lim_max])
                    ax_twin.set_ylabel(ylabel, fontsize=fontsize-3)
                else:
                    ax_twin_5.set_frame_on(True)
                    ax_twin_5.plot(one_season.lon - 180, one_season, color=color,  label=legend, ls=ls)
                    ax_twin_5.set_ylim([0, y_lim_max])
                    ax_twin_5.set_ylabel(ylabel, fontsize=fontsize-3)
                    axs[i].set_xlabel('Longitude', fontsize=fontsize-3)

            else:
                axs[i].plot(one_season.lat, one_season, color=color, label=legend, ls=ls)
                axs[i].set_ylim([0, y_lim_max])
                axs[i].set_ylabel(ylabel, fontsize=fontsize-3)
                axs[i].set_xlabel('Latitude', fontsize=fontsize-3)

            axs[i].grid(True)
        if coord == 'lon':
            if legend != '_Hidden':
                ax_twin_5.legend(loc=loc, fontsize=fontsize-3, ncol=2)
            if plot_title is not None:
                plt.suptitle(plot_title, fontsize=fontsize+2)
        else:
            if legend != '_Hidden':
                ax5.legend(loc=loc, fontsize=fontsize-3, ncol=2)
            if plot_title is not None:
                plt.suptitle(plot_title, fontsize=fontsize+2)

    elif 'DataArray' in str(type(data)):
        if fig is not None:
            fig, ax = fig
        elif add is None and fig is None:
            fig, ax = plt.subplots(figsize=(8*figsize, 5*figsize))
        elif add is not None:
            fig, ax = add
        if data.size == 1:
            plt.axhline(y=float(data.values),
                        color=color,  label=legend,  ls=ls)
        else:
            if coord == 'time':
                plt.scatter(labels_int, data,
                            color=color,  label=legend,  ls=ls)
            else:
                plt.plot(labels_int,    data,
                            color=color,  label=legend,  ls=ls)

        plt.gca().xaxis.set_major_locator(plt.MaxNLocator(maxticknum))
        plt.gca().tick_params(axis='both',   which='major',    pad=10)
        plt.xlim([min(labels_int),    max(labels_int)])

        plt.grid(True)

        if coord == 'time':
            plt.xlabel('Timestep index',
                        fontsize=fontsize-3)
            if data['time.year'][0].values == data['time.year'][-1].values:
                plt.xlabel(
                    str(data['time.year'][0].values),    fontsize=fontsize-3)
            else:
                plt.xlabel(str(data['time.year'][0].values)+' - '+str(data['time.year'][-1].values),
                            fontsize=fontsize-3)
        elif coord == 'lat':
            plt.xlabel('Latitude', fontsize=fontsize-3)
        elif coord == 'lon':
            plt.xlabel('Longitude', fontsize=fontsize-3)
        plt.ylabel(ylabel, fontsize=fontsize-3)

        if plot_title is not None:
            plt.title(plot_title, fontsize=fontsize+2,    pad=15)

        if legend != '_Hidden':
            plt.legend(loc=loc,
                        fontsize=12,    ncol=2)
        if ylogscale:
            plt.yscale('log')
        if xlogscale:
            plt.xscale('log')

    if isinstance(path_to_pdf, str):
        savefig(path_to_pdf, pdf_format)

    if 'Dataset' in str(type(data)):
        return [fig,  ax1, ax2, ax3, ax4, ax5, ax_twin_5]
    else:
        return [fig,  ax]
    

def plot_seasons_or_months(data, cbarlabel=None, all_season=None, all_months=None,
                          figsize=1, plot_title=None,  vmin=None, vmax=None,
                          path_to_pdf=None, pdf_format=True):
    """ Function to plot seasonal data.

    Args:
        data (xarray): First dataset to be plotted.
        cbarlabel (str, optional): Label for the colorbar. Defaults to None.
        all_season (list, optional): List of seasonal datasets. Defaults to None.
        all_months (list, optional): List of monthly datasets. Defaults to None.
        figsize (int, optional): Size of the figure. Defaults to 1.
        plot_title (str, optional): Title of the plot. Defaults to None.
        vmin (float, optional): Minimum value of the colorbar. Defaults to None.
        vmax (float, optional): Maximum value of the colorbar. Defaults to None.
        path_to_pdf (str, optional): Path to save the PDF file. Defaults to None.
        pdf_format (bool, optional): If True, save the figure in PDF format. Defaults to True.
    """

    if all_months is None:
        fig = plt.figure(figsize=(11*figsize, 10*figsize),
                            layout='constrained')
        gs = fig.add_gridspec(3, 2)
        ax1 = fig.add_subplot(gs[0, 0], projection=ccrs.PlateCarree())
        ax2 = fig.add_subplot(gs[0, 1], projection=ccrs.PlateCarree())
        ax3 = fig.add_subplot(gs[1, 0], projection=ccrs.PlateCarree())
        ax4 = fig.add_subplot(gs[1, 1], projection=ccrs.PlateCarree())
        ax5 = fig.add_subplot(gs[2, :], projection=ccrs.PlateCarree())
        axs = [ax1, ax2, ax3, ax4, ax5]

        if vmin is None and vmax is None:
            vmax = float(all_season[0].max().values)/10
            vmin = 0
        clevs = np.arange(vmin, vmax, abs(vmax - vmin)/10)

        titles = ["DJF", "MAM", "JJA", "SON", "Yearly"]

        for i in range(0, len(all_season)):
            one_season = all_season[i]

            one_season = one_season.where(one_season > vmin)
            one_season, lons = add_cyclic_point(
                one_season, coord=data['lon'])

            im1 = axs[i].contourf(lons, data['lat'], one_season, clevs,
                                    transform=ccrs.PlateCarree(),
                                    cmap='coolwarm', extend='both')

            axs[i].set_title(titles[i], fontsize=17)

            axs[i].coastlines()

            # Longitude labels
            axs[i].set_xticks(np.arange(-180, 181, 60),
                                crs=ccrs.PlateCarree())
            lon_formatter = cticker.LongitudeFormatter()
            axs[i].xaxis.set_major_formatter(lon_formatter)

            # Latitude labels
            axs[i].set_yticks(np.arange(-90, 91, 30),
                                crs=ccrs.PlateCarree())
            lat_formatter = cticker.LatitudeFormatter()
            axs[i].yaxis.set_major_formatter(lat_formatter)
            axs[i].grid(True)

    else:
        fig, axes = plt.subplots(ncols=3, nrows=4, subplot_kw={'projection': ccrs.PlateCarree()},
                                    figsize=(11*figsize, 8.5*figsize), layout='constrained')
        

        if vmin is None and vmax is None:
            vmax = float(all_months[6].max().values)
            vmin = 0

        clevs = np.arange(vmin, vmax, (vmax - vmin)/10)

        for i in range(0, len(all_months)):
            all_months[i] = all_months[i].where(all_months[i] > vmin)
            all_months[i], lons = add_cyclic_point(
                all_months[i], coord=data['lon'])

        titles = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September',
                    'October', 'November', 'December']
        axs = axes.flatten()

        for i in range(0, len(all_months)):
            im1 = axs[i].contourf(lons, data['lat'], all_months[i], clevs,
                                    transform=ccrs.PlateCarree(),
                                    cmap='coolwarm', extend='both')

            axs[i].set_title(titles[i], fontsize=17)

            axs[i].coastlines()

            # Longitude labels
            axs[i].set_xticks(np.arange(-180, 181, 60), crs=ccrs.PlateCarree())
            lon_formatter = cticker.LongitudeFormatter()
            axs[i].xaxis.set_major_formatter(lon_formatter)

            # Latitude labels
            axs[i].set_yticks(np.arange(-90, 91, 30), crs=ccrs.PlateCarree())
            lat_formatter = cticker.LatitudeFormatter()
            axs[i].yaxis.set_major_formatter(lat_formatter)
            axs[i].grid(True)
    # Draw the colorbar
    cbar = fig.colorbar(
        im1, ticks=[-7, -5, -3, -1, 1, 3, 5, 7], ax=ax5, location='bottom')
    cbar.set_label(cbarlabel, fontsize=14)

    if plot_title is not None:
        plt.suptitle(plot_title,                       fontsize=17)

    if isinstance(path_to_pdf, str):
        savefig(path_to_pdf, pdf_format)


def ticks_for_colorbar(data, vmin=None, vmax=None, model_variable='tprate', number_of_ticks=6):
    """Compute ticks and levels for a color bar based on provided data.

    Args:
        data: The data from which to compute the color bar.
        vmin: The minimum value of the color bar. If None, it is derived from the data.
        vmax: The maximum value of the color bar. If None, it is derived from the data.
        model_variable: The variable to consider for the color bar computation.
        number_of_ticks: The number of ticks to be computed for the color bar.

    Returns:
        Tuple: A tuple containing the computed ticks and levels for the color bar.

    Raises:
        ZeroDivisionError: If a division by zero occurs during computation.
    """
    if vmin is None and vmax is None:
        try:
            vmax = float(data[model_variable].max().values) / 10
        except KeyError:
            vmax = float(data.max().values) / 10
        vmin = -vmax
        ticks = [vmin + i * (vmax - vmin) / number_of_ticks for i in range(number_of_ticks + 1)]
    elif isinstance(vmax, int) and isinstance(vmin, int):
        ticks = list(range(vmin, vmax + 1))
    elif isinstance(vmax, float) or isinstance(vmin, float):
        ticks = [vmin + i * (vmax - vmin) / number_of_ticks for i in range(number_of_ticks + 1)]

    try:
        del_tick = abs(vmax - 2 - vmin) / (number_of_ticks + 1)
    except ZeroDivisionError:
        del_tick = abs(vmax - 2.01 - vmin) / (number_of_ticks + 1)
    clevs = np.arange(vmin, vmax, del_tick)

    return ticks, clevs


def map(data, titles=None, lonmin=-180, lonmax=181, latmin=-90, latmax=91,
        model_variable='tprate', figsize=1, number_of_ticks=6, cbarlabel='',
        plot_title=None, vmin=None, vmax=None, path_to_pdf=None, pdf_format=True):
    """
    Create a map with specified data and various optional parameters.

    Args:
        data (xarray): First dataset to be plotted.
        cbarlabel (str, optional): Label for the colorbar. Defaults to None.
        all_season (list, optional): List of seasonal datasets. Defaults to None.
        all_season_2 (list, optional): List of secondary seasonal datasets. Defaults to None.
        all_months (list, optional): List of monthly datasets. Defaults to None.
        all_months_2 (list, optional): List of secondary monthly datasets. Defaults to None.
        figsize (int, optional): Size of the figure. Defaults to 1.
        plot_title (str, optional): Title of the plot. Defaults to None.
        vmin (float, optional): Minimum value of the colorbar. Defaults to None.
        vmax (float, optional): Maximum value of the colorbar. Defaults to None.
        path_to_pdf (str, optional): Path to save the PDF file. Defaults to None.
        pdf_format (bool, optional): If True, save the figure in PDF format. Defaults to True.

    Returns:
        The pyplot figure in the PDF format
    """
    data_len = len(data)

    if titles is None:
        titles = [""] * data_len
    elif isinstance(titles, str) and data_len != 1 or len(titles) != data_len:
        raise KeyError("The length of plot titles must be the same as the number of provided data to plot.")
    
    if data_len == 1:
        ncols, nrows = 1, 1
    elif data_len % 2 == 0:
        ncols, nrows = 2, data_len // 2
    elif data_len % 3 == 0:
        ncols, nrows = 3, data_len // 3

    fig = plt.figure(figsize=(11*figsize*ncols, 8.5*figsize*nrows)) #, layout='constrained')
    gs = GridSpec(nrows=nrows, ncols=ncols + 1, figure=fig, wspace=0.2, hspace=0.2, width_ratios=[1] * ncols + [0.1], height_ratios=[1] * nrows) 
    # Add subplots using the grid
    axs =  [fig.add_subplot(gs[i, j], projection=ccrs.PlateCarree()) for i in range(nrows) for j in range(ncols)]

    ticks, clevs = ticks_for_colorbar(data, vmin=vmin, vmax=vmax, model_variable=model_variable, number_of_ticks=number_of_ticks)    

    for i in range(0, len(data)):   
        data_cycl, lons = add_cyclic_point(
            data[i], coord=data[i]['lon'])
        im1 = axs[i].contourf(lons, data[i]['lat'], data_cycl, clevs,
                            transform=ccrs.PlateCarree(),
                            cmap='coolwarm', extend='both')

        axs[i].set_title(titles[i], fontsize=17)
        axs[i].coastlines()
        # Longitude labels
        axs[i].set_xticks(np.arange(lonmin, lonmax, int(lonmax-lonmin)/number_of_ticks), crs=ccrs.PlateCarree())
        axs[i].xaxis.set_major_formatter(cticker.LongitudeFormatter())           
        # Latitude labels
        axs[i].set_yticks(np.arange(latmin, latmax, int(latmax-latmin)/number_of_ticks), crs=ccrs.PlateCarree())
        axs[i].yaxis.set_major_formatter(cticker.LatitudeFormatter())
        axs[i].grid(True)

    # Draw the colorbar
    cbar_ax = fig.add_subplot(gs[:, -1]) # Adjust the column index as needed
    cbar = fig.colorbar(im1, cax=cbar_ax, ticks=ticks, orientation='vertical', extend='both') #, shrink=0.8, pad=0.05, aspect=30)
    cbar.set_label(cbarlabel, fontsize=14)
    if plot_title is not None:
        plt.suptitle(plot_title,                       fontsize=17)

    if isinstance(path_to_pdf, str):
        #path_to_pdf = path_to_pdf + 'trop_rainfall_' + name_of_file + '_map.pdf'
        savefig(path_to_pdf, pdf_format)
