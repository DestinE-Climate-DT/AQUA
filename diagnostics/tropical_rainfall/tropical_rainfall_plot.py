import matplotlib.pyplot as plt
import matplotlib.colors as colors
# import boost_histogram as bh  # pip
from matplotlib.gridspec import GridSpec
import seaborn as sns

from aqua.util import create_folder
from aqua.logger import log_configure
from .tropical_rainfall_func import ToolsClass

import cartopy.crs as ccrs
import cartopy.mpl.ticker as cticker
from cartopy.util import add_cyclic_point

from matplotlib.ticker import StrMethodFormatter
#import matplotlib.colors as mcolors

import numpy as np
import xarray as xr

class PlottingClass:
    """This is class to create the plots."""
        
    def __init__(self, path_to_pdf=None, pdf_format=True, figsize=1, linewidth=3,
                 fontsize=14, smooth=True, step=False, color_map=False, cmap='coolwarm', #pdf=True,
                 ls='-', ylogscale=True, xlogscale=False, model_variable='tprate', number_of_axe_ticks=5, number_of_bar_ticks=6, loglevel: str = 'WARNING'):
        self.path_to_pdf = path_to_pdf
        self.pdf_format = pdf_format
        self.figsize = figsize
        self.fontsize = fontsize
        #self.pdf = pdf
        self.smooth = smooth
        self.step = step
        self.color_map = color_map
        self.cmap = cmap
        self.ls = ls
        self.linewidth = linewidth
        self.ylogscale = ylogscale
        self.xlogscale = xlogscale
        self.model_variable = model_variable
        self.number_of_axe_ticks = number_of_axe_ticks
        self.number_of_bar_ticks = number_of_bar_ticks
        self.loglevel = loglevel
        self.logger = log_configure(self.loglevel, 'Plot. Func.')
        self.tools = ToolsClass()
    
    def class_attributes_update(self, path_to_pdf=None, pdf_format=None, figsize=None, linewidth=None,
                 fontsize=None, smooth=None, step=None, color_map=None, cmap=None, #pdf=None,
                 ls=None, ylogscale=None, xlogscale=None, model_variable=None, number_of_axe_ticks=None, number_of_bar_ticks=None):
        """
        Update the class attributes based on the provided arguments.

        Args:
            path_to_pdf (str): The file path for saving the figure in PDF format. If None, the previous value will be retained.
            pdf_format (bool): A flag indicating whether the figure should be saved in PDF format. If None, the previous value will be retained.
            figsize (float): The size of the figure. If None, the previous value will be retained.
            fontsize (int): The font size of the text in the figure. If None, the previous value will be retained.
            pdf (bool): A flag indicating whether to save the figure in PDF format. If None, the previous value will be retained.
            smooth (bool): A flag indicating whether to smooth the figure. If None, the previous value will be retained.
            step (bool): A flag indicating whether to use step plotting. If None, the previous value will be retained.
            color_map (bool): A flag indicating whether to use a color map. If None, the previous value will be retained.
            ls (str): The linestyle for the figure. If None, the previous value will be retained.
            ylogscale (bool): A flag indicating whether to use a logarithmic scale for the y-axis. If None, the previous value will be retained.
            xlogscale (bool): A flag indicating whether to use a logarithmic scale for the x-axis. If None, the previous value will be retained.
            model_variable (str): The model variable to be used. If None, the previous value will be retained.
            number_of_bar_ticks (int): The number of ticks for bar plots. If None, the previous value will be retained.

        Returns:
            None
        """
        self.path_to_pdf = self.path_to_pdf if path_to_pdf is None else path_to_pdf
        self.pdf_format = self.pdf_format if pdf_format is None else pdf_format
        self.figsize = self.figsize if figsize is None else figsize
        self.fontsize = self.fontsize if fontsize is None else fontsize
        #self.pdf = self.pdf if pdf is None else pdf
        self.smooth = self.smooth if smooth is None else smooth
        self.step = self.step if step is None else step
        self.color_map = self.color_map if color_map is None else color_map
        self.cmap = self.cmap if cmap is None else cmap
        self.ls = self.ls if ls is None else ls
        self.linewidth = self.linewidth if linewidth is None else linewidth
        self.ylogscale = self.ylogscale if ylogscale is None else ylogscale
        self.xlogscale = self.xlogscale if xlogscale is None else xlogscale
        self.model_variable = self.model_variable if model_variable is None else model_variable
        self.number_of_axe_ticks = self.number_of_axe_ticks if number_of_axe_ticks is None else number_of_axe_ticks
        self.number_of_bar_ticks = self.number_of_bar_ticks if number_of_bar_ticks is None else number_of_bar_ticks
        

    def savefig(self, path_to_pdf=None, pdf_format=None):
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
            self.class_attributes_update(path_to_pdf=path_to_pdf, pdf_format=pdf_format)

            create_folder(folder=self.tools.extract_directory_path(
                        path_to_pdf), loglevel='WARNING')
            
            if pdf_format:
                plt.savefig(path_to_pdf, format="pdf", bbox_inches="tight", pad_inches=1, transparent=True,
                            facecolor="w", edgecolor='w', orientation='landscape')
            else:
                path_to_pdf = path_to_pdf.replace('.pdf', '.png')
                plt.savefig(path_to_pdf, bbox_inches="tight", pad_inches=1,
                            transparent=True, facecolor="w", edgecolor='w', orientation='landscape')

    def histogram_plot(self, x, data, positive=True, xlabel='', ylabel='',
                   weights=None, smooth=None, step=None, color_map=None,
                   ls=None, ylogscale=None, xlogscale=None,
                   color='tab:blue', figsize=None, legend='_Hidden',
                   plot_title=None, loc='upper right',
                   add=None, fig=None, path_to_pdf=None,
                   pdf_format=None, xmax=None,
                   linewidth=None, fontsize=None):
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
        self.class_attributes_update(path_to_pdf=path_to_pdf, pdf_format=pdf_format, color_map=color_map, xlogscale=xlogscale, 
                                ylogscale=ylogscale, figsize=figsize, fontsize=fontsize, smooth=smooth, step=step, ls=ls, linewidth=linewidth)
        self.logger.warning("TEST")
        if fig is not None:
            fig, ax = fig
        elif add is None and fig is None:
            fig, ax = plt.subplots(figsize=(8*self.figsize, 5*self.figsize))
        elif add is not None:
            fig, ax = add

        if positive:
            data = data.where(data > 0)
        if self.smooth:
            plt.plot(x, data,
                        linewidth=self.linewidth, ls=self.ls, color=color, label=legend)
            plt.grid(True)
        elif self.step:
            plt.step(x, data,
                        linewidth=self.linewidth, ls=self.ls, color=color, label=legend)
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
        plt.xlabel(xlabel, fontsize=self.fontsize)
        if self.ylogscale:
            plt.yscale('log')
        if self.xlogscale:
            plt.xscale('log')

        plt.ylabel(ylabel, fontsize=self.fontsize)
        plt.title(plot_title, fontsize=self.fontsize+2)

        if legend != '_Hidden':
            plt.legend(loc=loc, fontsize=self.fontsize-4)

        if xmax is not None:
            plt.xlim([0, xmax])
        if isinstance(self.path_to_pdf, str):
            self.savefig(self.path_to_pdf, self.pdf_format)
        return {fig, ax}

    def plot_of_average(self, data=None, trop_lat=None, ylabel='', coord=None, fontsize=None, pad=15, y_lim_max=None,
                        legend='_Hidden', figsize=None, ls=None, maxticknum=12, color='tab:blue', ylogscale=None, 
                        xlogscale=None, loc='upper right', add=None, fig=None, plot_title=None, path_to_pdf=None, 
                        pdf_format=None):
        """
        Make a plot with different y-axes using a second axis object.

        Args:
            data (list or DataArray): Data to plot.
            trop_lat (float): Tropospheric latitude. Defaults to None.
            ylabel (str): Label for the y-axis. Defaults to ''.
            coord (str): Coordinate for the plot. Can be 'lon', 'lat', or 'time'. Defaults to None.
            fontsize (int): Font size for the plot. Defaults to 15.
            pad (int): Padding value. Defaults to 15.
            y_lim_max (float): Maximum limit for the y-axis. Defaults to None.
            legend (str): Legend for the plot. Defaults to '_Hidden'.
            figsize (int): Figure size. Defaults to 1.
            ls (str): Line style for the plot. Defaults to '-'.
            maxticknum (int): Maximum number of ticks. Defaults to 12.
            color (str): Color for the plot. Defaults to 'tab:blue'.
            ylogscale (bool): Use logarithmic scale for the y-axis. Defaults to False.
            xlogscale (bool): Use logarithmic scale for the x-axis. Defaults to False.
            loc (str): Location for the legend. Defaults to 'upper right'.
            add (list): Additional objects to add. Defaults to None.
            fig (list): Figure objects. Defaults to None.
            plot_title (str): Title for the plot. Defaults to None.
            path_to_pdf (str): Path to save the figure as a PDF. Defaults to None.
            pdf_format (bool): Save the figure in PDF format. Defaults to True.

        Returns:
            list: List of figure and axis objects.
        """
        self.class_attributes_update(path_to_pdf=path_to_pdf, pdf_format=pdf_format, color_map=color_map, xlogscale=xlogscale, 
                                ylogscale=ylogscale, figsize=figsize, fontsize=fontsize, smooth=smooth, step=step, ls=ls)

        # make a plot with different y-axis using second axis object
        labels_int = data[coord].values

        if 'Dataset' in str(type(data)):
            if fig is not None:
                ax1, ax2, ax3, ax4, ax5, ax_twin_5 = fig[1], fig[2], fig[3], fig[4], fig[5], fig[6]
                fig = fig[0]
                axs = [ax1, ax2, ax3, ax4, ax5]

            elif add is None and fig is None:
                fig = plt.figure(figsize=(11*self.figsize, 10*self.figsize), layout='constrained')
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
                axs[i].set_title(titles[i], fontsize=self.fontsize+1)
                # Latitude labels
                if coord == 'lon':
                    axs[i].set_xlabel('Longitude',
                                        fontsize=self.fontsize-2)
                elif coord == 'lat':
                    axs[i].set_xlabel('Latitude',
                                        fontsize=self.fontsize-2)

                if self.ylogscale:
                    axs[i].set_yscale('log')
                if self.xlogscale:
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
                plt.axhline(y=float(data.values), color=color, label=legend, ls=ls)
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
                plt.title(plot_title, fontsize=fontsize+2, pad=15)

            if legend != '_Hidden':
                plt.legend(loc=loc, fontsize=fontsize-2, ncol=2)

            plt.yscale('log') if ylogscale else None
            plt.xscale('log') if xlogscale else None

        if isinstance(path_to_pdf, str):
            self.savefig(path_to_pdf, pdf_format)

        if 'Dataset' in str(type(data)):
            return [fig,  ax1, ax2, ax3, ax4, ax5, ax_twin_5]
        else:
            return [fig,  ax]
        

    def plot_seasons_or_months(self, data, cbarlabel=None, all_season=None, all_months=None, cmap='coolwarm',
                            figsize=None, plot_title=None,  vmin=None, vmax=None,
                            path_to_pdf=None, pdf_format=None):
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
        self.class_attributes_update(path_to_pdf=path_to_pdf, pdf_format=pdf_format, color_map=color_map, xlogscale=xlogscale, 
                                ylogscale=ylogscale, figsize=figsize, fontsize=fontsize, smooth=smooth, step=step, ls=ls)

        clevs = self.ticks_for_colorbar(data, vmin=vmin, vmax=vmax, model_variable=model_variable, number_of_bar_ticks=number_of_bar_ticks)

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

            titles = ["DJF", "MAM", "JJA", "SON", "Yearly"]

            for i in range(0, len(all_season)):
                one_season = all_season[i]

                one_season = one_season.where(one_season > vmin)
                one_season, lons = add_cyclic_point(
                    one_season, coord=data['lon'])

                im1 = axs[i].contourf(lons, data['lat'], one_season, clevs,
                                        transform=ccrs.PlateCarree(),
                                        cmap='coolwarm', extend='both')

                axs[i].set_title(titles[i], fontsize=fontsize+3)

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
                                        cmap=cmap, extend='both')

                axs[i].set_title(titles[i], fontsize=fontsize+3)

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
            im1, ticks=clevs, ax=ax5, location='bottom') #[-7, -5, -3, -1, 1, 3, 5, 7]
        cbar.set_label(cbarlabel, fontsize=fontsize)

        if plot_title is not None:
            plt.suptitle(plot_title,                       fontsize=fontsize+3)

        if isinstance(path_to_pdf, str):
            self.savefig(path_to_pdf, pdf_format)


    def ticks_for_colorbar(self, data, vmin=None, vmax=None, model_variable=None, number_of_bar_ticks=None):
        """Compute ticks and levels for a color bar based on provided data.

        Args:
            data: The data from which to compute the color bar.
            vmin: The minimum value of the color bar. If None, it is derived from the data.
            vmax: The maximum value of the color bar. If None, it is derived from the data.
            model_variable: The variable to consider for the color bar computation.
            number_of_bar_ticks: The number of ticks to be computed for the color bar.

        Returns:
            Tuple: A tuple containing the computed ticks and levels for the color bar.

        Raises:
            ZeroDivisionError: If a division by zero occurs during computation.
        """
        self.class_attributes_update(model_variable=model_variable, number_of_bar_ticks=number_of_bar_ticks)

        if vmin is None and vmax is None:
            try:
                vmax = float(data[self.model_variable].max().values) / 10
            except KeyError:
                vmax = float(data.max().values) / 10
            vmin = -vmax
            clevs = [vmin + i * (vmax - vmin) / self.number_of_bar_ticks for i in range(self.number_of_bar_ticks + 1)]
        elif isinstance(vmax, int) and isinstance(vmin, int):
            clevs = list(range(vmin, vmax + 1))
        elif isinstance(vmax, float) or isinstance(vmin, float):
            clevs = [vmin + i * (vmax - vmin) / self.number_of_bar_ticks for i in range(self.number_of_bar_ticks + 1)]
        #try:
        #    del_tick = abs(vmax - 2 - vmin) / (self.number_of_bar_ticks + 1)
        #except ZeroDivisionError:
        #    del_tick = abs(vmax - 2.01 - vmin) / (self.number_of_bar_ticks + 1)
        #clevs = ticks# np.arange(vmin, vmax, del_tick)
        #self.logger.debug('Ticks: {}'.format(ticks))
        self.logger.debug('Clevs: {}'.format(clevs))
        return clevs #ticks#, 


    def map(self, data, titles=None, lonmin=-180, lonmax=181, latmin=-90, latmax=91, cmap=None,
            model_variable=None, figsize=None,  number_of_axe_ticks=None, number_of_bar_ticks=None, cbarlabel='',
            plot_title=None, vmin=None, vmax=None, path_to_pdf=None, pdf_format=None,
            fontsize=None):
        """
        Generate a map with subplots for provided data.

        Args:
            data (list): List of data to plot.
            titles (list or str, optional): Titles for the subplots. If str, it will be repeated for each subplot. Defaults to None.
            lonmin (int, optional): Minimum longitude. Defaults to -180.
            lonmax (int, optional): Maximum longitude. Defaults to 181.
            latmin (int, optional): Minimum latitude. Defaults to -90.
            latmax (int, optional): Maximum latitude. Defaults to 91.
            model_variable (str, optional): Model variable for the plot. Defaults to 'tprate'.
            figsize (float, optional): Figure size. Defaults to 1.
            number_of_bar_ticks (int, optional): Number of ticks. Defaults to 6.
            cbarlabel (str, optional): Colorbar label. Defaults to ''.
            plot_title (str, optional): Plot title. Defaults to None.
            vmin (float, optional): Minimum value for the colorbar. Defaults to None.
            vmax (float, optional): Maximum value for the colorbar. Defaults to None.
            path_to_pdf (str, optional): Path to save the figure as a PDF. Defaults to None.
            pdf_format (bool, optional): Save the figure in PDF format. Defaults to True.
            fontsize (int, optional): Base font size for the plot. Defaults to 14.

        Returns:
            The pyplot figure in the PDF format.
        """
        self.class_attributes_update(path_to_pdf=path_to_pdf, pdf_format=pdf_format, figsize=figsize, fontsize=fontsize, 
                                model_variable=model_variable, number_of_axe_ticks=number_of_axe_ticks, number_of_bar_ticks=number_of_bar_ticks)                         
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

        horizontal_size = 10*abs(lonmax-lonmin)*ncols*self.figsize/360
        vertical_size = 8*abs(latmax-latmin)*nrows*self.figsize/180

        if horizontal_size < 8 or vertical_size < 4:
            figsize = 4 if horizontal_size < 4 or vertical_size < 2 else 2
        else:
            figsize = 1
        self.logger.debug('Size of the plot before auto re-scaling: {}, {}'.format(horizontal_size, vertical_size))
        self.logger.debug('Size of the plot after auto re-scaling: {}, {}'.format(horizontal_size*figsize, vertical_size*figsize))

        fig = plt.figure(figsize=(horizontal_size*figsize, vertical_size*figsize))
        gs = GridSpec(nrows=nrows, ncols=ncols, figure=fig, wspace=0.175, hspace=0.175, width_ratios=[1] * ncols, height_ratios=[1] * nrows)  
        # Add subplots using the grid
        axs =  [fig.add_subplot(gs[i, j], projection=ccrs.PlateCarree()) for i in range(nrows) for j in range(ncols)]
        clevs = self.ticks_for_colorbar(data, vmin=vmin, vmax=vmax, 
                                               model_variable=self.model_variable, number_of_bar_ticks=self.number_of_bar_ticks)

        if not isinstance(self.cmap, list):
            self.class_attributes_update(cmap=cmap)
            cmap = [self.cmap for _ in range(data_len)]

        for i in range(0, data_len):   
            data_cycl, lons = add_cyclic_point(data[i], coord=data[i]['lon'])
            im1 = axs[i].contourf(lons, data[i]['lat'], data_cycl, clevs, transform=ccrs.PlateCarree(),
                                  cmap=cmap[i],  extend='both')
            axs[i].set_title(titles[i], fontsize=self.fontsize+3)
            axs[i].coastlines()
            # Longitude labels
            axs[i].set_xticks(np.arange(lonmin, lonmax, int(lonmax-lonmin)/self.number_of_axe_ticks), crs=ccrs.PlateCarree())
            axs[i].xaxis.set_major_formatter(cticker.LongitudeFormatter())  
            # Longitude labels
            lon_formatter = StrMethodFormatter('{x:.1f}')  # Adjust the precision as needed
            axs[i].xaxis.set_major_formatter(lon_formatter) 
            axs[i].tick_params(axis='x', which='major', labelsize=self.fontsize-3) 

            # Latitude labels
            axs[i].set_yticks(np.arange(latmin, latmax, int(latmax-latmin)/self.number_of_axe_ticks), crs=ccrs.PlateCarree())
            axs[i].yaxis.set_major_formatter(cticker.LatitudeFormatter())
            # Latitude labels
            lat_formatter = StrMethodFormatter('{x:.1f}')  # Adjust the precision as needed
            axs[i].yaxis.set_major_formatter(lat_formatter)
            axs[i].tick_params(axis='y', which='major', labelsize=self.fontsize-3)     

            axs[i].grid(True)
        [axs[-1*i].set_xlabel('Longitude', fontsize=self.fontsize) for i in range(1, ncols+1)]
        [axs[ ncols*i].set_ylabel('Latitude', fontsize=self.fontsize) for i in range(0, nrows)]
        # Draw the colorbar
        fig.subplots_adjust(bottom=0.25, top=0.9, left=0.05, right=0.95,
                         wspace=0.2, hspace=0.5) 
        cbar_ax = fig.add_axes([0.2, 0.15, 0.6, 0.02])
                
        cbar = fig.colorbar(im1, cax=cbar_ax, ticks=clevs, orientation='horizontal', extend='both')
        cbar.set_label(cbarlabel, fontsize=self.fontsize)

        if plot_title is not None:
            plt.suptitle(plot_title, fontsize=self.fontsize+3)

        if isinstance(self.path_to_pdf, str):
            self.savefig(self.path_to_pdf, self.pdf_format)

    """
    def daily_variability_plot(self, ymax=12, trop_lat=None, relative=True, get_median=False,
                            legend='_Hidden', figsize=self.figsize, ls='-', maxticknum=12, color='tab:blue',
                            varname='tprate', ylogscale=False, xlogscale=False, loc='upper right',
                            add=None, fig=None, plot_title=None, path_to_pdf=None, new_unit='mm/day',
                            name_of_file=None, pdf_format=True, path_to_netcdf=None):

        Plot the daily variability of the dataset.

        This function generates a plot showing the daily variability of the provided dataset. It allows customization of various plot parameters such as color, scale, and legends.

        Args:
            ymax (int): The maximum y-value for the plot.
            trop_lat (float): The tropical latitude value to be used.
            relative (bool): A flag indicating whether the plot should be relative.
            get_median (bool): A flag indicating whether to calculate the median.
            legend (str): The legend for the plot.
            figsize (int): The size of the figure.
            ls (str): The linestyle for the plot.
            maxticknum (int): The maximum number of ticks for the plot.
            color (str): The color of the plot.
            varname (str): The variable name to be used.
            ylogscale (bool): A flag indicating whether to use a log scale for the y-axis.
            xlogscale (bool): A flag indicating whether to use a log scale for the x-axis.
            loc (str): The location for the legend.
            add: Additional parameters for the plot.
            fig: The figure to be used for the plot.
            plot_title (str): The title for the plot.
            path_to_pdf (str): The path to the PDF file to be saved.
            new_unit (str): The new unit to which the data should be converted.
            name_of_file (str): The name of the file to be saved.
            pdf_format (bool): A flag indicating whether the file should be saved in PDF format.
            path_to_netcdf (str): The path to the NetCDF file to be used.

        Returns:
            list: A list containing the figure and axis objects.



        self.class_attributes_update(trop_lat=trop_lat)
        if path_to_netcdf is None:
            raise Exception('The path needs to be provided')
        else:
            data = self.open_dataset(
                path_to_netcdf=path_to_netcdf)

        utc_time = data['utc_time']
        if relative:
            tprate = data['tprate_relative']
        else:
            tprate = data['tprate']
        try:
            units = data.units
        except AttributeError:
            try:
                units = data.tprate.units
            except AttributeError:
                units = 'mm/day'  # 'kg m**-2 s**-1'

        if 'Dataset' in str(type(data)):
            y_lim_max = self.precipitation_rate_units_converter(
                ymax, old_unit='mm/day', new_unit=new_unit)
            if fig is not None:
                fig, ax = fig
            elif add is None and fig is None:
                fig, ax = plt.subplots(
                    figsize=(11*figsize, 10*figsize), layout='constrained')
            elif add is not None:
                fig, ax = add
        ax.plot(utc_time, tprate,
                color=color,  label=legend,  ls=ls)

        if relative:
            ax.set_title(
                'Relative Value of Daily Precipitation Variability', fontsize=15)
            ax.set_xlabel('tprate variability, '+units,  fontsize=12)
        else:
            ax.set_title('Daily Precipitation Variability', fontsize=15)
            ax.set_xlabel('relative tprate',  fontsize=12)

        ax.set_frame_on(True)
        ax.grid(True)

        ax.set_xlabel('Local time', fontsize=12)

        if legend != '_Hidden':
            plt.legend(loc=loc,
                       fontsize=12,    ncol=2)

        if isinstance(path_to_pdf, str) and name_of_file is not None:
            path_to_pdf = path_to_pdf + 'trop_rainfall_' + name_of_file + '_dailyvar.pdf'
            self.savefig(path_to_pdf, pdf_format)

        return [fig,  ax]
    """