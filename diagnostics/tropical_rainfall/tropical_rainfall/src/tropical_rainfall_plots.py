import matplotlib.pyplot as plt
import matplotlib.colors as colors
from matplotlib.gridspec import GridSpec
from typing import Union, Tuple, Optional, List  # Any

from aqua.util import create_folder
from aqua.logger import log_configure
from .tropical_rainfall_tools import ToolsClass

import cartopy.crs as ccrs
import cartopy.mpl.ticker as cticker
from cartopy.util import add_cyclic_point

from matplotlib.ticker import StrMethodFormatter

import numpy as np
import xarray as xr


class PlottingClass:
    """This is class to create the plots."""

    def __init__(self, pdf_format: bool = None, figsize: float = None,
                 linewidth: float = None, fontsize: int = None, smooth: bool = None,
                 step: bool = None, color_map: bool = None, cmap: str = None,
                 linestyle: str = None, ylogscale: bool = None, xlogscale: bool = None,
                 model_variable: str = None, number_of_axe_ticks: int = None,
                 number_of_bar_ticks: int = None, loglevel: str = 'WARNING'):
        """
        Constructor for the plotting class, initializing various plotting parameters.

        Args:
            pdf_format (bool): A flag indicating whether the output format should be PDF.
            figsize (int): The size of the figure.
            linewidth (int): The width of the line in the plot.
            fontsize (int): The size of the font for the plot.
            smooth (bool): A flag indicating whether the plot should be smoothed.
            step (bool): A flag indicating whether the plot should be stepped.
            color_map (bool): A flag indicating whether a color map should be used.
            cmap (str): The name of the color map to use.
            linestyle (str): The style of the line in the plot.
            ylogscale (bool): A flag indicating whether the y-axis should be displayed in log scale.
            xlogscale (bool): A flag indicating whether the x-axis should be displayed in log scale.
            model_variable (str): The variable name to be used for the plot.
            number_of_axe_ticks (int): The number of ticks to display on the axes.
            number_of_bar_ticks (int): The number of ticks to display on the bar.
            loglevel (str): The level of logging to be used.

        """
        self.pdf_format = pdf_format
        self.figsize = figsize
        self.fontsize = fontsize
        self.smooth = smooth
        self.step = step
        self.color_map = color_map
        self.cmap = cmap
        self.linestyle = linestyle
        self.linewidth = linewidth
        self.ylogscale = ylogscale
        self.xlogscale = xlogscale
        self.model_variable = model_variable
        self.number_of_axe_ticks = number_of_axe_ticks
        self.number_of_bar_ticks = number_of_bar_ticks
        self.loglevel = loglevel
        self.logger = log_configure(self.loglevel, 'Plot. Func.')
        self.tools = ToolsClass(self.loglevel)

    def class_attributes_update(self, pdf_format: Optional[bool] = None, figsize: Optional[float] = None,
                                linewidth: Optional[float] = None, fontsize: Optional[int] = None,
                                smooth: Optional[bool] = None, step: Optional[bool] = None, color_map: Optional[bool] = None,
                                cmap: Optional[str] = None, linestyle: Optional[str] = None, ylogscale: Optional[bool] = None,
                                xlogscale: Optional[bool] = None, model_variable: Optional[str] = None,
                                number_of_axe_ticks: Optional[int] = None, number_of_bar_ticks: Optional[int] = None):
        """
        Update the class attributes based on the provided arguments.

        Args:
            pdf_format (bool, optional): A flag indicating whether the figure should be saved in PDF format.
            figsize (float, optional): The size of the figure.
            linewidth (float, optional): The width of the lines in the plot.
            fontsize (int, optional): The font size of the text in the figure.
            smooth (bool, optional): A flag indicating whether to smooth the figure.
            step (bool, optional): A flag indicating whether to use step plotting.
            color_map (bool, optional): A flag indicating whether to use a color map.
            cmap (str, optional): The color map to be used.
            linestyle (str, optional): The linestyle for the figure.
            ylogscale (bool, optional): A flag indicating whether to use a logarithmic scale for the y-axis.
            xlogscale (bool, optional): A flag indicating whether to use a logarithmic scale for the x-axis.
            model_variable (str, optional): The model variable to be used.
            number_of_bar_ticks (int, optional): The number of ticks for bar plots.

        Returns:
            None
        """
        self.pdf_format = self.pdf_format if pdf_format is None else pdf_format
        self.figsize = self.figsize if figsize is None else figsize
        self.fontsize = self.fontsize if fontsize is None else fontsize
        self.smooth = self.smooth if smooth is None else smooth
        self.step = self.step if step is None else step
        self.color_map = self.color_map if color_map is None else color_map
        self.cmap = self.cmap if cmap is None else cmap
        self.linestyle = self.linestyle if linestyle is None else linestyle
        self.linewidth = self.linewidth if linewidth is None else linewidth
        self.ylogscale = self.ylogscale if ylogscale is None else ylogscale
        self.xlogscale = self.xlogscale if xlogscale is None else xlogscale
        self.model_variable = self.model_variable if model_variable is None else model_variable
        self.number_of_axe_ticks = self.number_of_axe_ticks if number_of_axe_ticks is None else number_of_axe_ticks
        self.number_of_bar_ticks = self.number_of_bar_ticks if number_of_bar_ticks is None else number_of_bar_ticks

    def savefig(self, path_to_pdf: Optional[str] = None, pdf_format: Optional[bool] = None):
        """
        Save the current figure to a file in either PDF or PNG format.

        Args:
            path_to_pdf (str, optional): The file path where the figure will be saved. If None, the figure will not be saved.
            pdf_format (bool, optional): If True, the figure will be saved in PDF format; otherwise,
                                         it will be saved in PNG format.

        Returns:
            None

        Note:
            The function first checks the `path_to_pdf` to determine the format of the saved figure.
            If `pdf_format` is set to True, the figure will be saved in PDF format with the specified path.
            If `pdf_format` is False, the function replaces the '.pdf' extension in the `path_to_pdf` with '.png'
            and saves the figure in PNG format.

        Example:
            savefig(path_to_pdf='example.pdf', pdf_format=True)
            # This will save the current figure in PDF format as 'example.pdf'.
        """
        self.class_attributes_update(pdf_format=pdf_format)

        create_folder(folder=self.tools.extract_directory_path(
                    path_to_pdf), loglevel='WARNING')

        if pdf_format:
            plt.savefig(path_to_pdf, format="pdf", bbox_inches="tight", pad_inches=1, transparent=True,
                        facecolor="w", edgecolor='w', orientation='landscape')
        else:
            path_to_pdf = path_to_pdf.replace('.pdf', '.png')
            plt.savefig(path_to_pdf, bbox_inches="tight", pad_inches=1,
                        transparent=True, facecolor="w", edgecolor='w', orientation='landscape')
        self.logger.info(f"The path to plot is: {path_to_pdf}")

    def histogram_plot(self, x: Union[np.ndarray, List[float]], data: Union[np.ndarray, List[float]],
                       positive: bool = True, xlabel: str = '', ylabel: str = '',
                       weights: Optional[Union[np.ndarray, List[float]]] = None, smooth: Optional[bool] = None,
                       step: Optional[bool] = None, color_map: Union[bool, str] = None, linestyle: Optional[str] = None,
                       ylogscale: Optional[bool] = None, xlogscale: Optional[bool] = None,
                       save: bool = True, color: str = 'tab:blue', figsize: Optional[float] = None, legend: str = '_Hidden',
                       plot_title: Optional[str] = None, loc: str = 'upper right', add: Optional[Tuple] = None,
                       fig: Optional[object] = None, path_to_pdf: Optional[str] = None, pdf_format: Optional[bool] = None,
                       xmax: Optional[float] = None, linewidth: Optional[float] = None, fontsize: Optional[int] = None):
        """
        Function to generate a histogram figure based on the provided data.

        Args:
            x (Union[np.ndarray, List[float]]): The data for the x-axis of the histogram.
            data (Union[np.ndarray, List[float]]): The data for the histogram.
            positive (bool, optional): Whether to consider only positive values. Default is True.
            xlabel (str, optional): The label for the x-axis. Default is an empty string.
            ylabel (str, optional): The label for the y-axis. Default is an empty string.
            weights (Optional[Union[np.ndarray, List[float]]]): An array of weights for the data. Default is None.
            smooth (Optional[bool]): Whether to plot a smooth line. Default is None.
            step (Optional[bool]): Whether to plot a step line. Default is None.
            color_map (Union[bool, str], optional): Whether to apply a color map to the histogram bars. Default is None.
            linestyle (str, optional): The line style for the plot. Default is None.
            ylogscale (Optional[bool]): Whether to use a logarithmic scale for the y-axis. Default is None.
            xlogscale (Optional[bool]): Whether to use a logarithmic scale for the x-axis. Default is None.
            save (bool, optional): Whether to save the plot. Default is True.
            color (str, optional): The color of the plot. Default is 'tab:blue'.
            figsize (Optional[float]): The size of the figure. Default is None.
            legend (str, optional): The legend label for the plot. Default is '_Hidden'.
            plot_title (str, optional): The title of the plot. Default is None.
            loc(str, optional): The location of the legend. Default is 'upper right'.
            add (Optional[Tuple]): Tuple of (fig, ax) to add the plot to an existing figure. Default is None.
            fig (Optional[object]): The figure object to plot on. If provided, ignores the 'add' argument. Default is None.
            path_to_pdf (str, optional): The path to save the figure. If provided, saves the figure at the specified path.
                                         Default is None.
            pdf_format (Optional[bool]): Whether to save the figure in PDF format. Default is None.
            xmax (Optional[float]): The maximum value for the x-axis. Default is None.
            linewidth (Optional[float]): The width of the line. Default is None.
            fontsize (Optional[int]): The font size of the labels. Default is None.

        Returns:
            A dictionary containing the figure and axes objects.
        """
        self.class_attributes_update(pdf_format=pdf_format, color_map=color_map, xlogscale=xlogscale, ylogscale=ylogscale,
                                     figsize=figsize, fontsize=fontsize, smooth=smooth, step=step, linestyle=linestyle,
                                     linewidth=linewidth)
        if fig is not None:
            fig, ax = fig
        elif add is None and fig is None:
            fig, ax = plt.subplots(figsize=(8*self.figsize, 5*self.figsize))
        elif add is not None:
            fig, ax = add

        if positive:
            data = data.where(data > 0)
        if self.smooth:
            plt.plot(x, data, linewidth=self.linewidth, linestyle=self.linestyle, color=color, label=legend)
            plt.grid(True)
        elif self.step:
            plt.step(x, data, linewidth=self.linewidth, linestyle=self.linestyle, color=color, label=legend)
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
        if not any([child.get_visible() for child in ax.get_children()]):
            self.logger.error("The plot is empty. Skipping saving the plot.")
            return
        else:
            if save and isinstance(path_to_pdf, str):
                self.savefig(path_to_pdf, self.pdf_format)
        return {fig, ax}

    def plot_of_average(self, data: Union[list, xr.DataArray] = None, trop_lat: Optional[float] = None, ylabel: str = '',
                        coord: Optional[str] = None, fontsize: Optional[int] = None, pad: int = 15,
                        y_lim_max: Optional[float] = None, number_of_axe_ticks: Optional[int] = None,
                        legend: str = '_Hidden', figsize: Optional[int] = None, linestyle: Optional[str] = None,
                        maxticknum: int = 12, color: str = 'tab:blue', ylogscale: Optional[bool] = None,
                        xlogscale: Optional[bool] = None, loc: str = 'upper right', add: Optional[list] = None,
                        fig: Optional[list] = None, plot_title: Optional[str] = None, path_to_pdf: Optional[str] = None,
                        save: bool = True, pdf_format: Optional[bool] = None):
        """
        Make a plot with different y-axes using a second axis object.

        Args:
            data (Union[list, xr.DataArray]): Data to plot.
            trop_lat (float, optional): Tropospheric latitude. Defaults to None.
            ylabel (str, optional): Label for the y-axis. Defaults to ''.
            coord (str, optional): Coordinate for the plot. Can be 'lon', 'lat', or 'time'. Defaults to None.
            fontsize (int, optional): Font size for the plot. Defaults to None.
            pad (int): Padding value. Defaults to 15.
            y_lim_max (float, optional): Maximum limit for the y-axis. Defaults to None.
            legend (str, optional): Legend for the plot. Defaults to '_Hidden'.
            figsize (int, optional): Figure size. Defaults to None.
            linestyle (str, optional): Line style for the plot. Defaults to None.
            maxticknum (int, optional): Maximum number of ticks. Defaults to 12.
            color (str, optional): Color for the plot. Defaults to 'tab:blue'.
            ylogscale (bool, optional): Use logarithmic scale for the y-axis. Defaults to None.
            xlogscale (bool, optional): Use logarithmic scale for the x-axis. Defaults to None.
            loc (str, optional): Location for the legend. Defaults to 'upper right'.
            add (list, optional): Additional objects to add. Defaults to None.
            fig (list, optional): Figure objects. Defaults to None.
            plot_title (str, optional): Title for the plot. Defaults to None.
            path_to_pdf (str, optional): Path to save the figure as a PDF. Defaults to None.
            save (bool, optional): Whether to save the figure. Defaults to True.
            pdf_format (bool, optional): Save the figure in PDF format. Defaults to True.

        Returns:
            list: List of figure and axis objects.
        """
        self.class_attributes_update(pdf_format=pdf_format, xlogscale=xlogscale, number_of_axe_ticks=number_of_axe_ticks,
                                     ylogscale=ylogscale, figsize=figsize, fontsize=fontsize, linestyle=linestyle)

        # make a plot with different y-axis using second axis object
        labels_int = data[coord].values

        if 'Dataset' in str(type(data)):
            if fig is not None:
                ax1, ax2, ax3, ax4, ax5, ax_twin_5 = fig[1], fig[2], fig[3], fig[4], fig[5], fig[6]
                fig = fig[0]
                axs = [ax1, ax2, ax3, ax4, ax5]

            elif add is None and fig is None:
                fig = plt.figure(figsize=(10*self.figsize, 11*self.figsize), layout='constrained')
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
                    axs[i].set_xlabel('Longitude', fontsize=self.fontsize-2)
                    axs[i].set_ylabel('Latitude', fontsize=self.fontsize-2)
                elif coord == 'lat':
                    axs[i].set_xlabel('Latitude', fontsize=self.fontsize-2)

                plt.yscale('log') if self.ylogscale else None
                plt.xscale('log') if self.xlogscale else None

                if coord == 'lon':
                    # twin object for two different y-axis on the sample plot
                    ax_span = axs[i].twinx()
                    if trop_lat < 90:
                        ax_span.axhspan(-trop_lat, trop_lat, alpha=0.05, color='tab:red')
                        axs[i].coastlines(alpha=0.5, color='grey')
                    else:
                        axs[i].coastlines(alpha=0.5, color='k')
                    ax_span.set_ylim([-90, 90])
                    ax_span.set_xticks([])
                    ax_span.set_yticks([])
                    axs[i].set_xticks(np.arange(-180, 181, 360/self.number_of_axe_ticks), crs=ccrs.PlateCarree())
                    axs[i].xaxis.set_major_formatter(cticker.LongitudeFormatter())

                    # Latitude labels
                    axs[i].set_yticks(np.arange(-90, 91, 180/self.number_of_axe_ticks), crs=ccrs.PlateCarree())
                    axs[i].yaxis.set_major_formatter(cticker.LatitudeFormatter())

                    if i < 4:
                        ax_twin = axs[i].twinx()
                        ax_twin.set_frame_on(True)
                        ax_twin.plot(one_season.lon - 180, one_season, color=color, label=legend, linestyle=self.linestyle)
                        ax_twin.set_ylim([0, y_lim_max])
                        ax_twin.set_ylabel(ylabel, fontsize=self.fontsize-3)
                    else:
                        ax_twin_5.set_frame_on(True)
                        ax_twin_5.plot(one_season.lon - 180, one_season, color=color,  label=legend, linestyle=self.linestyle)
                        ax_twin_5.set_ylim([0, y_lim_max])
                        ax_twin_5.set_ylabel(ylabel, fontsize=self.fontsize-3)
                        axs[i].set_xlabel('Longitude', fontsize=self.fontsize-3)

                else:
                    axs[i].plot(one_season.lat, one_season, color=color, label=legend, linestyle=self.linestyle)
                    axs[i].set_ylim([0, y_lim_max])
                    axs[i].set_ylabel(ylabel, fontsize=self.fontsize-3)
                    axs[i].set_xlabel('Latitude', fontsize=self.fontsize-3)

                axs[i].grid(True)
            if coord == 'lon':
                if legend != '_Hidden':
                    ax_twin_5.legend(loc=loc, fontsize=self.fontsize-3, ncol=2)
                if plot_title is not None:
                    plt.suptitle(plot_title, fontsize=self.fontsize+2)
            else:
                if legend != '_Hidden':
                    ax5.legend(loc=loc, fontsize=self.fontsize-3, ncol=2)
                if plot_title is not None:
                    plt.suptitle(plot_title, fontsize=self.fontsize+2)
        elif 'DataArray' in str(type(data)):
            if fig is not None:
                fig, ax = fig
            elif add is None and fig is None:
                fig, ax = plt.subplots(figsize=(8*self.figsize, 5*self.figsize))
            elif add is not None:
                fig, ax = add
            if data.size == 1:
                plt.axhline(y=float(data.values), color=color, label=legend, linestyle=self.linestyle)
            else:
                if coord == 'time':
                    plt.scatter(labels_int, data, color=color, label=legend, linestyle=self.linestyle)
                else:
                    plt.plot(labels_int, data, color=color, label=legend, linestyle=self.linestyle)

            plt.gca().xaxis.set_major_locator(plt.MaxNLocator(maxticknum))
            plt.gca().tick_params(axis='both',   which='major',    pad=10)
            plt.xlim([min(labels_int),    max(labels_int)])
            plt.grid(True)
            if coord == 'time':
                plt.xlabel('Timestep index', fontsize=self.fontsize-3)
                if data['time.year'][0].values == data['time.year'][-1].values:
                    plt.xlabel(
                        str(data['time.year'][0].values),    fontsize=self.fontsize-3)
                else:
                    plt.xlabel(str(data['time.year'][0].values)+' - '+str(data['time.year'][-1].values),
                               fontsize=self.fontsize-3)
            elif coord == 'lat':
                plt.xlabel('Latitude', fontsize=self.fontsize-3)
            elif coord == 'lon':
                plt.xlabel('Longitude', fontsize=self.fontsize-3)
            plt.ylabel(ylabel, fontsize=self.fontsize-3)

            if plot_title is not None:
                plt.title(plot_title, fontsize=self.fontsize+2, pad=15)

            if legend != '_Hidden':
                plt.legend(loc=loc, fontsize=self.fontsize-2, ncol=2)

            plt.yscale('log') if self.ylogscale else None
            plt.xscale('log') if self.xlogscale else None

        if save and isinstance(path_to_pdf, str):
            self.savefig(path_to_pdf, self.pdf_format)

        if 'Dataset' in str(type(data)):
            return [fig,  ax1, ax2, ax3, ax4, ax5, ax_twin_5]
        else:
            return [fig,  ax]

    def plot_seasons_or_months(self, data: xr.DataArray, cbarlabel: Optional[str] = None, seasons: Optional[list] = None,
                               months: Optional[list] = None, cmap: str = 'coolwarm', save: bool = True,
                               figsize: Optional[int] = None, plot_title: Optional[str] = None,  vmin: Optional[float] = None,
                               vmax: Optional[float] = None, fontsize: Optional[int] = None, linestyle: Optional[str] = None,
                               path_to_pdf: Optional[str] = None, pdf_format: Optional[bool] = None):
        """ Function to plot seasonal data.

        Args:
            data (xarray.DataArray): First dataset to be plotted.
            cbarlabel (str, optional): Label for the colorbar. Defaults to None.
            seasons (list, optional): List of seasonal datasets. Defaults to None.
            months (list, optional): List of monthly datasets. Defaults to None.
            cmap (str, optional): Colormap for the plot. Defaults to 'coolwarm'.
            save (bool, optional): Whether to save the figure. Defaults to True.
            figsize (int, optional): Size of the figure. Defaults to None.
            plot_title (str, optional): Title of the plot. Defaults to None.
            vmin (float, optional): Minimum value of the colorbar. Defaults to None.
            vmax (float, optional): Maximum value of the colorbar. Defaults to None.
            fontsize (int, optional): Font size for the plot. Defaults to None.
            linestyle (str, optional): Line style for the plot. Defaults to None.
            path_to_pdf (str, optional): Path to save the PDF file. Defaults to None.
            pdf_format (bool, optional): If True, save the figure in PDF format. Defaults to True.
        """
        self.class_attributes_update(pdf_format=pdf_format, cmap=cmap,
                                     figsize=figsize, fontsize=fontsize, linestyle=linestyle)

        clevs = self.ticks_for_colorbar(data, vmin=vmin, vmax=vmax, model_variable=self.model_variable,
                                        number_of_bar_ticks=self.number_of_bar_ticks)

        if months is None:
            fig = plt.figure(figsize=(11*self.figsize, 10*self.figsize), layout='constrained')
            gs = fig.add_gridspec(3, 2)
            ax1 = fig.add_subplot(gs[0, 0], projection=ccrs.PlateCarree())
            ax2 = fig.add_subplot(gs[0, 1], projection=ccrs.PlateCarree())
            ax3 = fig.add_subplot(gs[1, 0], projection=ccrs.PlateCarree())
            ax4 = fig.add_subplot(gs[1, 1], projection=ccrs.PlateCarree())
            ax5 = fig.add_subplot(gs[2, :], projection=ccrs.PlateCarree())
            axs = [ax1, ax2, ax3, ax4, ax5]

            titles = ["DJF", "MAM", "JJA", "SON", "Yearly"]

            for i in range(0, len(seasons)):
                one_season = seasons[i]

                one_season = one_season.where(one_season > vmin)
                one_season, lons = add_cyclic_point(one_season, coord=data['lon'])
                im1 = axs[i].contourf(lons, data['lat'], one_season, clevs, transform=ccrs.PlateCarree(),
                                      cmap=self.cmap, extend='both')
                axs[i].set_title(titles[i], fontsize=self.fontsize+3)
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

        else:
            fig, axes = plt.subplots(ncols=3, nrows=4, subplot_kw={'projection': ccrs.PlateCarree()},
                                     figsize=(11*self.figsize, 8.5*self.figsize), layout='constrained')

            for i in range(0, len(months)):
                months[i] = months[i].where(months[i] > vmin)
                months[i], lons = add_cyclic_point(months[i], coord=data['lon'])

            titles = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September',
                      'October', 'November', 'December']
            axs = axes.flatten()

            for i in range(0, len(months)):
                im1 = axs[i].contourf(lons, data['lat'], months[i], clevs, transform=ccrs.PlateCarree(),
                                      cmap=self.cmap, extend='both')
                axs[i].set_title(titles[i], fontsize=self.fontsize+3)
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
        cbar = fig.colorbar(im1, ticks=clevs, ax=ax5, location='bottom')
        cbar.set_label(cbarlabel, fontsize=self.fontsize)

        if plot_title is not None:
            plt.suptitle(plot_title, fontsize=self.fontsize+3)

        if save and isinstance(path_to_pdf, str):
            self.savefig(path_to_pdf, self.pdf_format)

    def ticks_for_colorbar(self, data: Union[xr.DataArray, float, int], vmin: Optional[Union[float, int]] = None,
                           vmax: Optional[Union[float, int]] = None, model_variable: Optional[str] = None,
                           number_of_bar_ticks: Optional[int] = None):
        """Compute ticks and levels for a color bar based on provided data.

        Args:
            data (Union[xarray.DataArray, float, int]): The data from which to compute the color bar.
            vmin (Union[float, int], optional): The minimum value of the color bar. If None, it is derived from the data.
                                                Defaults to None.
            vmax (Union[float, int], optional): The maximum value of the color bar. If None, it is derived from the data.
                                                Defaults to None.
            model_variable (str, optional): The variable to consider for the color bar computation. Defaults to None.
            number_of_bar_ticks (int, optional): The number of ticks to be computed for the color bar. Defaults to None.

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
        self.logger.debug('Clevs: {}'.format(clevs))
        return clevs

    def map(self, data: List[xr.DataArray], titles: Optional[Union[List[str], str]] = None,
            lonmin: int = -180, lonmax: int = 181, latmin: int = -90, latmax: int = 91,
            cmap: Optional[str] = None, save: bool = True, model_variable: Optional[str] = None,
            figsize: Optional[float] = None,  number_of_axe_ticks: Optional[int] = None,
            number_of_bar_ticks: Optional[int] = None, cbarlabel: str = '',
            plot_title: Optional[str] = None, vmin: Optional[float] = None, vmax: Optional[float] = None,
            path_to_pdf: Optional[str] = None, pdf_format: Optional[bool] = None, fontsize: Optional[int] = None):
        """
        Generate a map with subplots for provided data.

        Args:
            data (list): List of data to plot.
            titles (Union[list, str], optional): Titles for the subplots. If str, it will be repeated for each subplot.
                                                 Defaults to None.
            lonmin (int, optional): Minimum longitude. Defaults to -180.
            lonmax (int, optional): Maximum longitude. Defaults to 181.
            latmin (int, optional): Minimum latitude. Defaults to -90.
            latmax (int, optional): Maximum latitude. Defaults to 91.
            model_variable (str, optional): Model variable for the plot. Defaults to 'mtpr'.
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
        self.class_attributes_update(pdf_format=pdf_format, figsize=figsize, fontsize=fontsize,
                                     model_variable=model_variable, number_of_axe_ticks=number_of_axe_ticks,
                                     number_of_bar_ticks=number_of_bar_ticks)
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
        self.logger.debug('Size of the plot after auto re-scaling: {}, {}'.format(horizontal_size*figsize,
                                                                                  vertical_size*figsize))

        fig = plt.figure(figsize=(horizontal_size*figsize, vertical_size*figsize))
        gs = GridSpec(nrows=nrows, ncols=ncols, figure=fig, wspace=0.175, hspace=0.175, width_ratios=[1] * ncols,
                      height_ratios=[1] * nrows)
        # Add subplots using the grid
        axs = [fig.add_subplot(gs[i, j], projection=ccrs.PlateCarree()) for i in range(nrows) for j in range(ncols)]
        clevs = self.ticks_for_colorbar(data, vmin=vmin, vmax=vmax, model_variable=self.model_variable,
                                        number_of_bar_ticks=self.number_of_bar_ticks)

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
        [axs[ncols*i].set_ylabel('Latitude', fontsize=self.fontsize) for i in range(0, nrows)]
        # Draw the colorbar
        fig.subplots_adjust(bottom=0.25, top=0.9, left=0.05, right=0.95, wspace=0.2, hspace=0.5)
        cbar_ax = fig.add_axes([0.2, 0.15, 0.6, 0.02])

        cbar = fig.colorbar(im1, cax=cbar_ax, ticks=clevs, orientation='horizontal', extend='both')
        cbar.set_label(cbarlabel, fontsize=self.fontsize)

        if plot_title is not None:
            plt.suptitle(plot_title, fontsize=self.fontsize+3)

        if save and isinstance(path_to_pdf, str):
            self.savefig(path_to_pdf, self.pdf_format)

    def daily_variability_plot(self, data, ymax: float = 12, relative: bool = True, save: bool = True,
                               legend: str = '_Hidden', figsize: float = None, linestyle: str = None, color: str = 'tab:blue',
                               model_variable: str = None, loc: str = 'upper right', fontsize: int = None,
                               add: Optional[Tuple] = None, fig: Optional[object] = None, plot_title: str = None,
                               path_to_pdf: str = None, pdf_format: bool = True):
        """
        Plot the daily variability of the dataset.

        This function generates a plot showing the daily variability of the provided dataset. It allows customization
        of various plot parameters such as color, scale, and legends.

        Args:
            data: The dataset to be plotted.
            ymax (float, optional): The maximum value for the y-axis. Defaults to 12.
            relative (bool, optional): Whether the plot is relative. Defaults to True.
            save (bool, optional): Whether to save the plot. Defaults to True.
            legend (str, optional): The legend for the plot. Defaults to '_Hidden'.
            figsize (float, optional): The figure size. Defaults to None.
            linestyle (str, optional): The line style for the plot. Defaults to None.
            color (str, optional): The color for the plot. Defaults to 'tab:blue'.
            model_variable (str, optional): The model variable for the plot. Defaults to None.
            loc (str, optional): The location for the legend. Defaults to 'upper right'.
            fontsize (int, optional): The font size for the plot. Defaults to None.
            add (list, optional): Additional objects to add. Defaults to None.
            fig (list, optional): The figure objects. Defaults to None.
            plot_title (str, optional): The title for the plot. Defaults to None.
            path_to_pdf (str, optional): The path to save the figure as a PDF. Defaults to None.
            pdf_format (bool, optional): Whether to save the figure in PDF format. Defaults to True.

        Returns:
            list: A list containing the figure and axis objects.
        """
        self.class_attributes_update(pdf_format=pdf_format, figsize=figsize, fontsize=fontsize,
                                     model_variable=model_variable)
        if fig is not None:
            fig, ax = fig
        elif add is None and fig is None:
            fig, ax = plt.subplots(figsize=(8*self.figsize, 5*self.figsize))
        elif add is not None:
            fig, ax = add

        grouped = data.groupby('local_time')
        mean_per_hour = grouped.mean()
        
        data['local_time'].values = data['local_time'].astype(int).values
        grouped_smooth = data.groupby('local_time')
        mean_per_hour_smooth = grouped_smooth.mean()
        
        utc_time = mean_per_hour['local_time']
        utc_time_smooth = mean_per_hour_smooth['local_time']
        if relative:
            mtpr = mean_per_hour['mtpr_relative']
            mtpr_smooth = mean_per_hour_smooth['mtpr_relative']
        else:
            mtpr = mean_per_hour[self.model_variable]
            mtpr_smooth = mean_per_hour_smooth[self.model_variable]
        try:
            units = mean_per_hour.units
        except AttributeError:
            units = mean_per_hour.mtpr.units

        plt.plot(utc_time, mtpr, color=color, linestyle=self.linestyle, alpha=0.25)
        plt.plot(utc_time_smooth, mtpr_smooth, color=color, label=legend, linestyle=self.linestyle,
                 linewidth=1*self.linewidth)
        if plot_title is None:
            if relative:
                plt.suptitle(
                    'Relative Value of Daily Precipitation Variability', fontsize=self.fontsize+1)
                plt.ylabel('mtpr variability, '+units, fontsize=self.fontsize-2)
            else:
                plt.suptitle('Daily Precipitation Variability', fontsize=self.fontsize+1)
                plt.ylabel('relative mtpr', fontsize=self.fontsize-2)
        else:
            plt.suptitle(plot_title, fontsize=self.fontsize+3)

        plt.grid(True)
        plt.ylim([-2,5])
        plt.xlim([0-0.2,24+0.2])
        plt.xlabel('Local time', fontsize=self.fontsize-2)

        if legend != '_Hidden':
            plt.legend(loc=loc,
                       fontsize=self.fontsize-2, ncol=2)

        if save and isinstance(path_to_pdf, str):
            self.savefig(path_to_pdf, self.pdf_format)

        return {fig, ax}
