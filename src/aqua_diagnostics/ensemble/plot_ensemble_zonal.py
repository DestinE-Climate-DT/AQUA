import matplotlib.pyplot as plt
import xarray as xr
from aqua.exceptions import NoDataError
from aqua.graphics import plot_vertical_profile
from aqua.logger import log_configure

from .base import BaseMixin

xr.set_options(keep_attrs=True)


class PlotEnsembleZonal(BaseMixin):
    def __init__(
        self,
        diagnostic_product: str = "EnsembleZonal",
        catalog_list: list[str] = None,
        model_list: list[str] = None,
        exp_list: list[str] = None,
        source_list: list[str] = None,
        region: str = None,
        outputdir="./",
        loglevel: str = "WARNING",
    ):
        """
        Class for plotting ensemble zonal mean data.

        This class inherits from `BaseMixin` and provides functionality to
        visualize ensemble datasets as zonal averages. It supports multiple
        catalogs, models, experiments, and sources, and allows specifying a
        region for the analysis. The resulting plots can be saved to a
        specified output directory.

        Args:
            diagnostic_product (str, optional): Name of the diagnostic product.
                Defaults to "EnsembleZonal".
            catalog_list (list[str], optional): List of catalog names. If None,
                assigned to 'None_catalog'.
            model_list (list[str], optional): List of model names. If None,
                assigned to 'None_model'.
            exp_list (list[str], optional): List of experiment names. If None,
                assigned to 'None_exp'.
            source_list (list[str], optional): List of source names. If None,
                assigned to 'None_source'.
            region (str, optional): Name of the region for zonal averaging. Defaults to None.
            outputdir (str, optional): Directory path to save plots. Defaults to "./".
            loglevel (str, optional): Logging level. Defaults to "WARNING".

        Attributes:
            diagnostic_product (str): Name of the diagnostic product.
            catalog_list (list[str]): List of catalogs being processed.
            model_list (list[str]): List of models being processed.
            exp_list (list[str]): List of experiments being processed.
            source_list (list[str]): List of sources being processed.
            region (str): Region used for zonal analysis.
            outputdir (str): Output directory for saving plots.
            loglevel (str): Logging level for messages.

        TODO:
            - Add support for sub-region selection.
            - Add optional regridding of input datasets.
            - Include automatic color scale adjustment for multi-model ensembles.
            - Add functionality to overlay observational or reference zonal datasets.
        """
        self.diagnostic_product = diagnostic_product
        self.catalog_list = catalog_list
        self.model_list = model_list
        self.exp_list = exp_list
        self.source_list = source_list
        self.region = region

        self.outputdir = outputdir
        self.loglevel = loglevel

        super().__init__(
            loglevel=self.loglevel,
            diagnostic_product=self.diagnostic_product,
            catalog_list=self.catalog_list,
            model_list=self.model_list,
            exp_list=self.exp_list,
            source_list=self.source_list,
            outputdir=self.outputdir,
        )

    def plot(
        self,
        var: str = None,
        dataset_mean=None,
        dataset_std=None,
        description=None,
        title_mean=None,
        title_std=None,
        figure_size=[10, 8],
        cbar_label=None,
        save_pdf=True,
        save_png=True,
        dpi=300,
        units=None,
        ylim=(5500, 0),
        levels=20,
        cmap="RdBu_r",
        ylabel="Depth (in m)",
        xlabel="Latitude (in deg North)",
    ):
        """
        Plot ensemble mean and standard deviation of zonal averages in Lev-Lat coordinates.

        This method generates contour plots of the ensemble mean and standard deviation
        for a given variable on a latitude vs. vertical level (Lev) grid. The resulting
        plots can be saved as PNG and/or PDF files using the `save_figure` method.

        Args:
            var (str): Name of the variable to plot.
            dataset_mean (xarray.DataArray or xarray.Dataset): Ensemble mean data.
            dataset_std (xarray.DataArray or xarray.Dataset): Ensemble standard deviation data.
            description (str, optional): Description for saving the plots.
            title_mean (str, optional): Title for the mean plot. Auto-generated if None.
            title_std (str, optional): Title for the standard deviation plot. Auto-generated if None.
            figure_size (list[int], optional): Figure size [width, height]. Default is [10, 8].
            cbar_label (str, optional): Label for the colorbar.
            save_pdf (bool, optional): Save plots as PDF. Default is True.
            save_png (bool, optional): Save plots as PNG. Default is True.
            dpi (int, optional): Resolution for saved figures. Default is 300.
            units (str, optional): Units of the variable. Used in titles and labels if provided.
            ylim (tuple, optional): Y-axis limits for the plot (vertical levels). Default is (5500, 0).
            levels (int, optional): Number of contour levels. Default is 20.
            cmap (str, optional): Colormap to use. Default is "RdBu_r".
            ylabel (str, optional): Label for y-axis. Default is "Depth (in m)".
            xlabel (str, optional): Label for x-axis. Default is "Latitude (in deg North)".

        Returns:
            dict: Dictionary containing figure and axes objects for mean and std plots:
                {
                    'mean_plot': [fig1, ax1],
                    'std_plot': [fig2, ax2]
                }

        Raises:
            NoDataError: If `dataset_mean` or `dataset_std` is None.

        Notes:
            - Automatically generates titles for mean and STD if not provided.
            - Uses `self.save_figure` to save the plots as PNG and PDF.
            - Designed for zonal mean visualizations in Lev-Lat coordinates.
            - Default y-axis (vertical levels) is set to descend from 5500 m to 0 m.

        TODO:
            - Add support for multiple variables in a single call.
            - Include optional overlay of observations or reference zonal datasets.
            - Improve automatic scaling of colorbars for multiple variables or ensembles.
            - Add interactive plotting options.
        """
        self.logger.info("Plotting the ensemble computation of Zonal-averages as mean and STD in Lev-Lon of var {self.var}")

        title_mean = "Ensemble mean of " + self.model if title_mean is None else title_mean
        title_std = "Ensemble standard deviation of " + self.model if title_std is None else title_std

        if (dataset_mean is None) or (dataset_std is None):
            raise NoDataError("No data given to the plotting function")

        if isinstance(dataset_mean, xr.Dataset):
            dataset_mean = dataset_mean[var]
        else:
            dataset_mean = dataset_mean
        self.logger.info("Plotting ensemble-mean Zonal-average")

        fig1 = plt.figure(figsize=figure_size)
        ax1 = fig1.add_subplot(1, 1, 1)
        im = ax1.contourf(
            dataset_mean.lat,
            dataset_mean.lev,
            dataset_mean,
            cmap=cmap,
            levels=levels,
            extend="both",
        )
        ax1.set_ylim(ylim)
        ax1.set_ylabel(ylabel, fontsize=9)
        ax1.set_xlabel(xlabel, fontsize=9)
        ax1.set_facecolor("grey")
        ax1.set_title(title_mean)
        cbar = fig1.colorbar(im, ax=ax1, shrink=0.9, extend="both")
        cbar.set_label(cbar_label)
        self.logger.debug(f"Saving Lev-Lon Zonal-average ensemble-mean as pdf and png")

        if isinstance(dataset_std, xr.Dataset):
            dataset_std = dataset_std[var]
        else:
            dataset_std = dataset_std
        self.logger.info("Plotting ensemble-STD Zonal-average")

        fig2 = plt.figure(figsize=(figure_size[0], figure_size[1]))
        ax2 = fig2.add_subplot(1, 1, 1)
        im = ax2.contourf(
            dataset_std.lat,
            dataset_std.lev,
            dataset_std,
            cmap=cmap,
            levels=levels,
            extend="both",
        )
        ax2.set_ylim(ylim)
        ax2.set_ylabel(ylabel, fontsize=9)
        ax2.set_xlabel(xlabel, fontsize=9)
        ax2.set_facecolor("grey")
        ax2.set_title(title_std)
        cbar = fig2.colorbar(im, ax=ax2, shrink=0.9, extend="both")
        cbar.set_label(cbar_label)
        self.logger.debug(f"Saving Lev-Lon Zonal-average ensemble-STD as pdf and png")

        # Saving plots
        if save_png:
            self.save_figure(var=var, fig=fig1, fig_std=fig2, description=description, format="png", dpi=dpi)
        if save_pdf:
            self.save_figure(var=var, fig=fig1, fig_std=fig2, description=description, format="pdf")

        return {"mean_plot": [fig1, ax1], "std_plot": [fig2, ax2]}
