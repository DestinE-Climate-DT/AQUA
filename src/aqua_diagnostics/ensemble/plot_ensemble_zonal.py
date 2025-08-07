import matplotlib.pyplot as plt
import xarray as xr
from aqua.logger import log_configure
from aqua.exceptions import NoDataError
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
        figure_size = (10,8),
        save_pdf = True,
        save_png = True,
        var: str = None,
        dataset_mean = None,
        dataset_std = None,
        description = None,
        title_mean = None,
        title_std = None,
        cbar_label = None,
        units = None,
        ylim = (5500,0),
        levels = 20,
        cmap = "RdBu_r",
        ylabel = "Depth (in m)",
        xlabel = "Latitude (in deg North)",
        outputdir="./",
        log_level: str = "WARNING",
    ):
        self.diagnostic_product = diagnostic_product
        self.catalog_list = catalog_list
        self.model_list = model_list
        self.exp_list = exp_list
        self.source_list = source_list
        self.region = region
        self.figure_size = figure_size
        self.save_pdf = save_pdf
        self.save_png = save_png
        self.var = var
        self.dataset_mean = dataset_mean
        self.dataset_std = dataset_std
        self.title_mean = title_mean
        self.title_std = title_std
        self.cbar_label = cbar_label
        self.units = units
        self.cmap = cmap
        self.levels = levels
        self.ylim = ylim
        self.xlabel = xlabel
        self.ylabel = ylabel

        self.outputdir = outputdir
        self.log_level = log_level

        super().__init__(
            log_level=self.log_level,
            diagnostic_product=self.diagnostic_product,
            catalog_list=self.catalog_list,
            model_list=self.model_list,
            exp_list=self.exp_list,
            source_list=self.source_list,
            outputdir=self.outputdir,
        )

        self.title_mean = "Ensemble mean of " + self.model if title_mean is None else title_mean
        self.title_std = "Ensemble standard deviation of " + self.model if title_std is None else title_std
        self.description = self.catalog + "_" + self.model if description is None else description

    def plot(self):
        """
        This plots the ensemble mean and standard deviation of the ensemble statistics.
        To edit the default settings please call the method "edit_attributes"
        
        Returns:
            a dict of fig and ax for mean and STD
            return {'mean_plot': [fig1, ax1], 'std_plot': [fig2, ax2]}
        """

        self.logger.info(
            "Plotting the ensemble computation of Zonal-averages as mean and STD in Lev-Lon of var {self.var}"
        )

        if (self.dataset_mean is None) or (self.dataset_std is None):
            raise NoDataError("No data given to the plotting function")

        if isinstance(self.dataset_mean, xr.Dataset):
            self.dataset_mean = self.dataset_mean[self.var]
        else:
            self.dataset_mean = self.dataset_mean
        self.logger.info("Plotting ensemble-mean Zonal-average")
        fig1 = plt.figure(figsize=self.figure_size)
        ax1 = fig1.add_subplot(1, 1, 1)
        im = ax1.contourf(
            self.dataset_mean.lat,
            self.dataset_mean.lev,
            self.dataset_mean,
            cmap=self.cmap,
            levels=self.levels,
            extend="both",
        )
        ax1.set_ylim(self.ylim)
        ax1.set_ylabel(self.ylabel, fontsize=9)
        ax1.set_xlabel(self.xlabel, fontsize=9)
        ax1.set_facecolor("grey")
        ax1.set_title(self.title_mean)
        cbar = fig1.colorbar(im, ax=ax1, shrink=0.9, extend="both")
        cbar.set_label(self.cbar_label)
        self.logger.debug(f"Saving Lev-Lon Zonal-average ensemble-mean as pdf and png")

        if isinstance(self.dataset_std, xr.Dataset):
            self.dataset_std = self.dataset_std[self.var]
        else:
            self.dataset_std = self.dataset_std
        self.logger.info("Plotting ensemble-STD Zonal-average")
        fig2 = plt.figure(figsize=(self.figure_size[0], self.figure_size[1]))
        ax2 = fig2.add_subplot(1, 1, 1)
        im = ax2.contourf(
            self.dataset_std.lat,
            self.dataset_std.lev,
            self.dataset_std,
            cmap=self.cmap,
            levels=self.levels,
            extend="both",
        )
        ax2.set_ylim(self.ylim)
        ax2.set_ylabel(self.ylabel, fontsize=9)
        ax2.set_xlabel(self.xlabel, fontsize=9)
        ax2.set_facecolor("grey")
        ax2.set_title(self.title_std)
        cbar = fig2.colorbar(im, ax=ax2, shrink=0.9, extend="both")
        cbar.set_label(self.cbar_label)
        self.logger.debug(f"Saving Lev-Lon Zonal-average ensemble-STD as pdf and png")
        
        # Saving plots
        if self.save_png:
            self.save_figure(var=self.var, fig=fig1, fig_std=fig2,  description=self.description, format='png')
        if self.save_pdf:
            self.save_figure(var=self.var, fig=fig1, fig_std=fig2, description=self.description, format='pdf')

        return {'mean_plot': [fig1, ax1], 'std_plot': [fig2, ax2]}



