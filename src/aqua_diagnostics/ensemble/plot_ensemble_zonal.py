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
        outputdir="./",
        loglevel: str = "WARNING",
    ):
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

    def plot(self, var: str = None, dataset_mean = None, dataset_std = None, description = None, title_mean = None, title_std = None, figure_size = [10,8], cbar_label = None, save_pdf=True, save_png=True, units = None, ylim = (5500,0), levels = 20, cmap = "RdBu_r", ylabel = "Depth (in m)", xlabel = "Latitude (in deg North)"):
        """
        This plots the ensemble mean and standard deviation of the ensemble statistics.
        
        Returns:
            a dict of fig and ax for mean and STD
            return {'mean_plot': [fig1, ax1], 'std_plot': [fig2, ax2]}
        """
        self.logger.info(
            "Plotting the ensemble computation of Zonal-averages as mean and STD in Lev-Lon of var {self.var}"
        )

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
            self.save_figure(var=var, fig=fig1, fig_std=fig2,  description=description, format='png')
        if save_pdf:
            self.save_figure(var=var, fig=fig1, fig_std=fig2, description=description, format='pdf')

        return {'mean_plot': [fig1, ax1], 'std_plot': [fig2, ax2]}



