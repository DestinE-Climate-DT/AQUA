import os
import matplotlib.pyplot as plt
import xarray as xr
# from dask import compute
from aqua.logger import log_configure
from aqua.util import create_folder
from aqua.exceptions import NoDataError
from .util import compute_statistics

xr.set_options(keep_attrs=True)


class EnsembleZonal:
    """
    A class to compute ensemble mean and standard deviation of the Zonal averages
    Make sure that the dataset has correct lev-lat dimensions.
    """

    def __init__(
        self,
        var=None,
        dataset=None,
        ensemble_dimension_name="ensemble",
        outputdir="./",
        loglevel="WARNING",
        **kwargs,
    ):
        """
        Args:
            var (str): Variable name.
            dataset: xarray Dataset composed of ensembles 2D Zonal data, i.e.,
                     the individual Dataset (lev-lat) are concatenated along.
                     a new dimension "ensemble". This ensemble name can be changed.
            ensemble_dimension_name="ensemble" (str): a default name given to the
                     dimensions along with the individual Datasets were concatenated.
            loglevel (str): Log level. Default is "WARNING".
            **kwargs (dict): dictonary contains options for plots. Defaults values have been set already.
        """
        self.loglevel = loglevel
        self.logger = log_configure(log_level=self.loglevel, log_name="Ensemble Zonal Averages")

        self.var = var
        self.dataset = dataset
        self.dim = ensemble_dimension_name
        self.dataset_mean = None
        self.dataset_std = None

        self.outputdir = outputdir + "Ensemble_Zonal"
        self.outputfile = f"global_Zonal_plot_{var}"

        self.figure = None
        plot_options = kwargs.get("plot_options", {})
        self.plot_label = plot_options.get("plot_label", True)
        self.plot_std = plot_options.get("plot_std", True)
        self.figure_size = plot_options.get("figure_size", [15, 15])
        self.units = plot_options.get("units", None)
        self.dpi = plot_options.get("dpi", 300)
        if self.units is None:
            self.units = self.dataset[self.var].units
        self.cbar_label = plot_options.get("cbar_label", None)
        if self.cbar_label is None:
            self.cbar_label = self.var + " in " + self.units
        self.mean_plot_title = plot_options.get(
            "mean_plot_title", f"Ensemble mean zonal average of {self.var}"
        )
        self.std_plot_title = plot_options.get(
            "std_plot_title", f"Ensemble standard deviation of zonal average of {self.var}"
        )

    def compute(self):
        """
        A function to compute the mean and standard devivation of the input dataset
        It is import to make sure that the dim along which the mean is compute is correct.
        The default dim="ensemble".
        """
        
        self.logger.info("Compute function in EnsembleZonal")
        
        if self.dataset is not None:
            self.dataset_mean, self.dataset_std = compute_statistics(
                variable=self.var, 
                ds=self.dataset, 
                ens_dim=self.dim,
                log_level=self.loglevel
            )
        else:
            self.logger.info("No ensemble data is provided to the compute method")
            raise NoDataError("No data is given to the compute method")

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

        create_folder(self.outputdir, self.loglevel)
        outfig = os.path.join(self.outputdir, "plots")
        self.logger.debug(f"Path to output diectory for plots: {outfig}")
        create_folder(outfig, self.loglevel)

        if isinstance(self.dataset_mean, xr.Dataset):
            self.dataset_mean = self.dataset_mean[self.var]
        else:
            self.dataset_mean = self.dataset_mean
        self.logger.info("Plotting ensemble-mean Zonal-average")
        fig1 = plt.figure(figsize=(self.figure_size[0], self.figure_size[1]))
        ax1 = fig1.add_subplot(1, 1, 1)
        im = ax1.contourf(
            self.dataset_mean.lat,
            self.dataset_mean.lev,
            self.dataset_mean,
            cmap="RdBu_r",
            levels=20,
            extend="both",
        )
        ax1.set_ylim((5500, 0))
        ax1.set_ylabel("Depth (in m)", fontsize=9)
        ax1.set_xlabel("Latitude (in deg North)", fontsize=9)
        ax1.set_facecolor("grey")
        ax1.set_title(self.mean_plot_title)
        cbar = fig1.colorbar(im, ax=ax1, shrink=0.9, extend="both")
        cbar.set_label(self.cbar_label)
        self.logger.debug(
            f"Saving Lev-Lon Zonal-average ensemble-mean as pdf and png to {outfig}"
        )
        # Lev-Lon Zonal mean plot in pdf
        fig1.savefig(
            os.path.join(outfig, self.var + "_LevLon_mean.pdf"),
            bbox_inches="tight",
            pad_inches=0.1,
            dpi=self.dpi,
        )
        # Lev-Lon Zonal mean plot in png
        fig1.savefig(
            os.path.join(outfig, self.var + "_LevLon_mean.png"),
            bbox_inches="tight",
            pad_inches=0.1,
            dpi=self.dpi,
        )

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
            cmap="OrRd",
            levels=20,
            extend="both",
        )
        ax2.set_ylim((5500, 0))
        ax2.set_ylabel("Depth (in m)", fontsize=9)
        ax2.set_xlabel("Latitude (in deg North)", fontsize=9)
        ax2.set_facecolor("grey")
        ax2.set_title(self.std_plot_title)
        cbar = fig2.colorbar(im, ax=ax2, shrink=0.9, extend="both")
        cbar.set_label(self.cbar_label)
        self.logger.debug(
            f"Saving Lev-Lon Zonal-average ensemble-STD as pdf and png to {outfig}"
        )
        # Lev-Lon Zonal STD plot in pdf
        fig2.savefig(
            os.path.join(outfig, self.var + "_LevLon_STD.pdf"),
            bbox_inches="tight",
            pad_inches=0.1,
            dpi=self.dpi,
        )
        # Lev-Lon Zonal STD plot in png
        fig2.savefig(
            os.path.join(outfig, self.var + "_LevLon_STD.png"),
            bbox_inches="tight",
            pad_inches=0.1,
            dpi=self.dpi,
        )

        return {'mean_plot': [fig1, ax1], 'std_plot': [fig2, ax2]}


