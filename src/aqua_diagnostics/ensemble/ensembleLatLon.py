import os
import xarray as xr
import cartopy.crs as ccrs
# from dask import compute
from aqua.graphics import plot_single_map
from aqua.logger import log_configure
from aqua.util import create_folder
from aqua.exceptions import NoDataError
from .util import compute_statistics

xr.set_options(keep_attrs=True)


class EnsembleLatLon:
    """
    A class to compute ensemble mean and standard deviation of a 2D (lon-lat) Dataset.
    Make sure that the dataset has correct lon-lat dimensions.
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
            dataset: xarray Dataset composed of ensembles 2D lon-lat data, i.e.,
                     the individual Dataset (lon-lat) are concatenated along.
                     a new dimension "ensemble". This ensemble name can be changed.
            ensemble_dimension_name="ensemble" (str): a default name given to the
                     dimensions along with the individual Datasets were concatenated.
            outputdir (str): String input for output path.
            loglevel (str): Log level. Default is "WARNING".
            **kwargs (dict): dictonary contains options for plots. Defaults values have been set already.
        """
        self.loglevel = loglevel
        self.logger = log_configure(log_level=self.loglevel, log_name="Ensemble LatLon")

        self.var = var
        self.dataset = dataset
        self.dataset_mean = None
        self.dataset_std = None
        self.dim = ensemble_dimension_name
        self.outputdir = outputdir + "Ensemble_LatLon"
        self.outputfile = f"global_2D_map_{self.var}"

        self.figure = None
        plot_options = kwargs.get("plot_options", {})
        self.plot_label = plot_options.get("plot_label", True)
        self.plot_std = plot_options.get("plot_std", True)
        self.figure_size = plot_options.get("figure_size", [15, 15])
        self.dpi = plot_options.get("dpi", 600)
        self.draw_labels = plot_options.get("draw_labels", True)

        # TODO: mention is the config file
        # Specific for colorbars is mean and std plots
        self.vmin_mean = plot_options.get("vmin_mean", None)
        self.vmax_mean = plot_options.get("vmax_mean", None)
        self.vmin_std = plot_options.get("vmin_std", None)
        self.vmax_std = plot_options.get("vmax_std", None)

        self.proj = plot_options.get("projection", ccrs.PlateCarree())
        self.transform_first = plot_options.get("transform_first", False)
        self.cyclic_lon = plot_options.get("cyclic_lon", False)
        self.contour = plot_options.get("contour", True)
        self.coastlines = plot_options.get("coastlines", True)

        self.units = plot_options.get("units", None)
        if self.units is None:
            self.units = self.dataset[self.var].units
        self.cbar_label = plot_options.get("cbar_label", None)
        if self.cbar_label is None:
            self.cbar_label = self.var + " in " + self.units
        self.mean_plot_title = plot_options.get(
            "mean_plot_title", f"Map of {self.var} for Ensemble mean"
        )
        self.std_plot_title = plot_options.get(
            "std_plot_title", f"Map of {self.var} for Ensemble standard deviation"
        )

        # TODO: save the ensemble mean and the std values as netcdf files
        # self.netcdf_save = True

    def compute(self):
        """
        A function to compute the mean and standard devivation of the input dataset
        It is import to make sure that the dim along which the mean is compute is correct.
        The default dim="ensemble". This can be reassianged with the method "edit_attributes".
        """
        self.logger.info("Compute function in EnsembleLatLon")
        
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
        
        Returns:
            a dict of fig and ax for mean and STD
            return {'mean_plot': [fig1, ax1], 'std_plot': [fig2, ax2]}
        """

        self.logger.info("Plotting the ensemble computation")
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
        if self.vmin_mean is None:
            self.vmin_mean = self.dataset_mean.values.min()
        if self.vmax_mean is None:
            self.vmax_mean = self.dataset_mean.values.max()
        fig1, ax1 = plot_single_map(
            self.dataset_mean,
            proj=self.proj,
            contour=self.contour,
            cyclic_lon=self.cyclic_lon,
            coastlines=self.coastlines,
            transform_first=self.transform_first,
            return_fig=True,
            title=self.mean_plot_title,
            vmin=self.vmin_mean,
            vmax=self.vmax_mean,
        )
        ax1.set_xlabel("Longitude")
        ax1.set_ylabel("Latitude")
        self.logger.debug(f"Saving 2D map of mean to {outfig}")
        # 2D mean plot in pdf
        fig1.savefig(
            os.path.join(outfig, self.var + "_LatLon_mean.pdf"),
            bbox_inches="tight",
            pad_inches=0.1,
            dpi=self.dpi,
        )
        # 2D mean plots in png
        fig1.savefig(
            os.path.join(outfig, self.var + "_LatLon_mean.png"),
            bbox_inches="tight",
            pad_inches=0.1,
            dpi=self.dpi,
        )

        if isinstance(self.dataset_std, xr.Dataset):
            self.dataset_std = self.dataset_std[self.var]
        else:
            self.dataset_std = self.dataset_std
        if self.vmin_std is None:
            self.vmin_std = self.dataset_std.values.min()
        if self.vmax_std is None:
            self.vmax_std = self.dataset_std.values.max()
        if self.vmin_std == self.vmax_std:
            self.logger.info("STD is Zero everywhere")
            return {'mean_plot': [fig1, ax1]}
        fig2, ax2 = plot_single_map(
            self.dataset_std,
            proj=self.proj,
            contour=self.contour,
            cyclic_lon=self.cyclic_lon,
            coastlines=self.coastlines,
            transform_first=self.transform_first,
            return_fig=True,
            title=self.std_plot_title,
            vmin=self.vmin_std,
            vmax=self.vmax_std,
        )
        ax2.set_xlabel("Longitude")
        ax2.set_ylabel("Latitude")
        self.logger.debug(f"Saving 2D map of STD to {outfig}")
        # 2D STD plot in pdf
        fig2.savefig(
            os.path.join(outfig, self.var + "_LatLon_STD.pdf"),
            bbox_inches="tight",
            pad_inches=0.1,
            dpi=self.dpi,
        )
        # 2D STD plots in png
        fig2.savefig(
            os.path.join(outfig, self.var + "_LatLon_STD.png"),
            bbox_inches="tight",
            pad_inches=0.1,
            dpi=self.dpi,
        )
        return {'mean_plot': [fig1, ax1], 'std_plot': [fig2, ax2]}


