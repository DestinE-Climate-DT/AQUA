import matplotlib.pyplot as plt
import xarray as xr
import cartopy.crs as ccrs
from aqua.graphics import plot_single_map
from aqua.exceptions import NoDataError
from .base import BaseMixin

xr.set_options(keep_attrs=True)


class PlotEnsembleLatLon(BaseMixin):
    """Class to plot the ensmeble lat-lon"""
 
    # TODO: support sub-region selection and reggriding option

    def __init__(
        self,
        diagnostic_product: str = "EnsembleLatLon",
        catalog_list: list[str] = None,
        model_list: list[str] = None,
        exp_list: list[str] = None,
        source_list: list[str] = None,
        region: str = None,
        save_pdf=True,
        save_png=True,
        var: str = None,
        dataset_mean=None,
        dataset_std=None,
        description=None,
        dpi=300,
        title_mean=None,
        title_std=None,
        vmin_mean=None,
        vmax_mean=None,
        vmin_std=None,
        vmax_std=None,
        proj=ccrs.PlateCarree(),
        transform_first=False,
        cyclic_lon=False,
        contour=True,
        coastlines=True,
        cbar_label=None,
        units=None,
        outputdir="./",
        log_level: str = "WARNING",
    ):
        """
        Args:
            var (str): Variable name.
            diagnostic_name (str): The name of the diagnostic. Default is 'ensemble'.
                                   This will be used to configure the logger and the output files.
            catalog_list (str): This variable defines the catalog list. The default is 'None'. 
                                    If None, the variable is assigned to 'None_catalog'. In case of Multi-catalogs, 
                                    the variable is assigned to 'multi-catalog'.
            model_list (str): This variable defines the model list. The default is 'None'. 
                                    If None, the variable is assigned to 'None_model'. In case of Multi-Model, 
                                    the variable is assigned to 'multi-model'.
            exp_list (str): This variable defines the exp list. The default is 'None'. 
                                    If None, the variable is assigned to 'None_exp'. In case of Multi-Exp, 
                                    the variable is assigned to 'multi-exp'.
            source_list (str): This variable defines the source list. The default is 'None'. 
                                    If None, the variable is assigned to 'None_source'. In case of Multi-Source, 
                                    the variable is assigned to 'multi-source'.
            ensemble_dimension_name="ensemble" (str): a default name given to the
                     dimensions along with the individual Datasets were concatenated.
            data_mean: xarray.Dataset timeseries monthly mean 
            data_std: xarray.Dataset timeseries monthly std
            outputdir (str): String input for output path.
            save_pdf (bool): Default is True.
            save_png (bool): Default is True.
            dpi (int): Default is 300.
            title (str): Title for plot.
            description (str): specific for saving the plot.
            loglevel (str): Log level. Default is "WARNING".
        """
        
        self.diagnostic_product = diagnostic_product
        self.catalog_list = catalog_list
        self.model_list = model_list
        self.exp_list = exp_list
        self.source_list = source_list
        self.region = region
        self.var = var
        self.save_pdf = save_pdf
        self.save_png = save_png

        self.dataset_mean = dataset_mean
        self.dataset_std = dataset_std

        self.outputdir = outputdir 
        self.log_level = log_level

        self.figure = None
        self.dpi = dpi

        # TODO: include in the config file
        # Specific for colorbars is mean and std plots
        self.vmin_mean = vmin_mean
        self.vmax_mean = vmax_mean
        self.vmin_std = vmin_std
        self.vmax_std = vmax_std

        self.proj = proj
        self.transform_first = transform_first
        self.cyclic_lon = cyclic_lon
        self.contour = contour
        self.coastlines = coastlines

        self.units = units
        if self.units is None:
            self.units = self.dataset_mean.units
            #self.units = self.dataset_mean[self.var].units
        self.cbar_label = cbar_label
        if self.cbar_label is None:
            self.cbar_label = self.var + " in " + self.units

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
        
        Returns:
            a dict of fig and ax for mean and STD
            return {'mean_plot': [fig1, ax1], 'std_plot': [fig2, ax2]}
        """

        self.logger.info("Plotting the ensemble computation")
        if (self.dataset_mean is None) or (self.dataset_std is None):
            raise NoDataError("No data given to the plotting function")

        # mean plot
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
            title=self.title_mean,
            vmin=self.vmin_mean,
            vmax=self.vmax_mean,
        )
        ax1.set_xlabel("Longitude")
        ax1.set_ylabel("Latitude")
        self.logger.debug(f"Saving 2D map of mean")

        # STD plot
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
            title=self.title_std,
            vmin=self.vmin_std,
            vmax=self.vmax_std,
        )
        ax2.set_xlabel("Longitude")
        ax2.set_ylabel("Latitude")

        # Saving plots
        if self.save_png:
            self.save_figure(var=self.var, fig=fig1, fig_std=fig2,  description=self.description, format='png')
        if self.save_pdf:
            self.save_figure(var=self.var, fig=fig1, fig_std=fig2, description=self.description, format='pdf')

        return {'mean_plot': [fig1, ax1], 'std_plot': [fig2, ax2]}


