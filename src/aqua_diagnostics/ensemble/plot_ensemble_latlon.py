import matplotlib.pyplot as plt
import xarray as xr
import cartopy.crs as ccrs
from aqua.graphics import plot_single_map
from aqua.exceptions import NoDataError
from .base import BaseMixin
from aqua.util import get_projection

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
        outputdir="./",
        loglevel: str = "WARNING",
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
            loglevel (str): Log level. Default is "WARNING".
        """
        
        self.diagnostic_product = diagnostic_product
        self.catalog_list = catalog_list
        self.model_list = model_list
        self.exp_list = exp_list
        self.source_list = source_list
        self.region = region

        self.outputdir = outputdir 
        self.loglevel = loglevel

        self.figure = None

        super().__init__(
            loglevel=self.loglevel,
            diagnostic_product=self.diagnostic_product,
            catalog_list=self.catalog_list,
            model_list=self.model_list,
            exp_list=self.exp_list,
            source_list=self.source_list,
            outputdir=self.outputdir,
        )

    def plot(self, var: str = None, dataset_mean=None, dataset_std=None, long_name= None, description=None, dpi=300, title_mean=None, title_std=None, save_pdf=True, save_png=True, vmin_mean=None, vmax_mean=None, vmin_std=None, vmax_std=None, proj='robinson', proj_params={}, transform_first=False, cyclic_lon=False, contour=True, coastlines=True, cbar_label=None, units=None):
        """
        Args:
            var (str): Variable name.
            diagnostic_name (str): The name of the diagnostic. Default is 'ensemble'.
                                   This will be used to configure the logger and the output files.
            save_pdf (bool): Default is True.
            save_png (bool): Default is True.
            dpi (int): Default is 300.
            title_mean (str): Title for plot mean plot.
            title_std (str): Title for plot std plot.
            description (str): specific for saving the plot.

        This plots the ensemble mean and standard deviation of the ensemble statistics.
        
        Returns:
            a dict of fig and ax for mean and STD
            return {'mean_plot': [fig1, ax1], 'std_plot': [fig2, ax2]}
        """

        self.logger.info("Plotting the ensemble computation")
        if (dataset_mean is None) or (dataset_std is None):
            raise NoDataError("No data given to the plotting function")
        if units is None:
            units = dataset_mean.attrs.get("units", None)
            #units = dataset_mean[var].units
        if cbar_label is None and units is not None:
            cbar_label = var + " in " + units

        if isinstance(self.model, list):
            model_str = " ".join(str(x) for x in self.model)
        else:
            model_str = str(self.model)
        if long_name is None: 
            long_name = dataset_mean.attrs.get("long_name", None)
            if long_name is None: long_name = var
        if units is not None:
            if title_mean is None: title_mean = "Ensemble mean of " + model_str + " for " + long_name + " " + units 
            if title_std is None: title_std = "Ensemble standard deviation of " + model_str + " for " + long_name + " " + units
        else:
            if title_mean is None: title_mean = "Ensemble mean of " + model_str + " for " + long_name
            if title_std is None: title_std = "Ensemble standard deviation of " + model_str + " for " + long_name

        proj = get_projection(proj, **proj_params)

        # mean plot
        if isinstance(dataset_mean, xr.Dataset):
            dataset_mean = dataset_mean[var]
        else:
            dataset_mean = dataset_mean
        if vmin_mean is None:
            vmin_mean = dataset_mean.values.min()
        if vmax_mean is None:
            vmax_mean = dataset_mean.values.max()
        fig1, ax1 = plot_single_map(
            dataset_mean,
            proj=proj,
            proj_params=proj_params,
            contour=contour,
            cyclic_lon=cyclic_lon,
            coastlines=coastlines,
            #transform_first=transform_first,
            return_fig=True,
            title=title_mean,
            vmin=vmin_mean,
            vmax=vmax_mean,
        )
        ax1.set_xlabel("Longitude")
        ax1.set_ylabel("Latitude")
        self.logger.debug(f"Saving 2D map of mean")

        # STD plot
        if isinstance(dataset_std, xr.Dataset):
            dataset_std = dataset_std[var]
        else:
            dataset_std = dataset_std
        if vmin_std is None:
            vmin_std = dataset_std.values.min()
        if vmax_std is None:
            vmax_std = dataset_std.values.max()
        if vmin_std == vmax_std:
            self.logger.info("STD is Zero everywhere")
            return {'mean_plot': [fig1, ax1]}
        fig2, ax2 = plot_single_map(
            dataset_std,
            proj=proj,
            proj_params=proj_params,
            contour=contour,
            cyclic_lon=cyclic_lon,
            coastlines=coastlines,
            #transform_first=transform_first,
            return_fig=True,
            title=title_std,
            vmin=vmin_std,
            vmax=vmax_std,
        )
        ax2.set_xlabel("Longitude")
        ax2.set_ylabel("Latitude")

        # Saving plots
        if save_png:
            self.save_figure(var=var, fig=fig1, fig_std=fig2, description=description, format='png')
        if save_pdf:
            self.save_figure(var=var, fig=fig1, fig_std=fig2, description=description, format='pdf')
        return {'mean_plot': [fig1, ax1], 'std_plot': [fig2, ax2]}
