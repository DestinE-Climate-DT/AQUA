import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import xarray as xr
from aqua.exceptions import NoDataError
from aqua.graphics import plot_single_map
from aqua.util import get_projection

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
        outputdir="./",
        loglevel: str = "WARNING",
    ):
        """
        Class for plotting ensemble latitude-longitude (Lat-Lon) data.

        This class inherits from `BaseMixin` and provides functionality to generate
        plots of ensemble datasets on a latitude-longitude grid. It supports
        multiple catalogs, models, experiments, and sources, and allows saving
        plots as PNG or PDF files. The class is intended for ensemble statistics
        visualization, such as mean and standard deviation maps.

        Args:
            diagnostic_product (str, optional): Name of the diagnostic product.
                Defaults to "EnsembleLatLon".
            catalog_list (list[str], optional): List of catalog names. If None, assigned to 'None_catalog'.
            model_list (list[str], optional): List of model names. If None, assigned to 'None_model'.
            exp_list (list[str], optional): List of experiment names. If None, assigned to 'None_exp'.
            source_list (list[str], optional): List of data source names. If None, assigned to 'None_source'.
            region (str, optional): Name of the region for plotting. Defaults to None.
            outputdir (str, optional): Directory to save output plots. Defaults to "./".
            loglevel (str, optional): Logging level. Defaults to "WARNING".

        Attributes:
            figure (matplotlib.figure.Figure or None): The figure object for the plot.
            diagnostic_product (str): Name of the diagnostic product being visualized.
            catalog_list (list[str]): List of catalogs being processed.
            model_list (list[str]): List of models being processed.
            exp_list (list[str]): List of experiments being processed.
            source_list (list[str]): List of sources being processed.
            region (str): Region name for plotting.
            outputdir (str): Directory path for saving plots.
            loglevel (str): Logging level for messages.

        Notes:
            - Designed to visualize ensemble mean and standard deviation on Lat-Lon grids.
            - Integrates with `BaseMixin` for consistent handling of catalogs, models, and experiments.
            - Uses `self.save_figure` for saving output plots in PNG and PDF formats.

        TODO:
            - Support sub-region selection for plotting.
            - Add regridding option for datasets with different grids.
            - Include automatic handling of color scales and legends for multiple ensemble members.
            - Add methods to overlay observations or reference datasets.
            - Enable interactive plotting for enhanced analysis.
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

    def plot(
        self,
        var: str = None,
        dataset_mean=None,
        dataset_std=None,
        long_name=None,
        description=None,
        dpi=300,
        title_mean=None,
        title_std=None,
        save_pdf=True,
        save_png=True,
        vmin_mean=None,
        vmax_mean=None,
        vmin_std=None,
        vmax_std=None,
        proj="robinson",
        proj_params={},
        transform_first=False,
        cyclic_lon=True,
        contour=True,
        coastlines=True,
        cbar_label=None,
        units=None,
    ):
        """
        Plot ensemble mean and standard deviation on a latitude-longitude map.

        Generates 2D maps of ensemble mean and standard deviation for a given
        variable using the specified projection and visualization options.
        The resulting figures can be saved as PNG and/or PDF files.

        Args:
            var (str): Variable name to plot.
            dataset_mean (xarray.DataArray or Dataset): Ensemble mean dataset.
            dataset_std (xarray.DataArray or Dataset): Ensemble standard deviation dataset.
            long_name (str, optional): Long descriptive name for the variable. Defaults to None.
            description (str, optional): Description string for saving the plot. Defaults to None.
            dpi (int, optional): Resolution for saved figures. Default is 300.
            title_mean (str, optional): Title for mean plot. Auto-generated if None.
            title_std (str, optional): Title for standard deviation plot. Auto-generated if None.
            save_pdf (bool, optional): Whether to save figures as PDF. Default is True.
            save_png (bool, optional): Whether to save figures as PNG. Default is True.
            vmin_mean, vmax_mean (float, optional): Color scale limits for mean plot. Auto-set if None.
            vmin_std, vmax_std (float, optional): Color scale limits for std plot. Auto-set if None.
            proj (str, optional): Map projection. Default is "robinson".
            proj_params (dict, optional): Extra parameters for the projection. Defaults to {}.
            transform_first (bool, optional): Whether to transform data before plotting. Default is False.
            cyclic_lon (bool, optional): Whether longitude is cyclic. Default is False.
            contour (bool, optional): Overlay contours. Default is True.
            coastlines (bool, optional): Draw coastlines. Default is True.
            cbar_label (str, optional): Label for the colorbar. Auto-generated if None.
            units (str, optional): Units of the variable. Used for titles and labels.

        Returns:
            dict: Dictionary containing figure and axes for mean and std plots:
                  {'mean_plot': [fig1, ax1], 'std_plot': [fig2, ax2]}. 
                  If standard deviation is zero everywhere, only 'mean_plot' is returned.

        Raises:
            NoDataError: If `dataset_mean` or `dataset_std` is None.

        Notes:
            - Titles and colorbar labels are automatically generated if not provided.
            - Uses `self.save_figure` to save PNG and PDF files.
            - Handles both xarray.DataArray and Dataset inputs.
            - If vmin_std equals vmax_std, std plot is skipped.

        TODO:
            - Add support for plotting multiple variables in one call.
            - Overlay observational or reference datasets.
            - Enable interactive plotting with cartopy or matplotlib widgets.
            - Improve handling of cyclic longitude for global datasets.
        """
        self.logger.info("Plotting the ensemble computation")
        if (dataset_mean is None) or (dataset_std is None):
            raise NoDataError("No data given to the plotting function")
        if units is None:
            units = dataset_mean.attrs.get("units", None)
            # units = dataset_mean[var].units
        if cbar_label is None and units is not None:
            cbar_label = var + " in " + units

        if isinstance(self.model, list):
            model_str = " ".join(str(x) for x in self.model)
        else:
            model_str = str(self.model)
        if long_name is None:
            long_name = dataset_mean.attrs.get("long_name", None)
            if long_name is None:
                long_name = var
        if units is not None:
            if title_mean is None:
                title_mean = "Ensemble mean of " + model_str + " for " + long_name + " " + units
            if title_std is None:
                title_std = "Ensemble standard deviation of " + model_str + " for " + long_name + " " + units
        else:
            if title_mean is None:
                title_mean = "Ensemble mean of " + model_str + " for " + long_name
            if title_std is None:
                title_std = "Ensemble standard deviation of " + model_str + " for " + long_name

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
            # transform_first=transform_first,
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
            return {"mean_plot": [fig1, ax1]}
        fig2, ax2 = plot_single_map(
            dataset_std,
            proj=proj,
            proj_params=proj_params,
            contour=contour,
            cyclic_lon=cyclic_lon,
            coastlines=coastlines,
            # transform_first=transform_first,
            return_fig=True,
            title=title_std,
            vmin=vmin_std,
            vmax=vmax_std,
        )
        ax2.set_xlabel("Longitude")
        ax2.set_ylabel("Latitude")

        # Saving plots
        if save_png:
            self.save_figure(var=var, fig=fig1, fig_std=fig2, description=description, format="png")
        if save_pdf:
            self.save_figure(var=var, fig=fig1, fig_std=fig2, description=description, format="pdf")
        return {"mean_plot": [fig1, ax1], "std_plot": [fig2, ax2]}
