""" PlotSeaIce doc """
import os
import xarray as xr
import cartopy.crs as ccrs
import matplotlib.colors as mcolors
from matplotlib import pyplot as plt
from aqua.diagnostics.core import OutputSaver
from aqua.graphics import plot_single_map, plot_single_map_diff, plot_maps
from aqua.logger import log_configure, log_history
from aqua.configurer import ConfigPath
from aqua.util import get_projection, plot_box, to_list, get_realizations
from aqua.util import evaluate_colorbar_limits, set_map_title, time_to_string
from aqua.util import generate_colorbar_ticks, int_month_name, apply_circular_window
from .util import extract_dates, _check_list_regions_type

xr.set_options(keep_attrs=True)

class Plot2DSeaIce:
    """
    A class for processing and visualizing surface maps and biases of sea ice fraction or thickness.

    Args:
        ref (xarray.DataArray or xarray.Dataset): Reference sea ice data.
        models (list of xarray.DataArray or xarray.Dataset): List of models with sea ice data.
        regions_to_plot (list): List of strings with the region names to plot which must match 
                                the 'AQUA_region' attribute in the data provided as input.
        outputdir (str): Output directory for saving plots.
        rebuild (bool): Whether to rebuild the plots if they already exist.
        dpi (int): Dots per inch for the saved figures.
        loglevel (str): Logging level for the logger. Default is 'WARNING'.
    """
    def __init__(self,
                 ref=None, models=None, 
                 regions_to_plot: list = ['Arctic', 'Antarctic'],
                 outputdir='./',
                 rebuild=True,
                 dpi=300, 
                 loglevel='WARNING'):

        self.loglevel = loglevel
        self.logger = log_configure(log_level=self.loglevel, log_name='Plot2DSeaIce')

        self.realizations = get_realizations(models)

        self.ref = self._handle_data(ref)
        self.models = self._handle_data(models)
        
        self.regions_to_plot = _check_list_regions_type(regions_to_plot, logger=self.logger)

        if self.regions_to_plot is None:
            self._detect_common_regions([self.models, self.ref])

        self.outputdir = outputdir
        self.rebuild = rebuild
        self.dpi = dpi

    def plot_2d_seaice(self, plot_type='var', months=[3,9], method='fraction', projkw=None,
                       plot_ref_contour=False, save_pdf=True, save_png=True, **kwargs):
        """
        Plot sea ice data and biases.

        Args:
            plot_type (str): Type of plot to generate ['var' or 'bias'].
            months (list):  List of months to plot, e.g. [2, 9] for February and September.
            projkw (dict):  Dictionary with projection parameters for the plot.
            save_pdf (bool): Whether to save the plot as a PDF.
            save_png (bool): Whether to save the plot as a PNG.
            plot_ref_contour (bool):     Whether to add a reference line at 0.2 for sea ice fraction.
            **kwargs: Additional keyword arguments for customization. See below functions for details.
        """
        self.logger.info("Starting Plot2DSeaIce run")

        if not all(1 <= m <= 12 for m in months):
            raise ValueError("Invalid month value. Months must be between 1 and 12.")
        self.months = months

        self.plot_type = plot_type
        self.save_pdf = save_pdf
        self.save_png = save_png

        self.method = method
        supported_methods = ['fraction', 'thickness']
        if self.method not in supported_methods:
            raise ValueError(f"Unsupported method '{method}'. Supported methods are {supported_methods}.")

        self.projname = projkw.get('projname', 'unknown')
        self.projpars = projkw.get('projpars', {})
        self.extent_regions = projkw.get('extent_regions', {})

        self.plot_ref_contour = plot_ref_contour

        if not self.models or not self.ref:
            raise ValueError("Missing models or reference data")

        for region in self.regions_to_plot:
            self.logger.info(f"Plotting region: {region}")

            if not self._find_data_for_region(region):
                continue  

            if plot_type == 'bias':
                self._plot_bias_map(region, **kwargs)
            elif plot_type == 'var':
                self._plot_var_map(region, **kwargs)
            else:
                raise ValueError(f"Unsupported plot_type '{plot_type}'. Supported: ['var', 'bias']")

    def _plot_bias_map(self, region, **kwargs):
        """
        Plot sea ice variable biases (e.g. fraction or thickness).

        Args:
            **kwargs: Additional keyword arguments for customization. Supported kwargs include:
                bias_vmin_vmax (dict): Dictionary with 'vmin' and 'vmax' for bias maps.
                cbar_ticks_rounding (int): Rounding for colorbar ticks.
        """
        ticks_rounding = kwargs.get('cbar_ticks_rounding', 1)
        bias_vmin_vmax = kwargs.get('bias_vmin_vmax', None)
        add_land = kwargs.get('add_land', True)

        if not self.reg_ref or not self.reg_models:
            self.logger.error(f"Missing data to plot biases. Ensure both models and ref data are available. Skipping {region}")
            return

        reg_ref = self.reg_ref[0]
        reg_models = [da for da in self.reg_models if da is not None]

        self.proj = get_projection(self.projname, **self._set_projpars())

        for reg_mod in reg_models:

            nrows, ncols = len(self.months), 3
            fig = plt.figure(figsize=(ncols * 4.8, nrows * 4.5))
            subfigs = fig.subfigures(nrows=nrows, ncols=1)

            for jmon, (month, subfig) in enumerate(zip(self.months, subfigs)):

                monref = self._mask_ice_at_mid_lats(reg_ref.sel(month=month))
                monmod = self._mask_ice_at_mid_lats(reg_mod.sel(month=month))

                axs = subfig.subplots(1, 3, subplot_kw={'projection': self.proj})

                subfig.suptitle(f"{set_map_title(reg_ref, put_model_name=False, put_exp_name=False)}. "
                                f"Month: {int_month_name(month)}", fontsize=14, y=1.02)

                # plot ref
                setup = self._get_cmap(monref)

                plot_single_map(monref, proj=self.proj, fig=fig, ax=axs[0],
                                cmap=setup['colormap'], norm=setup['norm'],
                                contour=False, cbar=False, add_land=add_land,
                                loglevel=self.loglevel,
                                **kwargs)

                cbar_ref = self._add_colorbar(fig, monref, ax=axs[0], orientation='vertical',
                                              norm=setup['norm'], boundaries=setup['boundaries'],
                                              ticks_rounding=ticks_rounding, 
                                              **{'fraction': 0.046, 'pad': 0.04})
                axs[0].set_title(f"{set_map_title(monref, skip_varname=True)}")

                # plot model
                setup = self._get_cmap(monmod)

                plot_single_map(monmod, proj=self.proj, fig=fig, ax=axs[1],
                                cmap=setup['colormap'], norm=setup['norm'],
                                contour=False, cbar=False, add_land=add_land,
                                loglevel=self.loglevel,
                                **kwargs)

                cbar_ref = self._add_colorbar(fig, monmod, ax=axs[1], orientation='vertical',
                                              norm=setup['norm'], boundaries=setup['boundaries'],
                                              ticks_rounding=ticks_rounding,
                                              **{'fraction': 0.046, 'pad': 0.04})

                if self.plot_ref_contour:
                    self._plot_reference_contour(ax=axs[1], month=month, data_type='model', **kwargs)

                axs[1].set_title(f"{set_map_title(monmod, skip_varname=True)}")

                # plot bias
                vmin_vmax_map = {'fraction': (-1, 1), 'thickness': (-5, 5)}
                default_vmin, default_vmax = vmin_vmax_map.get(self.method, (-1, 1))

                bias_vmin_vmax = kwargs.get('bias_vmin_vmax', {})
                vmin = bias_vmin_vmax.get('vmin', default_vmin)
                vmax = bias_vmin_vmax.get('vmax', default_vmax)

                plot_single_map_diff(monmod, monref, proj=self.proj, fig=fig, ax=axs[2],
                                     add_contour=False, add_land=add_land,
                                     nlevels=26,
                                     vmin_fill=vmin, vmax_fill=vmax,
                                     sym=False, # set False to later override with symmetric min-max values
                                     cbar=False, loglevel=self.loglevel,
                                     **kwargs)

                cbar_diff = self._add_colorbar(fig, monref, ax=axs[2], orientation='vertical',
                                               vmin=vmin, vmax=vmax, sym=True,
                                               ticks_rounding=ticks_rounding,
                                               **{'fraction': 0.046, 'pad': 0.04})

                axs[2].set_title(f"{set_map_title(monmod, skip_varname=True, put_model_name=True, put_exp_name=False)} - "
                                 f"{set_map_title(monref, skip_varname=True, put_model_name=True, put_exp_name=False)}")

                if self.extent_regions:
                    ext_coords = self.extent_regions.get(region, None)
                    for ax in axs:
                        apply_circular_window(ax, extent=ext_coords)

        description = (
            f"Spatial map and total bias of the sea ice {monmod.attrs.get('AQUA_method', '')} climatology "
            f"in the {monmod.attrs.get('AQUA_region', 'geographic')} region. "
            f"The model data is {monmod.attrs.get('AQUA_model')} with experiment {monmod.attrs.get('AQUA_exp')} "
            f"spanning from {time_to_string(monmod.attrs.get('AQUA_startdate', ''))} to {time_to_string(monmod.attrs.get('AQUA_enddate', ''))}. "
            f"The reference dataset is {monref.attrs.get('AQUA_model')} with experiment {monref.attrs.get('AQUA_exp')} "
            f"spanning from {time_to_string(monref.attrs.get('AQUA_startdate', ''))} to {time_to_string(monref.attrs.get('AQUA_enddate', ''))}. "
            f"{'The red contour line represents the regional sea ice fraction equal to 0.2.' if self.method == 'fraction' else ''}"
            )
        self._save_plots(fig=fig, data=monmod, data_ref=monref, diagnostic_product='bias', 
                         description=description, extra_keys={'method': self.method, 'region': region})
        plt.close(fig)

    def _plot_var_map(self, region, **kwargs):
        """
        Plot monthly climatological sea ice variable only (e.g. fraction or thickness).
        """
        self.proj = get_projection(self.projname, **self._set_projpars())

        if self.reg_ref:
            for datarr in self.reg_ref:
                if datarr is not None:
                    self._plot_single_dataset(datarr, region, 'reference', **kwargs)

        if self.reg_models:
            for datarr in self.reg_models:
                if datarr is not None:
                    self._plot_single_dataset(datarr, region, 'model', **kwargs)

    def _plot_single_dataset(self, datarr, region, data_type, **kwargs):
        """
        Plot a single dataset (reference or model).
        
        Args:
            datarr: The data array to plot
            data_type: 'reference' or 'model'
            **kwargs: Additional plotting arguments
        """
        self.logger.info(f"Processing {data_type} data: {datarr.name}")

        nrows, ncols = plot_box(num_plots=len(self.months))
        fig = plt.figure(figsize=(ncols * 4.5, nrows * 4))
        fig.subplots_adjust(bottom=0.2, hspace=0.9)

        setup = self._get_cmap(datarr)
        cmap, norm, boundaries = (setup[k] for k in ('colormap', 'norm', 'boundaries'))

        for jm, month in enumerate(self.months):
            mondat = self._mask_ice_at_mid_lats(datarr.sel(month=month))

            fig, ax = plot_single_map(mondat, proj=self.proj, fig=fig,
                                      cmap=cmap, norm=norm, 
                                      add_land=True, contour=False, 
                                      cbar=False, return_fig=True,
                                      loglevel=self.loglevel, ax_pos=(nrows, ncols, jm+1), 
                                      **kwargs)
            
            if self.plot_ref_contour and data_type == 'model':
                self._plot_reference_contour(ax=ax, month=month, data_type=data_type, **kwargs)

            if self.extent_regions:
                ext_coords = self.extent_regions.get(region, None)
                ax = apply_circular_window(ax, extent=ext_coords)

            ax.set_title(f"Month: {int_month_name(month)}", fontsize=12)
                            
        # Adjust the location of the subplots on the page to make room for the colorbar
        fig.subplots_adjust(bottom=0.25, top=0.8, left=0.15, right=0.85, wspace=0.03, hspace=0.5)
        cbar_ax = fig.add_axes([0.2, 0.15, 0.6, 0.03])

        cbar = self._add_colorbar(fig, mondat, ax=ax, cax=cbar_ax,
                                  orientation='horizontal',
                                  ticks_rounding=kwargs.get('cbar_ticks_rounding', 1),
                                  **{'shrink': 0.3, 'pad': 0.07})

        fig.suptitle(f"{set_map_title(datarr)}", fontsize=13)
        
        description = (
            f"Spatial map of the sea ice {mondat.attrs.get('AQUA_method','')} climatology "
            f"for the {mondat.attrs.get('AQUA_model','')} model, experiment {mondat.attrs.get('AQUA_exp','')} "
            f"over {mondat.attrs.get('AQUA_region', 'geographic')} region "
            f"from {time_to_string(mondat.attrs.get('AQUA_startdate',''))} to {time_to_string(mondat.attrs.get('AQUA_enddate',''))}. "
            f"{'The red contour line represent the regional sea ice fraction equal to 0.2.' if self.method == 'fraction' and self.plot_ref_contour else ''}"
        )
        self._save_plots(fig=fig, data=mondat, data_ref=None, 
                         diagnostic_product='varmap', description=description, extra_keys={'method': self.method, 'region': region})
        plt.close(fig)
        
    def _get_colorbar_ticks(self, data, vmin=None, vmax=None, norm=None,
                            boundaries=None, sym=False, ticks_rounding=1):
        """
        Generate ticks for colorbar based on data range, normalization, or specified boundaries.

        Args:
            data (xarray.DataArray): DataArray containing the data for which to generate colorbar ticks.
            vmin (float, optional): Minimum value for the colorbar. If None, it will be calculated from data.
            vmax (float, optional): Maximum value for the colorbar. If None, it will be calculated from data.
            norm (matplotlib.colors.Normalize, optional): Normalization instance to use for the colorbar.
            boundaries (list, optional): List of boundaries for discrete normalization (can be non-linear discretisation).
            sym (bool, optional): If True, use symmetric limits for the colorbar.
            ticks_rounding (int, optional): Rounding for colorbar ticks.

        Returns:
            list: A list of ticks for the colorbar.
        """
        if norm is None:
            if vmin is None or vmax is None:
                vmin, vmax = evaluate_colorbar_limits(maps=[data], sym=sym)
            return generate_colorbar_ticks(vmin=vmin, vmax=vmax, sym=sym,
                                           nlevels=10, ticks_rounding=ticks_rounding)
        else:
            return boundaries[::2] + [boundaries[-1]]
    
    def _add_colorbar(self, fig, data, 
                      mappable=None, ax=None, cax=None, 
                      vmin=None, vmax=None, norm=None, boundaries=None, 
                      sym=False, orientation='horizontal', ticks_rounding=1, **cb_kwargs):
        """
        Add a colorbar to the current figure.

        Args:
            fig (matplotlib.Figure): The figure to which the colorbar will be added.
            data (xarray.DataArray): DataArray containing the data for which the colorbar is generated.
            mappable (matplotlib.cm.ScalarMappable, optional): The mappable object to which the colorbar applies. 
                                                               Defaults to the first collection in the axis.
            ax (matplotlib.axes.Axes, optional): The axis to which the colorbar is associated. Defaults to None.
            cax (matplotlib.axes.Axes, optional): The axis on which the colorbar is drawn. Defaults to None.
            vmin (float, optional): Minimum value for the colorbar. Defaults to None.
            vmax (float, optional): Maximum value for the colorbar. Defaults to None.
            norm (matplotlib.colors.Normalize, optional): Normalization instance to use for the colorbar. Defaults to None.
            boundaries (list, optional): List of boundaries for discrete normalization. Defaults to None.
            sym (bool, optional): If True, use symmetric limits for the colorbar. Defaults to False.
            orientation (str, optional): Orientation of the colorbar ('horizontal' or 'vertical'). Defaults to 'horizontal'.
            ticks_rounding (int, optional): Rounding for colorbar ticks. Defaults to 1.
            **cb_kwargs: Additional keyword arguments for the colorbar.

        Returns:
            matplotlib.colorbar.Colorbar: The created colorbar object.
        """
        if mappable is None:
            mappable = ax.collections[0]
        cbar_ticks = self._get_colorbar_ticks(data, vmin=vmin, vmax=vmax,
                                              norm=norm, boundaries=boundaries,
                                              sym=sym, ticks_rounding=ticks_rounding)
        cb = fig.colorbar(mappable, cax=cax, ax=ax,
                          orientation=orientation, **cb_kwargs)
        cb.set_ticks(cbar_ticks)
        cb.ax.ticklabel_format(style='sci', axis='x', scilimits=(-3, 3))
        units = data.attrs.get('units', '')
        if units and not (units.startswith('[') and units.endswith(']')):
            units = f'[{units}]'
        if units:
            units = ' ' + units
        cb.set_label(f"Sea-ice {data.attrs.get('AQUA_method', '')}{units}", fontsize=11)
        return cb

    def _get_cmap(self, datarr):
        """
        Get the personalised colormap for sea ice variable.
        Args:
            datarr (xarray.DataArray): DataArray containing sea ice data with attributes.
        Returns:
            dict: A dictionary containing the colormap, norm, and boundaries.
        """
        self.logger.debug(f"Using method '{self.method}' for colormap generation")

        if self.method == 'fraction':
            # Define a custom blue-to-white gradient colormap
            source_colors = [[0.15, 0.35, 0.55], [0.4, 0.7, 0.85], [0.5, 0.75, 0.9],
                             [0.6, 0.8, 0.95],   [0.7, 0.85, 1.0], [0.8, 0.9, 1.0],
                             [0.9, 0.95, 1.0],   [1.0, 1.0, 1.0]]
            colormap = mcolors.LinearSegmentedColormap.from_list('custom_fraction_colormap', source_colors, N=15)
            norm = None
            boundaries = None
        else:
            # Define boundaries for a discrete normalization using the 'turbo' colormap
            boundaries = [0, 0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5, 5,
                          5.5, 6, 6.5, 7, 7.5, 8.0, 10, 15, 20, 30]
            colormap = plt.get_cmap('turbo')
            norm = mcolors.BoundaryNorm(boundaries, ncolors=colormap.N, clip=True)

        return {'colormap': colormap, 'norm': norm, 'boundaries': boundaries}

    def _set_projpars(self):
        """
        Set projection parameters based on the provided projection name and additional keyword arguments.
        Each parameter can be defined either as a string referring to a registered function, or as a number.

        Returns:
            dict: A dictionary of computed projection parameters.
        """
        if not self.projpars:
            raise ValueError("Missing 'projpars' in 'projkw'. Please provide valid projection parameters as a dict.")

        regdata = self.reg_ref[0] if self.reg_ref else self.reg_models[0]
        
        function_registry = {
            "max_lat_signed": lambda data: max(data['lat'].values, key=abs)
        }
        processed_projpars = {}
        for key, fncall in self.projpars.items():
            if isinstance(fncall, str):
                if fncall in function_registry:
                    func = function_registry[fncall]
                    processed_projpars[key] = func(regdata)
                else:
                    self.logger.error(f"Function '{fncall}' not found in registry, "
                                      f"skipping projpars[{key}]. Consider updating projpars.")
            elif isinstance(fncall, (int, float)):
                processed_projpars[key] = fncall
            else:
                self.logger.error(f"Unsupported type for projpars[{key}]: {type(fncall)}, skipping")
        return processed_projpars

    def _plot_reference_contour(self, ax, month, data_type, **kwargs):
        """
        Add contour for reference data to a given axis for a specific month.
        Only plot contours on model data plots.
        Args:
            ax (matplotlib.axes._subplots.AxesSubplot): Axis to add the contour to.
            month (int): Month for which the reference data is plotted.
            data_type (str): 'reference' or 'model' - only plot contours for 'model' data.
            **kwargs: Additional keyword arguments for customization, such as:
                line_levels (list):      List of contour levels to draw. Default is [0.2].
        """
        line_levels = kwargs.get('line_levels', [0.2])

        if self.reg_ref and data_type == 'model':

            ref_dat = self.reg_ref[0].sel(month=month)

            if ref_dat is not None:
                self.logger.debug(f"Adding contour for reference data at {line_levels} for month: {month}")
                ref_dat.plot.contour(ax=ax, transform=ccrs.PlateCarree(),
                                    levels=line_levels, colors='red', linewidths=1, 
                                    linestyles='-', add_colorbar=False)
        else:
            self.logger.debug(f"No reference contours for data_type: {data_type}, month: {month}")

    def _mask_ice_at_mid_lats(self, datarr):
        """
        Further clean the data array.
        For 'thickness' method, remove values below 0.01 and mask lats inside the [-50, 50] range.
        For 'fraction' method, for lats values inside the range [-45, 40] overwrite NaNs with 0.

        Args:
            datarr (xarray.DataArray): DataArray containing sea ice data with attributes.

        Returns:
            xarray.DataArray: Cleaned DataArray with masked values.
        """
        lat = datarr['lat'].broadcast_like(datarr)
        if self.method == 'thickness':
            mask = ((lat > -50) & (lat < 50)) | (datarr < 0.01)
            return datarr.where(~mask)
        elif self.method == 'fraction':
            mask = (lat >= -45) & (lat <= 40)
            return datarr.where(~mask, 0) # overwrite NaNs

    def _handle_data(self, datain) -> list | None:
        """
        Handle `datain` and return a flat list of xarray.DataArray objects.
        Allow the following cases:
            - A single xarray.Dataset: includes all its data variables (data_vars)
            - A single xarray.DataArray: includes the DataArray itself
            - A list or tuple of either type (mixed allowed), skip None values in the list
        
        Args:
            datain (xarray.DataArray, xarray.Dataset, list, tuple): Input data to process.

        Returns:
            list: A flat list of xarray.DataArray objects or None
        """
        if datain is None:
            self.logger.debug("No datain provided, thus returning None.")
            return None
        
        datain_list = to_list(datain)
        
        data_arrays = []
        for model in datain_list:
            if model is None:
                continue
            if isinstance(model, xr.Dataset):
                data_arrays.extend(model.data_vars.values())
            elif isinstance(model, xr.DataArray):
                data_arrays.append(model)
            elif isinstance(model, (list, tuple)):
                # If a list or tuple is provided, recursively handle each item
                data_arrays.extend(self._handle_data(model))
            else:
                raise TypeError(f"Unsupported type in 'datain' list: {type(model)}")
        if not data_arrays:
            raise ValueError("No valid data found in 'datain'. Ensure it contains xarray.DataArray or xarray.Dataset objects.")
        return data_arrays

    def _detect_common_regions(self, dalists) -> list:
        """
        Detect AQUA_regions from list of data variables.
        """
        def _update_regions_list(dalist):
            if dalist is None:
                self.logger.warning(f"Input data list is None. Skipping region detection")
                return
            
            self.regions_to_plot = []
            for da in dalist:
                if da is None:
                    continue
                if not isinstance(da, xr.DataArray):
                    self.logger.warning(f"Expected xarray.DataArray, got {type(da)}. Skipping")
                    continue
                if 'AQUA_region' not in da.attrs:
                    self.logger.warning(f"DataArray {da.name} does not have 'AQUA_region' attribute, skipping")
                    continue

                region = da.attrs['AQUA_region']
                if region not in self.regions_to_plot:
                    self.regions_to_plot.append(region)

            if not self.regions_to_plot:
                self.logger.warning("No valid regions detected in the input list.")

        for dalist in dalists:
            _update_regions_list(dalist)

        if not self.regions_to_plot:
            raise ValueError("No regions to plot detected.")

    def _find_data_for_region(self, aqua_region):
        """
        Filter a list of xarray.DataArray objects by a specific region.
        """
        def _filter_by_region_in_list(dalist):
            if dalist is None:
                raise ValueError("No data available for filtering by region.")

            filtered = [da for da in dalist if da.attrs.get('AQUA_region') == aqua_region]
            return filtered if filtered else None

        self.reg_ref = _filter_by_region_in_list(self.ref) if self.ref else None
        self.reg_models = _filter_by_region_in_list(self.models) if self.models else None

        if not self.reg_ref and not self.reg_models:
            self.logger.error(f"No data found for region '{aqua_region}'. Skipping this region.")
            return False
        return True

    def _save_plots(self, fig, data, diagnostic_product, description, 
                    data_ref=None, extra_keys=None):
        """
        Handles the saving of a figure using OutputSaver.

        Args:
            fig (matplotlib.Figure): The figure to save.
            data (xarray.Dataset): Dataset.
            data_ref (xarray.Dataset, optional): Reference dataset.
            diagnostic_product (str): Name of the diagnostic product.
            description (str): Description of the figure.
            extra_keys (dict, optional): Extra keys for filename.
        """
        # Ensure data is not None
        if data is None:
            raise ValueError("Data cannot be None for saving figures")

        if not self.save_pdf and not self.save_png:
            return
        
        outputsaver = OutputSaver(
            diagnostic='seaice',
            catalog=data.attrs.get('AQUA_catalog',''),
            model=data.attrs.get('AQUA_model',''),
            exp=data.attrs.get('AQUA_exp',''),
            model_ref=data_ref.attrs.get('AQUA_model','') if data_ref is not None else None,
            exp_ref=data_ref.attrs.get('AQUA_exp','') if data_ref is not None else None,
            outputdir=self.outputdir,
            loglevel=self.loglevel,
            realization=self.realizations
        )
        
        metadata = {"Description": description}
        extra_keys = {} if extra_keys is None else dict(extra_keys)
        
        outputsaver.save_figure(fig, diagnostic_product,
                                extra_keys=extra_keys, metadata=metadata,
                                save_pdf=self.save_pdf, save_png=self.save_png,
                                rebuild=self.rebuild, dpi=self.dpi)
