""" PlotSeaIce doc """
import os
import xarray as xr
from matplotlib import pyplot as plt

from aqua.diagnostics.core import Diagnostic
from aqua.diagnostics import GlobalBiases, PlotGlobalBiases
from aqua.exceptions import NoDataError, NotEnoughDataError
from aqua.logger import log_configure, log_history
from aqua.util import ConfigPath, OutputSaver, get_projection, plot_box
from .util import extract_dates, _check_list_regions_type
from aqua.graphics import plot_single_map, plot_single_map_diff, plot_maps
from matplotlib.colors import LinearSegmentedColormap

xr.set_options(keep_attrs=True)

class Plot2DSeaIce:
    """ A class for processing and visualizing surface maps and 
    biases of sea ice fraction or thickness. """

    def __init__(self,
                 models=None,
                 ref=None,
                 model: str = None, exp: str = None, source: str = None, catalog: str = None,
                 regions_to_plot: list = None, # ['Arctic', 'Antarctic'], # this is a list of strings with the region names to plot
                 outdir='./',
                 rebuild=True,
                 dpi=300, loglevel='WARNING'):

        self.loglevel = loglevel
        self.logger = log_configure(log_level=self.loglevel, log_name='Plot2DSeaIce')

        self.ref = self._handle_data(ref)
        self.models = self._handle_data(models)
        
        self.regions_to_plot = _check_list_regions_type(regions_to_plot, logger=self.logger)
        self._detect_common_regions([self.models, self.ref])

        self.catalog = catalog
        self.model = model
        self.exp = exp
        self.source = source

        self.outdir  = outdir
        self.rebuild = rebuild
        self.dpi = dpi

    def plot_2d_seaice(self, plot_type='var', months=[2,9], projkw=None,
                       save_pdf=True, save_png=True, **kwargs):
        """
        Plot sea ice data and biases.

        Args:
            plot_type (str): Type of plot to generate ['var' or 'bias'].
            **kwargs: Additional keyword arguments for customization. Supported kwargs include:
                projkw (dict): Dictionary with projection parameters for the plot.
        """
        self.logger.info("Starting Plot2DSeaIce run")

        if not all(1 <= m <= 12 for m in months):
            raise ValueError("Invalid month value. Months must be between 1 and 12.")
        self.months = months

        self.plot_type = plot_type
        self.save_pdf = save_pdf
        self.save_png = save_png

        self.projname = projkw.get('projname', 'unknown')
        self.projpars = projkw.get('projpars', {})

        if plot_type == 'bias':
            pass
        else:
            self.plot_var_map()

    def _set_projpars(self, **kwargs):
        """
        Set projection parameters based on the provided projection name and additional keyword arguments.
        """
        if not isinstance(self.reg_data[0], xr.DataArray):
            raise TypeError("datain must be an xarray.DataArray.")

        if self.projname == 'orthographic':
            projpars_dict = {'central_longitude':0.0, 
                             'central_latitude':max(self.reg_data[0]['lat'].values, key=abs)}
        else:
            projpars_dict = {}
        return projpars_dict

    def plot_var_map(self):
        """
        Plot sea ice variable only (fraction or thickness).
        """
        self.logger.debug("Starting plot_var_map")

        if not self.models and not self.ref:
            raise NoDataError("No models and ref provided for plotting.")

        for region in self.regions_to_plot:
            self.logger.debug(f"Plotting region: {region}")

            self._get_data_in_region(region)
    
            self.projpars = self._set_projpars()
            self.proj = get_projection(self.projname, **self.projpars)

            for month in self.months:
                nrows, ncols = plot_box(num_plots=len(self.reg_data))
                fig = plt.figure(figsize=(ncols * 5, nrows * 4))
        
                for i, dat in enumerate(self.reg_data):
                    print(f"Plotting data variable {i+1}/{len(self.reg_data)}: {dat.name}")
                    
                    mon_dat = dat.sel(month=month)

                    ax = fig.add_subplot(ncols, nrows, i + 1, projection=self.proj)
                    plot_single_map(mon_dat, proj=self.proj, ax=ax, fig=fig,
                                    cmap=self._get_cmap_fraction(), add_land=True,
                                    contour=False, cbar_ticks_rounding=1,
                                    cbar_label=f'Sea ice {mon_dat.attrs.get('AQUA_method', '')} {mon_dat.attrs.get("units", "unknown")}')
    
    def _handle_data(self, datain) -> list:
        """
        Handle `datain` and return a flat list of xarray.DataArray objects.
        Allow the following cases:
            - A single xarray.Dataset: includes all its data variables (data_vars)
            - A single xarray.DataArray: includes the DataArray itself
            - A list or tuple of either type (mixed allowed)
        """
        if datain is None:
            self.logger.debug("No datain provided, thus returning None.")
            return None
        
        datain_list = [datain] if isinstance(datain, (xr.Dataset, xr.DataArray)) else datain

        data_arrays = []
        for model in datain_list:
            if isinstance(model, xr.Dataset):
                data_arrays.extend(model.data_vars.values())
            elif isinstance(model, xr.DataArray):
                data_arrays.append(model)
            else:
                raise TypeError(f"Unsupported type in 'datain' list: {type(model)}")
        if not data_arrays:
            raise NoDataError("No valid data found in 'datain'. Ensure it contains xarray.DataArray or xarray.Dataset objects.")
        return data_arrays

    def _detect_common_regions(self, dalists) -> list:
        """
        Detect AQUA_regions from list of data variables.
        """
        def _update_regions_list(dalist):
            if dalist is None:
                self.logger.warning(f"Input data list is None. Skipping region detection")
                return
            if self.regions_to_plot is None:
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
        
        valid_dalists = [dalist for dalist in dalists if dalist is not None]

        for dalist in dalists:
            _update_regions_list(dalist)
        if not self.regions_to_plot:
            raise NoDataError("No regions to plot detected.")

    def _get_data_in_region(self, region):
        """
        Filter a list of xarray.DataArray objects by a specific region.
        """
        def _filter_by_region_in_list(dalist):
            if dalist is None:
                raise NoDataError("No data available for filtering by region.")
            filtered = [da for da in dalist if da.attrs.get('AQUA_region') == region]
            return filtered if filtered else None

        reg_ref, reg_models = None, None
        if self.ref:
            reg_ref = _filter_by_region_in_list(self.ref)
        if self.models:
            reg_models = _filter_by_region_in_list(self.models)
        
        self.reg_data = []
        if reg_ref:
            self.reg_data.extend(reg_ref)
        if reg_models:
            self.reg_data.extend(reg_models)
        if not self.reg_data:
            raise NoDataError(f"No data found for the region '{region}'.")

    def _get_cmap_fraction(self):
        """
        Get the colormap for sea ice fraction.
        """
        source_colors = [[0.0, 0.0, 0.2], [0.0, 0.0, 0.0],[0.5, 0.5, 0.5], [0.6, 0.6, 0.6], 
                         [0.7, 0.7, 0.7], [0.8, 0.8, 0.8], [0.9, 0.9, 0.9],[1.0, 1.0, 1.0]]
        pcm = LinearSegmentedColormap.from_list('pcm', source_colors, N = 15)
        return pcm
