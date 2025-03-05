""" PlotSeaIce doc """
import os
import xarray as xr
from matplotlib import pyplot as plt

from aqua.exceptions import NoDataError, NotEnoughDataError
from aqua.logger import log_configure, log_history
from aqua.util import ConfigPath, OutputSaver
from aqua.graphics import plot_timeseries

xr.set_options(keep_attrs=True)

class PlotSeaIce:
    """ PlotSeaIce class """

    def __init__(self, 
                 monthly_models=None, annual_models=None,
                 monthly_ref=None, annual_ref=None,
                 monthly_std_ref: str = None, annual_std_ref: str = None,
                 harmonise_time:  str = None, # 'common', 'to_ref' (only if ref is given), tuple: ('to_model', int) [the int give the list index for the time to use]
                 fillna: str = None, # 'zero', 'nan', 'interpolate', 'value'
                 plot_kw=None,
                 unit=None, # This might involve some function to get the unit of the variable or to convert the unit [from timeseries.py 
                            # Units of the variable. Default is None and units attribute is used.]
                 outdir='./',
                 rebuild=None, 
                 filename_keys=None,  # List of keys to keep in the filename. Default is None, which includes all keys.
                 save_pdf=True, 
                 save_png=True, dpi=300,
                 loglevel='WARNING'):
        
        # Logging setup
        self.loglevel = loglevel
        self.logger = log_configure(log_level=self.loglevel, log_name='PlotSeaIce')
        # self.logger = log_history()
        
        # Data attributes
        self.monthly_models = monthly_models  
        self.annual_models = annual_models
        self.monthly_ref = monthly_ref
        self.annual_ref = annual_ref
        self.monthly_std_ref = monthly_std_ref
        self.annual_std_ref  = annual_std_ref

        # Processing options
        self.harmonise_time = harmonise_time
        self.fillna = fillna

        # Plot configuration
        self.plot_kw = plot_kw or { 'ylimits': {},  'xlimits': {},  'title': None, 
                                    'xlabel':  None,'ylabel': None, 'grid': None, 
                                    'figsize': None}
        self.unit = unit

        # Output & saving settings
        self.outdir  = outdir
        self.rebuild = rebuild
        self.filename_keys = filename_keys
        self.save_pdf = save_pdf
        self.save_png = save_png
        self.dpi = dpi

    def get_labels(self, input_xrarray):
        """Extracts standard_name attributes from xr.DataArray or a list of xr.DataArrays."""
        if input_xrarray is None:
            return None

        # If a single DataArray is passed, return a string (do not convert to list!)
        if isinstance(input_xrarray, xr.DataArray):
            return input_xrarray.attrs.get("standard_name", input_xrarray.name)
        
        # If a list of DataArray is passed, return a list of strings
        if isinstance(input_xrarray, list) and all(isinstance(da, xr.DataArray) for da in input_xrarray):
            return [da.attrs.get("standard_name", da.name) for da in input_xrarray]

        error_msg = "Error input data: must be a list of xr.DataArray or a single xr.DataArray"
        self.logger.debug(error_msg)

        raise TypeError(error_msg)

    def plot_seaice(self, **kwargs):
        """ Plot data """
        # Assign default values only if the data were not provided
        data_labels = self.get_labels(self.monthly_models) if self.monthly_models is not None else (
                      self.get_labels(self.annual_models)  if self.annual_models  is not None else None )
        ref_label = self.get_labels(self.monthly_ref) if self.monthly_ref is not None else (
                    self.get_labels(self.annual_ref) if self.annual_ref is not None else None)
        std_label = self.get_labels(self.monthly_std_ref) if self.monthly_std_ref is not None else (
                    self.get_labels(self.annual_std_ref) if self.annual_std_ref is not None else None)
                    
        self.logger.info("Plotting sea ice data...")
        
        fig, ax = plot_timeseries(monthly_data=self.monthly_models, 
                                  annual_data=self.annual_models,
                                  ref_monthly_data=self.monthly_ref, 
                                  ref_annual_data=self.annual_ref,
                                  std_monthly_data=self.monthly_std_ref, 
                                  std_annual_data=self.annual_std_ref,
                                  data_labels=data_labels,
                                  ref_label=ref_label,
                                  std_label=std_label,
                                  **kwargs)
        
        print(data_labels)
        print(ref_label)

        fig.savefig(os.path.join(self.outdir, "seaice_plot.png"), format="png", dpi=self.dpi)

        # Returning the figure and axis for further modifications if needed
        return fig, ax
