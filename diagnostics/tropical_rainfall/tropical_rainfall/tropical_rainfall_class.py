import os
import yaml
from typing import Union, Optional
from aqua.logger import log_configure
from .src.tropical_rainfall_tools import ToolsClass
from .src.tropical_rainfall_plots import PlottingClass
from .src.tropical_rainfall_main import MainClass
from .src.tropical_rainfall_meta import MetaClass

class Tropical_Rainfall(metaclass=MetaClass):
    """This class is a minimal version of the Tropical Precipitation Diagnostic."""

    def __init__(self,
                 config_file: str = None,
                 trop_lat: Optional[float] = None,
                 s_time: Union[str, int, None] = None,
                 f_time: Union[str, int, None] = None,
                 s_year: Optional[int] = None,
                 f_year: Optional[int] = None,
                 s_month: Optional[int] = None,
                 f_month: Optional[int] = None,
                 num_of_bins: Optional[int] = None,
                 first_edge: Optional[float] = None,
                 width_of_bin: Optional[float] = None,
                 bins: Optional[list] = None,
                 new_unit: Optional[str] = None,
                 model_variable: Optional[str] = None,
                 path_to_netcdf: Optional[str] = None,
                 path_to_pdf: Optional[str] = None,
                 loglevel: str = 'WARNING'):
        """ The constructor of the class.
        """

        if config_file is None:
            # Read the configuration path from the known location within the package directory
            config_file = os.path.join(os.path.dirname(__file__), 'config', 'current_config.yml')
            if not os.path.exists(config_file):
                config_file = os.path.join(os.path.dirname(__file__), 'config', 'config-tropical-rainfall.yml')
                if not os.path.exists(config_file):
                    raise FileNotFoundError(f"No configuration file found. Please use 'tropical_rainfall add_config' to set one.")

        self.config_file = config_file

        # Load configuration
        self.config = self.load_config(self.config_file)

        # Extract class attributes from config
        self.trop_lat = trop_lat or self.config.get('class_attributes', {}).get('trop_lat', 10)
        self.s_time = s_time or self.config.get('time_frame', {}).get('s_time', None)
        self.f_time = f_time or self.config.get('time_frame', {}).get('f_time', None)
        self.s_year = s_year or self.config.get('time_frame', {}).get('s_year', None)
        self.f_year = f_year or self.config.get('time_frame', {}).get('f_year', None)
        self.s_month = s_month or self.config.get('time_frame', {}).get('s_month', None)
        self.f_month = f_month or self.config.get('time_frame', {}).get('f_month', None)
        self.num_of_bins = num_of_bins or self.config.get('class_attributes', {}).get('num_of_bins', 1000)
        self.first_edge = first_edge or self.config.get('class_attributes', {}).get('first_edge', 0)
        self.width_of_bin = width_of_bin or self.config.get('class_attributes', {}).get('width_of_bin', 0.05)
        self.bins = bins or self.config.get('class_attributes', {}).get('bins', 0)
        self.new_unit = new_unit or self.config.get('class_attributes', {}).get('new_unit', 'mm/day')
        self.model_variable = model_variable or self.config.get('class_attributes', {}).get('model_variable', 'mtpr')

        self.loglevel = loglevel
        self.logger = log_configure(self.loglevel, 'Trop. Rainfall')
        self.tools = ToolsClass(config_file=self.config_file, loglevel=self.loglevel)

        self.path_to_netcdf = path_to_netcdf or self.config.get('path_to_netcdf', './')
        self.path_to_pdf = path_to_pdf or self.config.get('path_to_pdf', './')

        self.main = MainClass(trop_lat=self.trop_lat, s_time=self.s_time, f_time=self.f_time,
                              s_year=self.s_year, f_year=self.f_year, s_month=self.s_month, f_month=self.f_month,
                              num_of_bins=self.num_of_bins, first_edge=self.first_edge,
                              width_of_bin=self.width_of_bin, bins=self.bins, new_unit=self.new_unit, model_variable=self.model_variable,
                              path_to_netcdf=self.path_to_netcdf, path_to_pdf=self.path_to_pdf, loglevel=self.loglevel)

        self.precipitation_rate_units_converter = self.main.precipitation_rate_units_converter

        self.plots = PlottingClass(pdf_format=self.config.get('plot_attributes', {}).get('pdf_format', True),
                                   figsize=self.config.get('plot_attributes', {}).get('figsize', 1),
                                   linewidth=self.config.get('plot_attributes', {}).get('linewidth', 2),
                                   fontsize=self.config.get('plot_attributes', {}).get('fontsize', 14),
                                   smooth=self.config.get('plot_attributes', {}).get('smooth', False),
                                   step=self.config.get('plot_attributes', {}).get('step', True),
                                   color_map=self.config.get('plot_attributes', {}).get('color_map', False),
                                   cmap=self.config.get('plot_attributes', {}).get('cmap', 'coolwarm'),
                                   linestyle=self.config.get('plot_attributes', {}).get('linestyle', '-'),
                                   ylogscale=self.config.get('plot_attributes', {}).get('ylogscale', True),
                                   xlogscale=self.config.get('plot_attributes', {}).get('xlogscale', False),
                                   model_variable=self.model_variable,
                                   number_of_axe_ticks=self.config.get('plot_attributes', {}).get('number_of_axe_ticks', 4),
                                   number_of_bar_ticks=self.config.get('plot_attributes', {}).get('number_of_bar_ticks', 6),
                                   loglevel=self.loglevel)

        self.import_methods()

    def load_config(self, config_file):
        with open(config_file, 'r') as f:
            return yaml.safe_load(f)

    def import_methods(self):
        pass
