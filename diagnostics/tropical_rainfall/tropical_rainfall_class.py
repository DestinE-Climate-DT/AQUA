"""The module contains Tropical Precipitation Diagnostic:

.. moduleauthor:: AQUA team <natalia.nazarova@polito.it>

"""
from typing import Union, Tuple, Optional, Any, List
from aqua.logger import log_configure
from aqua.util import ConfigPath, load_yaml

import types
import sys 
import yaml
import os

from .src.tropical_rainfall_tools import ToolsClass
from .src.tropical_rainfall_plots import PlottingClass 
from .src.tropical_rainfall_main import MainClass 
from .src.tropical_rainfall_meta import MetaClass 

print('Running tropical rainfall diagnostic...')

print('Reading configuration yaml file..')

configname = './tropical_rainfall/config-tropical-rainfall.yml'
root_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Get the root folder
config_path = os.path.join(root_folder, configname) 
if not os.path.exists(config_path):
    print(f"The configuration file '{configname}' does not exist.")
    raise FileNotFoundError(f"The configuration file '{configname}' does not exist.")
try:
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
except FileNotFoundError as e:
    # Handle FileNotFoundError exception
    print(f"An unexpected error occurred: {e}")
    raise e
except Exception as e:
    # Handle other exceptions
    print(f"An unexpected error occurred: {e}")
            
#try:
    # caution: path[0] is reserved for script path (or '' in REPL)
    #config = load_yaml('config-tropical-rainfall.yml')
#except ValueError:
#    config = load_yaml('diagnostics/tropical_rainfall/config-tropical-rainfall.yml')

def get_arg(config_value, default_value):
    if config_value is not None:
        return config_value
    else:
        return default_value
    
loglevel = get_arg(config['loglevel'], 'WARNING')
trop_lat = get_arg(config['class_attributes']['trop_lat'], 10)
num_of_bins = get_arg(config['class_attributes']['num_of_bins'], 1000)
first_edge = get_arg(config['class_attributes']['first_edge'], 0)
bins = get_arg(config['class_attributes']['bins'], 0)
width_of_bin = None
model_variable = get_arg(config['class_attributes']['model_variable'], 'tprate')
new_unit = get_arg(config['class_attributes']['new_unit'], 'mm/day')
path_to_netcdf = get_arg(config[ConfigPath().machine]['path_to_netcdf'], './')
path_to_pdf = get_arg(config[ConfigPath().machine]['path_to_pdf'], './')
#time_frame
s_time = get_arg(config['time_frame']['s_time'], None)
f_time = get_arg(config['time_frame']['f_time'], None)
s_year = get_arg(config['time_frame']['s_year'], None)
f_year = get_arg(config['time_frame']['f_year'], None)
s_month = get_arg(config['time_frame']['s_month'], None)
f_month = get_arg(config['time_frame']['f_month'], None) 
#plot_attributes
pdf_format = get_arg(config['plot_attributes']['pdf_format'], True)
figsize = get_arg(config['plot_attributes']['figsize'], 1)
linewidth = get_arg(config['plot_attributes']['linewidth'], 3)
fontsize = get_arg(config['plot_attributes']['fontsize'], 14)
smooth = get_arg(config['plot_attributes']['smooth'], True) 
step = get_arg(config['plot_attributes']['step'], False)  
color_map = get_arg(config['plot_attributes']['color_map'], False)
cmap = get_arg(config['plot_attributes']['cmap'], 'coolwarm')
linestyle = get_arg(config['plot_attributes']['linestyle'], '-')
ylogscale = get_arg(config['plot_attributes']['ylogscale'], True) 
xlogscale = get_arg(config['plot_attributes']['xlogscale'], False)
number_of_axe_ticks = get_arg(config['plot_attributes']['number_of_axe_ticks'], 4)
number_of_bar_ticks = get_arg(config['plot_attributes']['number_of_bar_ticks'], 6) 

class Tropical_Rainfall(metaclass=MetaClass):
    """This class is a minimal version of the Tropical Precipitation Diagnostic."""

    def __init__(self,
                 trop_lat: Optional[float] = trop_lat,
                 s_time: Union[str, int, None] = s_time,
                 f_time: Union[str, int, None] = f_time,
                 s_year: Optional[int] = s_year,
                 f_year: Optional[int] = f_year,
                 s_month: Optional[int] = s_month,
                 f_month: Optional[int] = f_month,
                 num_of_bins: Optional[int] = num_of_bins,
                 first_edge: Optional[float] = first_edge,
                 width_of_bin: Optional[float] = width_of_bin,
                 bins: Optional[list] = bins,
                 new_unit: Optional[str] = new_unit,
                 model_variable: Optional[str] = model_variable,
                 path_to_netcdf: Optional[str] = path_to_netcdf,  
                 path_to_pdf: Optional[str] = path_to_pdf,
                 loglevel: str = loglevel):
        """ The constructor of the class.

        Args:
            trop_lat (float, optional): The latitude of the tropical zone. Defaults to 10.
            s_time (Union[str, int, None], optional): The start time of the time interval. Defaults to None.
            f_time (Union[str, int, None], optional): The end time of the time interval. Defaults to None.
            s_year (Union[int, None], optional): The start year of the time interval. Defaults to None.
            f_year (Union[int, None], optional): The end year of the time interval. Defaults to None.
            s_month (Union[int, None], optional): The start month of the time interval. Defaults to None.
            f_month (Union[int, None], optional): The end month of the time interval. Defaults to None.
            num_of_bins (Union[int, None], optional): The number of bins. Defaults to None.
            first_edge (float, optional): The first edge of the bin. Defaults to 0.
            width_of_bin (Union[float, None], optional): The width of the bin. Defaults to None.
            bins (list, optional): The bins. Defaults to 0.
            new_unit (str, optional): The unit for the new data. Defaults to 'mm/day'.
            model_variable (str, optional): The name of the model variable. Defaults to 'tprate'.
            path_to_netcdf (Union[str, None], optional): The path to the netCDF file. Defaults to None.
            path_to_pdf (Union[str, None], optional): The path to the PDF file. Defaults to None.
            loglevel (str, optional): The log level for logging. Defaults to 'WARNING'.
        """

        self.trop_lat = trop_lat
        self.s_time = s_time
        self.f_time = f_time
        self.s_year = s_year
        self.f_year = f_year
        self.s_month = s_month
        self.f_month = f_month
        self.num_of_bins = num_of_bins
        self.first_edge = first_edge    
        self.bins = bins
        self.new_unit = new_unit
        self.model_variable = model_variable
        
        self.loglevel = loglevel
        self.logger = log_configure(self.loglevel, 'Trop. Rainfall')
        self.tools = ToolsClass(loglevel=loglevel)
        
        self.path_to_netcdf = self.tools.get_netcdf_path() if path_to_netcdf is None else path_to_netcdf
        self.path_to_pdf = self.tools.get_pdf_path() if path_to_pdf is None else path_to_pdf

        self.main = MainClass(trop_lat=self.trop_lat, s_time=self.s_time, f_time=self.f_time, s_year=self.s_year, f_year=self.f_year,
                        s_month=self.s_month, f_month=self.f_month, num_of_bins=self.num_of_bins, first_edge=self.first_edge,
                        width_of_bin=None, bins=self.bins, new_unit=self.new_unit, model_variable=self.model_variable,
                        path_to_netcdf=self.path_to_netcdf, path_to_pdf=self.path_to_pdf, loglevel=self.loglevel)

        if width_of_bin is None:
            self.precipitation_rate_units_converter = self.main.precipitation_rate_units_converter
            self.width_of_bin = self.precipitation_rate_units_converter(0.05, old_unit='mm/day', new_unit=self.new_unit)
        else:
            self.width_of_bin = width_of_bin
        self.main.width_of_bin = self.width_of_bin  
        
        self.plots = PlottingClass(pdf_format = pdf_format, figsize = figsize, linewidth = linewidth,
                            fontsize = fontsize, smooth = smooth, step = step, color_map = color_map, cmap = cmap,
                            linestyle = linestyle, ylogscale = ylogscale, xlogscale = xlogscale, model_variable = model_variable, 
                            number_of_axe_ticks = number_of_axe_ticks, number_of_bar_ticks = number_of_bar_ticks, loglevel=loglevel)
        
        self.import_methods()
                     
    def import_methods(self):
        pass
        #for method_name in methods_to_import:
        #    setattr(self, method_name, getattr(self.main, method_name))
        #    setattr(self, method_name, types.MethodType(getattr(self, method_name), self))
Tropical_Rainfall.class_attributes_update.__doc__ = MainClass.class_attributes_update.__doc__