"""The module contains Tropical Precipitation Diagnostic:

.. moduleauthor:: AQUA team <natalia.nazarova@polito.it>

"""
from typing import Union, Optional
from aqua.logger import log_configure

from .src.tropical_rainfall_tools import ToolsClass
from .src.tropical_rainfall_plots import PlottingClass
from .src.tropical_rainfall_main import MainClass
from .src.tropical_rainfall_meta import MetaClass


print('Running tropical rainfall diagnostic...')

print('Reading configuration yaml file..')
config = ToolsClass().get_config()
machine = ToolsClass().get_machine()

loglevel = ToolsClass().get_config_value(config, 'loglevel', default='WARNING')
trop_lat = ToolsClass().get_config_value(config, 'class_attributes', 'trop_lat', default=10)
num_of_bins = ToolsClass().get_config_value(config, 'class_attributes', 'num_of_bins', default=1000)
first_edge = ToolsClass().get_config_value(config, 'class_attributes', 'first_edge', default=0)
bins = ToolsClass().get_config_value(config, 'class_attributes', 'bins', default=0)
width_of_bin = ToolsClass().get_config_value(config, 'class_attributes', 'width_of_bin', default=0.05)  # in [mm/day]
model_variable = ToolsClass().get_config_value(config, 'class_attributes', 'model_variable', default='tprate')
new_unit = ToolsClass().get_config_value(config, 'class_attributes', 'new_unit', default='mm/day')
path_to_netcdf = ToolsClass().get_config_value(config, machine, 'path_to_netcdf', default='./')
path_to_pdf = ToolsClass().get_config_value(config, machine, 'path_to_pdf', default='./')
# time_frame
s_time = ToolsClass().get_config_value(config, 'time_frame', 's_time', default=None)
f_time = ToolsClass().get_config_value(config, 'time_frame', 'f_time', default=None)
s_year = ToolsClass().get_config_value(config, 'time_frame', 's_year', default=None)
f_year = ToolsClass().get_config_value(config, 'time_frame', 'f_year', default=None)
s_month = ToolsClass().get_config_value(config, 'time_frame', 's_month', default=None)
f_month = ToolsClass().get_config_value(config, 'time_frame', 'f_month', default=None)
# plot_attributes
pdf_format = ToolsClass().get_config_value(config, 'plot_attributes', 'pdf_format', default=True)
figsize = ToolsClass().get_config_value(config, 'plot_attributes', 'figsize', default=1)
linewidth = ToolsClass().get_config_value(config, 'plot_attributes', 'linewidth', default=3)
fontsize = ToolsClass().get_config_value(config, 'plot_attributes', 'fontsize', default=14)
smooth = ToolsClass().get_config_value(config, 'plot_attributes', 'smooth', default=True)
step = ToolsClass().get_config_value(config, 'plot_attributes', 'step', default=False)
color_map = ToolsClass().get_config_value(config, 'plot_attributes', 'color_map', default=False)
cmap = ToolsClass().get_config_value(config, 'plot_attributes', 'cmap', default='coolwarm')
linestyle = ToolsClass().get_config_value(config, 'plot_attributes', 'linestyle', default='-')
ylogscale = ToolsClass().get_config_value(config, 'plot_attributes', 'ylogscale', default=True)
xlogscale = ToolsClass().get_config_value(config, 'plot_attributes', 'xlogscale', default=False)
number_of_axe_ticks = ToolsClass().get_config_value(config, 'plot_attributes', 'number_of_axe_ticks', default=4)
number_of_bar_ticks = ToolsClass().get_config_value(config, 'plot_attributes', 'number_of_bar_ticks', default=6)


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

        self.main = MainClass(trop_lat=self.trop_lat, s_time=self.s_time, f_time=self.f_time,
                              s_year=self.s_year, f_year=self.f_year, s_month=self.s_month, f_month=self.f_month,
                              num_of_bins=self.num_of_bins, first_edge=self.first_edge,
                              width_of_bin=None, bins=self.bins, new_unit=self.new_unit, model_variable=self.model_variable,
                              path_to_netcdf=self.path_to_netcdf, path_to_pdf=self.path_to_pdf, loglevel=self.loglevel)

        self.precipitation_rate_units_converter = self.main.precipitation_rate_units_converter
        self.width_of_bin = self.precipitation_rate_units_converter(width_of_bin, old_unit='mm/day', new_unit=self.new_unit)
        self.main.width_of_bin = self.width_of_bin

        self.plots = PlottingClass(pdf_format=pdf_format, figsize=figsize, linewidth=linewidth,
                                   fontsize=fontsize, smooth=smooth, step=step, color_map=color_map,
                                   cmap=cmap, linestyle=linestyle, ylogscale=ylogscale,
                                   xlogscale=xlogscale, model_variable=model_variable,
                                   number_of_axe_ticks=number_of_axe_ticks, number_of_bar_ticks=number_of_bar_ticks,
                                   loglevel=loglevel)

        self.import_methods()

    def import_methods(self):
        pass


Tropical_Rainfall.class_attributes_update.__doc__ = MainClass.class_attributes_update.__doc__
