"""The module contains Tropical Precipitation Diagnostic:

.. moduleauthor:: AQUA team <natalia.nazarova@polito.it>

"""
from typing import Union, Tuple, Optional, Any, List#, get_type_hints
from aqua.logger import log_configure


from .src.tropical_rainfall_tools import ToolsClass
from .src.tropical_rainfall_plots import PlottingClass 
from .src.tropical_rainfall_main import MainClass 

class Tropical_Rainfall:
    """This class is a minimal version of the Tropical Precipitation Diagnostic."""

    def __init__(self,
                 trop_lat: float = 10,
                 s_time: Union[str, int, None] = None,
                 f_time: Union[str, int, None] = None,
                 s_year: Union[int, None] = None,
                 f_year: Union[int, None] = None,
                 s_month: Union[int, None] = None,
                 f_month: Union[int, None] = None,
                 num_of_bins: Union[int, None] = None,
                 first_edge: float = 0,
                 width_of_bin: Union[float, None] = None,
                 bins: list = 0,
                 new_unit: str = 'mm/day',
                 model_variable: str = 'tprate',
                 path_to_netcdf: Union[str, None] = None,  
                 path_to_pdf: Union[str, None] = None,
                 loglevel: str = 'WARNING'):
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
        
        # the most important for import
        self.histogram = self.main.histogram
        self.merge_list_of_histograms = self.main.merge_list_of_histograms
        self.histogram_plot = self.main.histogram_plot
        self.average_into_netcdf = self.main.average_into_netcdf
        self.plot_of_average = self.main.plot_of_average
        self.plot_bias = self.main.plot_bias
        self.plot_seasons_or_months = self.main.plot_seasons_or_months
        self.seasonal_or_monthly_mean = self.main.seasonal_or_monthly_mean
        self.map = self.main.map
        self.get_95percent_level = self.main.get_95percent_level
        self.seasonal_095level_into_netcdf = self.main.seasonal_095level_into_netcdf
        self.add_UTC_DataAaray = self.main.add_UTC_DataAaray
        self.daily_variability_plot = self.main.daily_variability_plot

        # the less iportant for import 
        self.histogram_lowres = self.main.histogram_lowres
        self.merge_two_datasets =self.main.merge_two_datasets
        self.add_frequency_and_pdf = self.main.add_frequency_and_pdf
        self.precipitation_rate_units_converter = self.main.precipitation_rate_units_converter
        self.preprocessing = self.main.preprocessing

        # for test change the test
        self.dataset_to_netcdf = self.main.dataset_to_netcdf
        self.latitude_band = self.main.latitude_band


    def class_attributes_update(self, **kwargs): 
        """
        Update the class attributes with new values.
        """
        attribute_names = ['trop_lat', 's_time', 'f_time', 's_year', 'f_year', 's_month', 
                       'f_month', 'num_of_bins', 'first_edge', 'width_of_bin', 'bins', 
                       'model_variable', 'new_unit']
        for attr_name in attribute_names:
            if attr_name in kwargs and isinstance(kwargs[attr_name], type(getattr(self, attr_name))):
                setattr(self, attr_name, kwargs[attr_name])
                setattr(self.main, attr_name, kwargs[attr_name])
                #self.main.class_attributes_update(getattr(attr_name)=getattr(attr_name)) #need to test the types 
            elif attr_name in kwargs and not isinstance(kwargs[attr_name], type(getattr(self, attr_name))):
                raise TypeError(f"{attr_name} must be {type(getattr(self, attr_name))}")
Tropical_Rainfall.class_attributes_update.__doc__ = MainClass.class_attributes_update.__doc__