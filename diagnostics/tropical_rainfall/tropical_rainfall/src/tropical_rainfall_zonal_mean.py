import numpy as np
import xarray as xr
from typing import Union, Tuple, Optional, Any, List
import matplotlib.figure as figure
from aqua.logger import log_configure

from .tropical_rainfall_plots import PlottingClass
from .tropical_rainfall_tools import ToolsClass
from .tropical_rainfall_data_manager import TropicalPrecipitationDataManager

class ZonalMeanClass: 
    """This class is a minimal version of the Tropical Precipitation Diagnostic."""

    def __init__(self,
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
            model_variable (str, optional): The name of the model variable. Defaults to 'mtpr'.
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
        self.plots = PlottingClass(loglevel=loglevel)
        self.tools = ToolsClass(loglevel=loglevel)
        self.path_to_netcdf = self.tools.get_netcdf_path() if path_to_netcdf is None else path_to_netcdf
        self.path_to_pdf = self.tools.get_pdf_path() if path_to_pdf is None else path_to_pdf
        self.datamanager = TropicalPrecipitationDataManager(trop_lat=self.trop_lat, s_time=self.s_time, f_time=self.f_time,
                                                            s_year=self.s_year, f_year=self.f_year, s_month=self.s_month, f_month=self.f_month,
                                                            num_of_bins=self.num_of_bins, first_edge=self.first_edge,
                                                            width_of_bin=None, bins=self.bins, new_unit=self.new_unit, model_variable=self.model_variable,
                                                            path_to_netcdf=self.path_to_netcdf, path_to_pdf=self.path_to_pdf, loglevel=self.loglevel)

        
        
    def mean_along_coordinate(self, data: xr.Dataset, model_variable: str = None, preprocess: bool = True,
                              trop_lat: float = None, coord: str = 'time', glob: bool = False, s_time: str = None,
                              f_time: str = None, positive: bool = True, s_year: str = None, f_year: str = None,
                              new_unit: str = None, s_month: str = None, f_month: str = None) -> xr.Dataset:
        """
        Function to calculate the mean value of variable in Dataset.

        Args:
            data (xarray.Dataset): The Dataset.
            model_variable (str, optional): The variable of the Dataset. Defaults to 'mtpr'.
            trop_lat (float, optional): The maximum and minimal tropical latitude values in Dataset. Defaults to None.
            coord (str, optional): The coordinate of the Dataset. Defaults to 'time'.
            s_time (str, optional): The starting time of the Dataset. Defaults to None.
            f_time (str, optional): The ending time of the Dataset. Defaults to None.
            s_year (str, optional): The starting year of the Dataset. Defaults to None.
            f_year (str, optional): The ending year of the Dataset. Defaults to None.
            s_month (str, optional): The starting month of the Dataset. Defaults to None.
            f_month (str, optional): The ending month of the Dataset. Defaults to None.
            glob (bool, optional): If True, the median value is calculated for all lat and lon. Defaults to False.
            preprocess (bool, optional): If True, the Dataset is preprocessed. Defaults to True.
            positive (bool, optional): The flag to indicate if the data should be positive. Defaults to True.
            new_unit (str, optional): The new unit. Defaults to None.

        Returns:
            xarray.Dataset: The mean value of the variable.
        """
        self.datamanager.class_attributes_update(model_variable=model_variable, new_unit=new_unit)

        if preprocess:
            data = self.datamanager.preprocessing(data, preprocess=preprocess,
                                      model_variable=self.model_variable, trop_lat=self.trop_lat,
                                      s_time=self.s_time, f_time=self.f_time, s_year=self.s_year, f_year=self.f_year,
                                      s_month=None, f_month=None, dask_array=False, new_unit=self.new_unit)
        if positive:
            data = np.maximum(data, 0.)
        coord_lat, coord_lon = self.datamanager.coordinate_names(data)
        if coord in data.dims:

            self.datamanager.class_attributes_update(trop_lat=trop_lat, s_time=s_time, f_time=f_time,
                                                     s_year=s_year, f_year=f_year, s_month=s_month, f_month=f_month)
            if glob:
                return data.mean()
            else:
                if coord == 'time':
                    return data.mean(coord_lat).mean(coord_lon)
                elif coord == coord_lat:
                    return data.mean('time').mean(coord_lon)
                elif coord == coord_lon:
                    return data.mean('time').mean(coord_lat)
        else:
            for i in data.dims:
                coord = i
            return data.median(coord)

    def median_along_coordinate(self, data: xr.Dataset, trop_lat: float = None, preprocess: bool = True,
                                model_variable: str = None, coord: str = 'time', glob: bool = False, s_time: str = None,
                                f_time: str = None, positive: bool = True, s_year: str = None, f_year: str = None,
                                new_unit: str = None, s_month: str = None, f_month: str = None) -> xr.Dataset:
        """
        Function to calculate the median value of a variable in a Dataset.

        Args:
            data (xarray.Dataset): The Dataset.
            model_variable (str, optional): The variable of the Dataset. Defaults to 'mtpr'.
            trop_lat (float, optional): The maximum and minimal tropical latitude values in the Dataset. Defaults to None.
            coord (str, optional): The coordinate of the Dataset. Defaults to 'time'.
            s_time (str, optional): The starting time of the Dataset. Defaults to None.
            f_time (str, optional): The ending time of the Dataset. Defaults to None.
            s_year (str, optional): The starting year of the Dataset. Defaults to None.
            f_year (str, optional): The ending year of the Dataset. Defaults to None.
            s_month (str, optional): The starting month of the Dataset. Defaults to None.
            f_month (str, optional): The ending month of the Dataset. Defaults to None.
            glob (bool, optional): If True, the median value is calculated for all latitudes and longitudes. Defaults to False.
            preprocess (bool, optional): If True, the Dataset is preprocessed. Defaults to True.
            positive (bool, optional): The flag to indicate if the data should be positive. Defaults to True.
            new_unit (str, optional): The new unit. Defaults to None.

        Returns:
            xarray.Dataset: The median value of the variable.
        """
        self.datamanager.class_attributes_update(model_variable=model_variable, new_unit=new_unit)
        if preprocess:
            data = self.datamanager.preprocessing(data, preprocess=preprocess,
                                      model_variable=self.model_variable, trop_lat=self.trop_lat,
                                      s_time=self.s_time, f_time=self.f_time, s_year=self.s_year, f_year=self.f_year,
                                      s_month=None, f_month=None, dask_array=False, new_unit=self.new_unit)

        if positive:
            data = np.maximum(data, 0.)
        coord_lat, coord_lon = self.datamanager.coordinate_names(data)
        if coord in data.dims:
            self.datamanager.class_attributes_update(trop_lat=trop_lat, s_time=s_time, f_time=f_time,
                                                     s_year=s_year, f_year=f_year, s_month=s_month, f_month=f_month)

            if glob:
                return data.median(coord_lat).median(coord_lon).mean('time')
            else:
                if coord == 'time':
                    return data.median(coord_lat).median(coord_lon)
                elif coord == coord_lat:
                    return data.median('time').median(coord_lon)
                elif coord == coord_lon:
                    return data.median('time').median(coord_lat)

        else:
            for i in data.dims:
                coord = i
            return data.median(coord)

    def average_into_netcdf(self, data: xr.Dataset, glob: bool = False, preprocess: bool = True,
                            model_variable: str = None, coord: str = 'lat', trop_lat: float = None,
                            get_mean: bool = True, get_median: bool = False, s_time: str = None,
                            f_time: str = None, s_year: str = None, f_year: str = None, s_month: str = None,
                            f_month: str = None, new_unit: str = None, name_of_file: str = None,
                            seasons_bool: bool = True, path_to_netcdf: str = None) -> xr.Dataset:
        """
        Function to plot the mean or median value of the variable in a Dataset.

        Args:
            data (xarray.Dataset): The Dataset.
            glob (bool, optional): If True, the value is calculated for all latitudes and longitudes. Defaults to False.
            preprocess (bool, optional): If True, the Dataset is preprocessed. Defaults to True.
            model_variable (str, optional): The variable of the Dataset. Defaults to 'mtpr'.
            coord (str, optional): The coordinate of the Dataset. Defaults to 'time'.
            trop_lat (float, optional): The maximumal and minimal tropical latitude values in the Dataset. Defaults to None.
            get_mean (bool, optional): The flag to calculate the mean of the variable. Defaults to True.
            get_median (bool, optional): The flag to calculate the median of the variable. Defaults to False.
            s_time (str, optional): The starting time of the Dataset. Defaults to None.
            f_time (str, optional): The ending time of the Dataset. Defaults to None.
            s_year (str, optional): The starting year of the Dataset. Defaults to None.
            f_year (str, optional): The ending year of the Dataset. Defaults to None.
            s_month (str, optional): The starting month of the Dataset. Defaults to None.
            f_month (str, optional): The ending month of the Dataset. Defaults to None.
            new_unit (str, optional): The unit of the model variable. Defaults to None.
            name_of_file (str, optional): The name of the file. Defaults to None.
            seasons_bool (bool, optional): The flag to calculate the seasonal mean. Defaults to True.
            path_to_netcdf (str, optional): The path to the NetCDF file. Defaults to None.

        Returns:
            xarray.Dataset: The calculated mean or median value of the variable.
        """
        self.datamanager.class_attributes_update(trop_lat=trop_lat, model_variable=model_variable, new_unit=new_unit,
                                     s_time=s_time, f_time=f_time, s_year=s_year, f_year=f_year,
                                     s_month=s_month, f_month=f_month)

        if path_to_netcdf is None and self.path_to_netcdf is not None:
            path_to_netcdf = self.path_to_netcdf+'mean/'

        if preprocess:
            data_with_final_grid = self.datamanager.preprocessing(data, preprocess=preprocess,
                                                      model_variable=self.model_variable, trop_lat=self.trop_lat,
                                                      s_time=self.s_time, f_time=self.f_time, s_year=self.s_year,
                                                      f_year=self.f_year, s_month=None, f_month=None, dask_array=False,
                                                      new_unit=self.new_unit)

        if get_mean:
            if seasons_bool:
                data_average = self.seasonal_or_monthly_mean(data, preprocess=preprocess,
                                                             seasons_bool=seasons_bool, model_variable=self.model_variable,
                                                             trop_lat=self.trop_lat, new_unit=self.new_unit, coord=coord)

                seasonal_average = data_average[0].to_dataset(name="DJF")
                seasonal_average["MAM"], seasonal_average["JJA"] = data_average[1], data_average[2]
                seasonal_average["SON"], seasonal_average["Yearly"] = data_average[3], data_average[4]
            else:
                data_average = self.mean_along_coordinate(data, preprocess=preprocess, glob=glob,
                                                          model_variable=self.model_variable, trop_lat=trop_lat,
                                                          coord=coord, s_time=self.s_time, f_time=self.f_time,
                                                          s_year=self.s_year, f_year=self.f_year,
                                                          s_month=self.s_month, f_month=self.f_month)
        if get_median:
            data_average = self.median_along_coordinate(data, preprocess=preprocess, glob=glob,
                                                        model_variable=self.model_variable, trop_lat=self.trop_lat,
                                                        coord=coord, s_time=self.s_time, f_time=self.f_time,
                                                        s_year=self.s_year, f_year=self.f_year, s_month=self.s_month,
                                                        f_month=self.f_month)

        s_month, f_month = None, None
        self.datamanager.class_attributes_update(s_month=s_month, f_month=f_month)
        if seasons_bool:
            seasonal_average.attrs = data_with_final_grid.attrs
            seasonal_average = self.datamanager.grid_attributes(
                data=data_with_final_grid, mtpr_dataset=seasonal_average)
            for variable in ('DJF', 'MAM', 'JJA', 'SON', 'Yearly'):
                seasonal_average[variable].attrs = data_with_final_grid.attrs
                seasonal_average = self.datamanager.grid_attributes(
                    data=data_with_final_grid, mtpr_dataset=seasonal_average, variable=variable)
            average_dataset = seasonal_average
        else:
            data_average.attrs = data_with_final_grid.attrs
            data_average = self.datamanager.grid_attributes(
                data=data_with_final_grid,      mtpr_dataset=data_average)
            average_dataset = data_average

        if average_dataset.time_band == []:
            raise Exception('Time band is empty')

        if isinstance(path_to_netcdf, str) and name_of_file is not None:
            return self.datamanager.dataset_to_netcdf(
                average_dataset, path_to_netcdf=path_to_netcdf, name_of_file=name_of_file+'_'+str(coord))
        else:
            return average_dataset

    def plot_of_average(self, data: xr.Dataset = None, ymax: int = 12, fontsize: int = None, pad: int = 15, save: bool = True,
                        trop_lat: float = None, get_mean: bool = True, get_median: bool = False, legend: str = '_Hidden',
                        projection: bool = False,
                        figsize: int = None, linestyle: str = None, maxticknum: int = 12, color: str = 'tab:blue',
                        model_variable: str = None, ylogscale: bool = False, xlogscale: bool = False, loc: str = 'upper right',
                        add: figure.Figure = None, fig: figure.Figure = None, plot_title: str = None,
                        path_to_pdf: str = None, new_unit: str = None, name_of_file: str = '', pdf_format: bool = True,
                        path_to_netcdf: str = None) -> None:
        """
        Function to plot the mean or median value of the variable in Dataset.

        Args:
            data (xarray.Dataset): The Dataset.
            ymax (int, optional): The maximum value on the y-axis. Defaults to 12.
            fontsize (int, optional): The font size of the plot. Defaults to None.
            pad (int, optional): The padding value. Defaults to 15.
            save (bool, optional): The flag to save the plot. Defaults to True.
            trop_lat (float, optional): The maximumal and minimal tropical latitude values in the Dataset. Defaults to None.
            get_mean (bool, optional): The flag to calculate the mean of the variable. Defaults to True.
            get_median (bool, optional): The flag to calculate the median of the variable. Defaults to False.
            legend (str, optional): The legend of the plot. Defaults to '_Hidden'.
            figsize (int, optional): The size of the plot. Defaults to None.
            linestyle (str, optional): The line style of the plot. Defaults to None.
            maxticknum (int, optional): The maximum number of ticks on the x-axis. Defaults to 12.
            color (str, optional): The color of the plot. Defaults to 'tab:blue'.
            model_variable (str, optional): The name of the variable. Defaults to None.
            ylogscale (bool, optional): The flag to use a logarithmic scale for the y-axis. Defaults to False.
            xlogscale (bool, optional): The flag to use a logarithmic scale for the x-axis. Defaults to False.
            loc (str, optional): The location of the legend. Defaults to 'upper right'.
            add (matplotlib.figure.Figure, optional): The add previously created figure to plot. Defaults to None.
            fig (matplotlib.figure.Figure, optional): The add previously created figure to plot. Defaults to None.
            plot_title (str, optional): The title of the plot. Defaults to None.
            path_to_pdf (str, optional): The path to the pdf file. Defaults to None.
            new_unit (str, optional): The unit of the model variable. Defaults to None.
            name_of_file (str, optional): The name of the file. Defaults to ''.
            pdf_format (bool, optional): The flag to save the plot in pdf format. Defaults to True.
            path_to_netcdf (str, optional): The path to the NetCDF file. Defaults to None.

        Returns:
            None.
        """
        self.datamanager.class_attributes_update(trop_lat=trop_lat, model_variable=model_variable, new_unit=new_unit)
        if path_to_pdf is None:
            path_to_pdf = self.path_to_pdf

        if data is None and path_to_netcdf is not None:
            data = self.tools.open_dataset(path_to_netcdf=path_to_netcdf)
        elif path_to_netcdf is None and data is None:
            raise Exception('The path or dataset must be provided.')

        coord_lat, coord_lon = self.datamanager.coordinate_names(data)

        if coord_lat is not None:
            coord = coord_lat
            self.logger.debug("Latitude coordinate is used.")
        elif coord_lon is not None:
            coord = coord_lon
            self.logger.debug("Longitude coordinate is used.")
        else:
            raise Exception('Unknown coordinate name')

        if data[coord].size <= 1:
            raise ValueError(
                "The length of the coordinate should be more than 1.")

        if self.new_unit is not None and 'xarray' in str(type(data)):
            data = self.datamanager.precipitation_rate_units_converter(data, new_unit=self.new_unit)
            units = self.new_unit
        else:
            units = data.units
        y_lim_max = self.datamanager.precipitation_rate_units_converter(ymax, old_unit=data.units, new_unit=self.new_unit)

        ylabel = self.model_variable+', '+str(units)
        if plot_title is None:
            if get_mean:
                plot_title = 'Mean values of ' + self.model_variable
            elif get_median:
                plot_title = 'Median values of ' + self.model_variable

        if isinstance(path_to_pdf, str) and name_of_file is not None:
            path_to_pdf = path_to_pdf + 'trop_rainfall_' + name_of_file + 'mean'+'_along_'+str(coord)+'.pdf'

        return self.plots.plot_of_average(data=data, trop_lat=self.trop_lat, ylabel=ylabel, coord=coord, fontsize=fontsize,
                                          pad=pad, y_lim_max=y_lim_max, legend=legend, figsize=figsize, linestyle=linestyle,
                                          maxticknum=maxticknum, color=color, ylogscale=ylogscale, xlogscale=xlogscale,
                                          projection=projection,
                                          loc=loc, add=add, fig=fig, plot_title=plot_title, path_to_pdf=path_to_pdf,
                                          save=save, pdf_format=pdf_format)
        
        