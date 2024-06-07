import numpy as np
import xarray as xr
from typing import Union, Tuple, Optional, Any, List
from aqua.logger import log_configure

from .tropical_rainfall_plots import PlottingClass
from .tropical_rainfall_tools import ToolsClass
from .tropical_rainfall_main import TropicalPrecipitationDataManager

class ExtraFunctionalityClass: 
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
        
        
    def get_seasonal_or_monthly_data(self, data: xr.DataArray, preprocess: bool = True, seasons_bool: bool = True,
                                     model_variable: str = None, trop_lat: float = None, new_unit: str = None) -> xr.DataArray:
        """
        Function to retrieve seasonal or monthly data.

        Args:
            data (xarray.DataArray): Data to be processed.
            preprocess (bool, optional): If True, the data will be preprocessed. Default is True.
            seasons_bool (bool, optional): If True, the data will be calculated for the seasons. Default is True.
            model_variable (str, optional): Name of the model variable. Default is 'mtpr'.
            trop_lat (float, optional): Latitude of the tropical region. Default is None.
            new_unit (str, optional): New unit of the data. Default is None.

        Returns:
            xarray.DataArray: Seasonal or monthly data.
        """
        self.datamanager.class_attributes_update(trop_lat=trop_lat, model_variable=model_variable, new_unit=new_unit)
        if seasons_bool:
            seasons = {
                'DJF_1': {'s_month': 12, 'f_month': 12},
                'DJF_2': {'s_month': 1, 'f_month': 2},
                'MAM': {'s_month': 3, 'f_month': 5},
                'JJA': {'s_month': 6, 'f_month': 8},
                'SON': {'s_month': 9, 'f_month': 11}
            }

            global_data = self.datamanager.preprocessing(data, preprocess=preprocess, trop_lat=self.trop_lat,
                                             model_variable=self.model_variable, new_unit=self.new_unit)

            preprocessed_data = {}
            for key, value in seasons.items():
                preprocessed_data[key] = self.datamanager.preprocessing(data, preprocess=preprocess, trop_lat=self.trop_lat,
                                                            model_variable=self.model_variable, s_month=value['s_month'],
                                                            f_month=value['f_month'])
                if self.new_unit is not None:
                    preprocessed_data[key] = self.datamanager.precipitation_rate_units_converter(preprocessed_data[key],
                                                                                     new_unit=self.new_unit)

            DJF_data = xr.concat([preprocessed_data['DJF_1'], preprocessed_data['DJF_2']], dim='time')
            seasonal_data = [DJF_data, preprocessed_data['MAM'], preprocessed_data['JJA'], preprocessed_data['SON'],
                             global_data]

            return seasonal_data
        else:
            all_monthly_data = []
            for i in range(1, 13):
                if preprocess:
                    monthly_data = self.datamanager.preprocessing(data, preprocess=preprocess, trop_lat=self.trop_lat,
                                                      model_variable=self.model_variable, s_month=i, f_month=i)
                    if self.new_unit is not None:
                        monthly_data = self.datamanager.precipitation_rate_units_converter(monthly_data, new_unit=self.new_unit)
                all_monthly_data.append(monthly_data)
            return all_monthly_data

    def seasonal_or_monthly_mean(self, data: xr.DataArray, preprocess: bool = True, seasons_bool: bool = True,
                                 model_variable: str = None, trop_lat: float = None, new_unit: str = None,
                                 coord: str = None, positive: bool = True) -> xr.DataArray:
        """ Function to calculate the seasonal or monthly mean of the data.

        Args:
            data (xarray.DataArray):        Data to be calculated.
            preprocess (bool, optional):    If True, the data will be preprocessed.                 The default is True.
            seasons_bool (bool, optional):       If True, the data will be calculated for the seasons.   The default is True.
            model_variable (str, optional): Name of the model variable.                             The default is 'mtpr'.
            trop_lat (float, optional):     Latitude of the tropical region.                        The default is None.
            new_unit (str, optional):       New unit of the data.                                   The default is None.
            coord (str, optional):          Name of the coordinate.                                 The default is None.

        Returns:
            xarray.DataArray:             Seasonal or monthly mean of the data.

        """

        self.datamanager.class_attributes_update(trop_lat=trop_lat, model_variable=model_variable, new_unit=new_unit)
        if seasons_bool:
            [DJF, MAM, JJA, SON, glob] = self.get_seasonal_or_monthly_data(data, preprocess=preprocess,
                                                                           seasons_bool=seasons_bool,
                                                                           model_variable=self.model_variable,
                                                                           trop_lat=self.trop_lat, new_unit=self.new_unit)
            if positive:
                DJF = np.maximum(DJF, 0.)
                MAM = np.maximum(MAM, 0.)
                JJA = np.maximum(JJA, 0.)
                SON = np.maximum(SON, 0.)
                glob = np.maximum(glob, 0.)
            glob_mean = glob.mean('time')
            DJF_mean = DJF.mean('time')
            MAM_mean = MAM.mean('time')
            JJA_mean = JJA.mean('time')
            SON_mean = SON.mean('time')
            if coord == 'lon' or coord == 'lat':
                DJF_mean = DJF_mean.mean(coord)
                MAM_mean = MAM_mean.mean(coord)
                JJA_mean = JJA_mean.mean(coord)
                SON_mean = SON_mean.mean(coord)
                glob_mean = glob_mean.mean(coord)
            seasons = [DJF_mean, MAM_mean, JJA_mean, SON_mean, glob_mean]
            return seasons
        else:
            months = self.get_seasonal_or_monthly_data(data, preprocess=preprocess, seasons_bool=seasons_bool,
                                                       model_variable=self.model_variable, trop_lat=self.trop_lat,
                                                       new_unit=self.new_unit)

            for i in range(1, 13):
                mon_mean = months[i].mean('time')
                months[i] = mon_mean
            return months

    def plot_bias(self, data: xr.DataArray, dataset_2: xr.DataArray, preprocess: bool = True, seasons_bool: bool = True,
                  model_variable: str = None, figsize: float = None, save: bool = True, trop_lat: float = None,
                  plot_title: str = None, new_unit: str = None, vmin: float = None, vmax: float = None,
                  path_to_pdf: str = None, name_of_file: str = '', pdf_format: bool = True) -> None:
        """ Function to plot the bias of model_variable between two datasets.

        Args:
            data (xarray): First dataset to be plotted
            dataset_2 (xarray):   Second dataset to be plotted
            preprocess (bool, optional):    If True, data is preprocessed.              Defaults to True.
            seasons_bool (bool, optional):  If True, data is plotted in seasons. If False, data is plotted in months.
                                            Defaults to True.
            model_variable (str, optional): Name of the model variable.                 Defaults to 'mtpr'.
            figsize (float, optional):      Size of the figure.                         Defaults to 1.
            trop_lat (float, optional):     Latitude band of the tropical region.       The default is None.
            new_unit (str, optional):       New unit of the data.                       The default is None.
            contour (bool, optional):       If True, contour is plotted.                The default is True.
            path_to_pdf (str, optional):    Path to the pdf file.                       The default is None.
            name_of_file(str, optional):    Name of the file.                           The default is None.
            pdf_format(bool, optional):     If True, the figure is saved in PDF format. The default is True.

        Returns:
            The pyplot figure in the PDF format
        """
        self.datamanager.class_attributes_update(trop_lat=trop_lat, model_variable=model_variable, new_unit=new_unit)

        if seasons_bool:
            months = None
            try:
                seasons = [data.DJF, data.MAM, data.JJA, data.SON, data.Yearly]
            except AttributeError:
                seasons = self.seasonal_or_monthly_mean(data, preprocess=preprocess, seasons_bool=seasons_bool,
                                                        model_variable=self.model_variable, trop_lat=self.trop_lat,
                                                        new_unit=self.new_unit)
            try:
                seasons = [dataset_2.DJF, dataset_2.MAM, dataset_2.JJA, dataset_2.SON, dataset_2.Yearly]
            except AttributeError:
                seasons_2 = self.seasonal_or_monthly_mean(dataset_2, preprocess=preprocess,
                                                          seasons_bool=seasons_bool, model_variable=self.model_variable,
                                                          trop_lat=self.trop_lat, new_unit=self.new_unit)

            for i in range(0, len(seasons)):
                seasons[i].values = seasons[i].values - seasons_2[i].values
        else:
            seasons = None
            months = self.seasonal_or_monthly_mean(data, preprocess=preprocess, seasons_bool=seasons_bool,
                                                   model_variable=self.model_variable, trop_lat=self.trop_lat,
                                                   new_unit=self.new_unit)

            months_2 = self.seasonal_or_monthly_mean(dataset_2, preprocess=preprocess, seasons_bool=seasons_bool,
                                                     model_variable=self.model_variable, trop_lat=self.trop_lat,
                                                     new_unit=self.new_unit)
            for i in range(0, len(months)):
                months[i].values = months[i].values - months_2[i].values
        if self.new_unit is None:
            try:
                unit = data[self.model_variable].units
            except KeyError:
                unit = data.units
        else:
            unit = self.new_unit
        cbarlabel = self.model_variable+", ["+str(unit)+"]"

        if path_to_pdf is None:
            path_to_pdf = self.path_to_pdf
        if isinstance(path_to_pdf, str) and name_of_file is not None:
            if seasons_bool:
                path_to_pdf = path_to_pdf + 'trop_rainfall_' + name_of_file + '_seasonal_bias.pdf'
            else:
                path_to_pdf = path_to_pdf + 'trop_rainfall_' + name_of_file + '_monthly_bias.pdf'
        return self.plots.plot_seasons_or_months(data=data, cbarlabel=cbarlabel, seasons=seasons, months=months,
                                                 figsize=figsize, plot_title=plot_title,  vmin=vmin, vmax=vmax, save=save,
                                                 path_to_pdf=path_to_pdf, pdf_format=pdf_format)

    def plot_seasons_or_months(self, data: xr.DataArray, preprocess: bool = True, seasons_bool: bool = True,
                               model_variable: str = None, figsize: float = None, save: bool = True,
                               trop_lat: float = None, plot_title: str = None, new_unit: str = None,
                               vmin: float = None, vmax: float = None, get_mean: bool = True, percent95_level: bool = False,
                               path_to_pdf: str = None, path_to_netcdf: str = None, name_of_file: str = '',
                               pdf_format: bool = True, value: float = 0.95, rel_error: float = 0.1) -> None:
        """ Function to plot seasonal data.

        Args:
            data (xarray): First dataset to be plotted
            preprocess (bool, optional):    If True, data is preprocessed.          Defaults to True.
            seasons_bool (bool, optional):  If True, data is plotted in seasons. If False, data is plotted in months.
                                            Defaults to True.
            model_variable (str, optional): Name of the model variable.             Defaults to 'mtpr'.
            figsize (float, optional):      Size of the figure.                     Defaults to 1.
            trop_lat (float, optional):     Latitude of the tropical region.        Defaults to None.
            plot_title (str, optional):     Title of the plot.                      Defaults to None.
            new_unit (str, optional):       Unit of the data.                       Defaults to None.
            vmin (float, optional):         Minimum value of the colorbar.          Defaults to None.
            vmax (float, optional):         Maximum value of the colorbar.          Defaults to None.
            contour (bool, optional):       If True, contours are plotted.          Defaults to True.
            path_to_pdf (str, optional):    Path to the pdf file.                   Defaults to None.
            path_to_netcdf (str, optional): Path to the netcdf file.                Defaults to None.
            name_of_file (str, optional):   Name of the pdf file.                   Defaults to None.
            pdf_format (bool, optional):    If True, the figure is saved in PDF format. Defaults to True.

        Returns:
            The pyplot figure in the PDF format
        """

        self.datamanager.class_attributes_update(trop_lat=trop_lat, model_variable=model_variable, new_unit=new_unit)
        if seasons_bool:
            months = None
            try:
                seasons = [data.DJF, data.MAM, data.JJA, data.SON, data.Yearly]
            except AttributeError:
                if get_mean:
                    seasons = self.seasonal_or_monthly_mean(data, preprocess=preprocess, seasons_bool=seasons_bool,
                                                            model_variable=self.model_variable, trop_lat=self.trop_lat,
                                                            new_unit=self.new_unit)
                elif percent95_level:
                    seasons = self.seasonal_095level_into_netcdf(data, reprocess=preprocess, seasons_bool=seasons_bool,
                                                                 new_unit=self.new_unit, model_variable=self.model_variable,
                                                                 path_to_netcdf=path_to_netcdf,
                                                                 name_of_file=name_of_file, trop_lat=self.trop_lat,
                                                                 value=value, rel_error=rel_error)
        else:
            seasons = None
            months = self.seasonal_or_monthly_mean(data, preprocess=preprocess, seasons_bool=seasons_bool,
                                                   model_variable=self.model_variable, trop_lat=self.trop_lat,
                                                   new_unit=self.new_unit)
        if self.new_unit is None:
            try:
                unit = data[self.model_variable].units
            except KeyError:
                unit = data.units
        else:
            unit = self.new_unit
        cbarlabel = self.model_variable+", ["+str(unit)+"]"

        if path_to_pdf is None:
            path_to_pdf = self.path_to_pdf
        if isinstance(path_to_pdf, str) and name_of_file is not None:
            if seasons_bool:
                path_to_pdf = path_to_pdf + 'trop_rainfall_' + name_of_file + '_seasons.pdf'
            else:
                path_to_pdf = path_to_pdf + 'trop_rainfall_' + name_of_file + '_months.pdf'
        return self.plots.plot_seasons_or_months(data=data, cbarlabel=cbarlabel, seasons=seasons, months=months,
                                                 figsize=figsize, plot_title=plot_title,  vmin=vmin, vmax=vmax, save=save,
                                                 path_to_pdf=path_to_pdf, pdf_format=pdf_format)

    def map(self, data, titles: str = None, lonmin: int = -180, lonmax: int = 181, latmin: int = -90, latmax: int = 91,
            cmap: str = None, pacific_ocean: bool = False, atlantic_ocean: bool = False, indian_ocean: bool = False,
            tropical: bool = False, save: bool = True, model_variable: str = None, figsize: int = None,
            number_of_axe_ticks: int = None, number_of_bar_ticks: int = None, fontsize: int = None, trop_lat: float = None,
            plot_title: str = None, new_unit: str = None, vmin: float = None, vmax: float = None, time_selection: str = '01',
            path_to_pdf: str = None, name_of_file: str = '', pdf_format: bool = None):
        """
        Create a map with specified data and various optional parameters.

        Args:
            data (dtype): The data to be used for mapping.
            titles (str): The title for the map.
            lonmin (int): The minimum longitude for the map.
            lonmax (int): The maximum longitude for the map.
            latmin (int): The minimum latitude for the map.
            latmax (int): The maximum latitude for the map.
            pacific_ocean (bool): Whether to include the Pacific Ocean.
            atlantic_ocean (bool): Whether to include the Atlantic Ocean.
            indian_ocean (bool): Whether to include the Indian Ocean.
            tropical (bool): Whether to focus on tropical regions.
            model_variable (str): The model variable to use.
            figsize (int): The size of the figure.
            number_of_axe_ticks (int): The number of ticks to display.
            time_selection (str): The time selection to use.
            fontsize (int): The font size for the plot.
            cmap (str): The color map to use.
            number_of_bar_ticks (int): The number of ticks to display.
            trop_lat (dtype): The latitude for tropical focus.
            plot_title (str): The title for the plot.
            new_unit (dtype): The new unit for the data.
            vmin (dtype): The minimum value for the color scale.
            vmax (dtype): The maximum value for the color scale.
            path_to_pdf (str): The path to save the map as a PDF file.
            name_of_file (str): The name of the file.
            pdf_format (bool): Whether to save the map in PDF format.

        Returns:
            The pyplot figure in the PDF format
        """

        self.datamanager.class_attributes_update(trop_lat=trop_lat, model_variable=model_variable, new_unit=new_unit)
        if path_to_pdf is None:
            path_to_pdf = self.path_to_pdf
        data = data if isinstance(data, list) else [data]
        if self.new_unit is None:
            try:
                unit = data[0][self.model_variable].units
            except KeyError:
                unit = data[0].units
        else:
            unit = self.new_unit

        for i in range(0, len(data)):
            if any((pacific_ocean, atlantic_ocean, indian_ocean, tropical)):
                lonmin, lonmax, latmin, latmax = self.tools.zoom_in_data(trop_lat=self.trop_lat,
                                                                         pacific_ocean=pacific_ocean,
                                                                         atlantic_ocean=atlantic_ocean,
                                                                         indian_ocean=indian_ocean, tropical=tropical)

            if lonmin != -180 or lonmax not in (180, 181):
                data[i] = data[i].sel(lon=slice(lonmin, lonmax))
            if latmin != -90 or latmax not in (90, 91):
                data[i] = data[i].sel(lat=slice(latmin-1, latmax))

            data[i] = data[i].where(data[i] > vmin)

            if data[i].time.size == 1:
                pass
            else:
                time_selection = self.tools.improve_time_selection(data[i], time_selection=time_selection)
                data[i] = data[i].sel(time=time_selection)
                if data[i].time.size != 1:
                    self.logger.error('The time selection went wrong. Please check the value of input time.')

            try:
                data[i] = data[i][self.model_variable]
            except KeyError:
                pass

            if self.new_unit is not None:
                data[i] = self.datamanager.precipitation_rate_units_converter(data[i], model_variable=self.model_variable,
                                                                  new_unit=self.new_unit)

        cbarlabel = self.model_variable+", ["+str(unit)+"]"
        if isinstance(path_to_pdf, str) and name_of_file is not None:
            path_to_pdf = path_to_pdf + 'trop_rainfall_' + name_of_file + '_map.pdf'

        return self.plots.map(data=data, titles=titles, lonmin=lonmin, lonmax=lonmax, latmin=latmin, latmax=latmax,
                              cmap=cmap, fontsize=fontsize, save=save, model_variable=self.model_variable,
                              figsize=figsize, number_of_axe_ticks=number_of_axe_ticks,
                              number_of_bar_ticks=number_of_bar_ticks, cbarlabel=cbarlabel, plot_title=plot_title,
                              vmin=vmin, vmax=vmax, path_to_pdf=path_to_pdf, pdf_format=pdf_format)

    def get_95percent_level(self, data=None, original_hist=None, value: float = 0.95, preprocess: bool = True,
                            rel_error: float = 0.1, model_variable: str = None, new_unit: str = None, weights=None,
                            trop_lat: float = None):
        """
        Calculate the precipitation rate threshold value at which a specified percentage (1 - value) of the data is below it.

        Args:
            data (xarray.Dataset): The dataset containing the data to analyze.
            original_hist (xarray.Dataset): The original histogram of the data (optional).
            value (float): The desired percentage (between 0 and 1) of data below the threshold.
            preprocess (bool): Whether to preprocess the data (e.g., filtering).
            rel_error (float): The relative error allowed when calculating the threshold.
            model_variable (str): The model variable to use for analysis.
            new_unit (str): The desired unit for precipitation rate conversion (optional).
            weights (xarray.DataArray): Weights associated with the data (optional).
            trop_lat (float): The latitude value for tropical focus (optional).

        Returns:
            float: The calculated threshold value for the specified percentage.
            str: The unit of the threshold value.
            float: The actual percentage of data below the threshold.
        """
        self.datamanager.class_attributes_update(trop_lat=trop_lat, model_variable=model_variable, new_unit=new_unit)

        if self.new_unit is not None:
            data = self.datamanager.precipitation_rate_units_converter(data, new_unit=self.new_unit)
            units = self.new_unit

        value = 1 - value
        rel_error = value*rel_error
        if original_hist is None:
            original_hist = self.histogram(data, weights=weights, preprocess=preprocess,
                                           trop_lat=self.trop_lat, model_variable=self.model_variable,
                                           num_of_bins=self.num_of_bins, first_edge=self.first_edge,
                                           width_of_bin=self.width_of_bin, bins=self.bins)

        counts_sum = sum(original_hist.counts)
        relative_value = [float((original_hist.counts[i]/counts_sum).values)
                          for i in range(0, len(original_hist.counts))]
        new_sum = 0

        for i in range(len(relative_value)-1, 0, -1):
            new_sum += relative_value[i]
            if new_sum > 0.05:
                break

        bin_i = float(original_hist.center_of_bin[i-1].values)
        del_bin = float(
            original_hist.center_of_bin[i].values) - float(original_hist.center_of_bin[i-1].values)
        last_bin = float(original_hist.center_of_bin[-1].values)

        self.num_of_bins = None
        self.first_edge = None
        self.width_of_bin = None

        for i in range(0, 100):
            self.bins = np.sort([0, bin_i + 0.5*del_bin, last_bin])
            new_hist = self.histogram(data)

            counts_sum = sum(new_hist.counts.values)
            threshold = new_hist.counts[-1].values/counts_sum
            if abs(threshold-value) < rel_error:
                break
            if threshold < value:
                del_bin = del_bin - abs(0.5*del_bin)
            else:
                del_bin = del_bin + abs(0.5*del_bin)

        try:
            units = data[self.model_variable].units
        except KeyError:
            units = data.units

        bin_value = bin_i + del_bin

        return bin_value, units, 1 - threshold

    def seasonal_095level_into_netcdf(self, data, preprocess: bool = True, seasons_bool: bool = True,
                                      model_variable: str = None, path_to_netcdf: str = None, name_of_file: str = None,
                                      trop_lat: float = None, value: float = 0.95, rel_error: float = 0.1,
                                      new_unit: str = None, lon_length: int = None, lat_length: int = None,
                                      space_grid_factor: int = None, tqdm: bool = False):
        """ Function to plot.

        Args:
            data (xarray): The data to be used for plotting.
            preprocess (bool): Whether to preprocess the data.
            seasons_bool (bool): Whether to use seasons for plotting.
            model_variable (str): The model variable to use for plotting.
            path_to_netcdf (str): The path to the netCDF file.
            name_of_file (str): The name of the file.
            trop_lat (float): The latitude value for the tropical region.
            value (float): The specified value for calculation.
            rel_error (float): The relative error allowed for the threshold.
            new_unit (str): The new unit for the data.
            lon_length (int): The length of the longitude.
            lat_length (int): The length of the latitude.
            space_grid_factor (int): The factor for the space grid.
            tqdm (bool): Whether to show the progress bar.

        Returns:
            The calculated seasonal 95th percentile level.
        """
        self.datamanager.class_attributes_update(trop_lat=trop_lat, model_variable=model_variable, new_unit=new_unit)

        data = self.tools.space_regrider(data, space_grid_factor=space_grid_factor,
                                         lat_length=lat_length, lon_length=lon_length)

        self.datamanager.class_attributes_update(trop_lat=trop_lat)
        if seasons_bool:
            [DJF, MAM, JJA, SON, glob] = self.get_seasonal_or_monthly_data(data, preprocess=preprocess,
                                                                           seasons_bool=seasons_bool,
                                                                           model_variable=self.model_variable,
                                                                           trop_lat=trop_lat,
                                                                           new_unit=self.new_unit)

            num_of_bins, first_edge, width_of_bin, bins = self.num_of_bins, self.first_edge, self.width_of_bin, self.bins
            self.s_month, self.f_month = None, None
            s_month, f_month = None, None
            progress_bar_template = "[{:<40}] {}%"
            for lat_i in range(0, DJF.lat.size):

                for lon_i in range(0, DJF.lon.size):
                    if tqdm:
                        ratio = ((DJF.lon.size-1)*lat_i + lon_i) / \
                            (DJF.lat.size*DJF.lon.size)
                        progress = int(40 * ratio)
                        print(progress_bar_template.format(
                            "=" * progress, int(ratio * 100)), end="\r")

                    self.datamanager.class_attributes_update(s_month=s_month, f_month=f_month, num_of_bins=num_of_bins,
                                                 first_edge=first_edge, width_of_bin=width_of_bin, bins=bins)
                    DJF_095level = DJF.isel(time=0).copy(deep=True)
                    self.logger.debug('DJF:{}'.format(DJF))
                    bin_value, units, threshold = self.get_95percent_level(DJF.isel(lat=lat_i).isel(lon=lon_i),
                                                                           preprocess=False, value=value, rel_error=rel_error)
                    DJF_095level.isel(lat=lat_i).isel(
                        lon=lon_i).values = bin_value

                    self.datamanager.class_attributes_update(s_month=s_month, f_month=f_month, num_of_bins=num_of_bins,
                                                 first_edge=first_edge, width_of_bin=width_of_bin, bins=bins)
                    MAM_095level = MAM.isel(time=0).copy(deep=True)
                    bin_value, units, threshold = self.get_95percent_level(MAM.isel(lat=lat_i).isel(lon=lon_i),
                                                                           preprocess=False, value=value, rel_error=rel_error)
                    MAM_095level.isel(lat=lat_i).isel(
                        lon=lon_i).values = bin_value

                    self.datamanager.class_attributes_update(s_month=s_month, f_month=f_month, num_of_bins=num_of_bins,
                                                 first_edge=first_edge, width_of_bin=width_of_bin, bins=bins)
                    JJA_095level = JJA.isel(time=0).copy(deep=True)
                    bin_value, units, threshold = self.get_95percent_level(JJA.isel(lat=lat_i).isel(lon=lon_i),
                                                                           preprocess=False, value=value, rel_error=rel_error)
                    JJA_095level.isel(lat=lat_i).isel(
                        lon=lon_i).values = bin_value

                    self.datamanager.class_attributes_update(s_month=s_month, f_month=f_month, num_of_bins=num_of_bins,
                                                 first_edge=first_edge, width_of_bin=width_of_bin, bins=bins)
                    SON_095level = SON.isel(time=0).copy(deep=True)
                    bin_value, units, threshold = self.get_95percent_level(SON.isel(lat=lat_i).isel(lon=lon_i),
                                                                           preprocess=False, value=value, rel_error=rel_error)
                    SON_095level.isel(lat=lat_i).isel(
                        lon=lon_i).values = bin_value

                    self.datamanager.class_attributes_update(s_month=s_month, f_month=f_month, num_of_bins=num_of_bins,
                                                 first_edge=first_edge, width_of_bin=width_of_bin, bins=bins)
                    glob_095level = glob.isel(time=0).copy(deep=True)
                    bin_value, units, threshold = self.get_95percent_level(glob.isel(lat=lat_i).isel(lon=lon_i),
                                                                           preprocess=False, value=value, rel_error=rel_error)
                    glob_095level.isel(lat=lat_i).isel(
                        lon=lon_i).values = bin_value

            seasonal_095level = DJF_095level.to_dataset(name="DJF")
            seasonal_095level["MAM"] = MAM_095level
            seasonal_095level["JJA"] = JJA_095level
            seasonal_095level["SON"] = SON_095level
            seasonal_095level["Yearly"] = glob_095level

            s_month, f_month = None, None
            self.datamanager.class_attributes_update(s_month=s_month, f_month=f_month)

            seasonal_095level.attrs = SON.attrs
            seasonal_095level = self.datamanager.grid_attributes(
                data=SON, mtpr_dataset=seasonal_095level)
            for variable in ('DJF', 'MAM', 'JJA', 'SON', 'Yearly'):
                seasonal_095level[variable].attrs = SON.attrs
                seasonal_095level = self.datamanager.grid_attributes(
                    data=SON, mtpr_dataset=seasonal_095level, variable=variable)

        if seasonal_095level.time_band == []:
            raise Exception('Time band is empty')
        if isinstance(path_to_netcdf, str) and name_of_file is not None:
            self.datamanager.dataset_to_netcdf(
                seasonal_095level, path_to_netcdf=path_to_netcdf, name_of_file=name_of_file)
        else:
            return seasonal_095level

    