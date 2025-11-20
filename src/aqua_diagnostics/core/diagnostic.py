import os
import xarray as xr
from aqua import Reader
from aqua.exceptions import NotEnoughDataError
from aqua.logger import log_configure
from aqua.configurer import ConfigPath
from aqua.util import load_yaml, convert_units
from aqua.util import xarray_to_pandas_freq, pandas_freq_to_string
from aqua.util import DEFAULT_REALIZATION
from .output_saver import OutputSaver


class Diagnostic():

    def __init__(self, model: str, exp: str, source: str,
                 catalog: str | None = None, regrid: str | None = None,
                 startdate: str | None = None, enddate: str | None = None, loglevel: str = 'WARNING'):
        """
        Initialize the diagnostic class. This is a general purpose class that can be used
        by the diagnostic classes to retrieve data from a single model and to save the data
        to a netcdf file. It is not a working diagnostic class by itself.

        Args:
            model (str): The model to be used.
            exp (str): The experiment to be used.
            source (str): The source to be used.
            catalog (str): The catalog to be used. If None, the catalog will be determined by the Reader.
            regrid (str | None): The target grid to be used for regridding. If None, no regridding will be done.
            startdate (str | None): The start date of the data to be retrieved.
                        If None, all available data will be retrieved.
            enddate (str | None): The end date of the data to be retrieved.
                           If None, all available data will be retrieved.
            loglevel (str): The log level to be used. Default is 'WARNING'.
        """

        self.logger = log_configure(log_name='Diagnostic', log_level=loglevel)
        self.loglevel = loglevel
        self.catalog = catalog
        self.model = model
        self.exp = exp
        self.source = source
        self.realization = None

        self.regrid = regrid
        self.startdate = startdate
        self.enddate = enddate

        # Data to be retrieved
        self.data = None

    def retrieve(self, var: str | None = None, reader_kwargs: dict = {},
                 months_required: int | None = None):
        """
        Retrieve the data from the model.

        Args:
            var (str | None): The variable to be retrieved. If None, all variables will be retrieved.
            reader_kwargs (dict): Additional keyword arguments to be passed to the Reader.
            months_required (int | None): The number of months of data required. If None, no check will be performed.

        Attributes:
            self.data: The data retrieved from the model. If return_data is True, the data will be returned.
            self.catalog: The catalog used to retrieve the data if no catalog was provided.
        """
        self.data, self.reader, self.catalog = self._retrieve(model=self.model, exp=self.exp, source=self.source,
                                                              var=var, catalog=self.catalog, startdate=self.startdate,
                                                              enddate=self.enddate, regrid=self.regrid,
                                                              reader_kwargs=reader_kwargs, months_required=months_required,
                                                              loglevel=self.logger.level)

        self.realization = reader_kwargs['realization'] if 'realization' in reader_kwargs else DEFAULT_REALIZATION

        if self.regrid is not None:
            self.logger.info(f'Regridded data to {self.regrid} grid')
        if self.startdate is None:
            self.startdate = self.data.time.values[0]
            self.logger.debug(f'Start date: {self.startdate}')
        if self.enddate is None:
            self.enddate = self.data.time.values[-1]
            self.logger.debug(f'End date: {self.enddate}')

    def save_netcdf(self, data, diagnostic: str, diagnostic_product: str = None,
                    outputdir: str = '.', rebuild: bool = True,
                    create_catalog_entry: bool = False, dict_catalog_entry: dict = None, **kwargs):
        """
        Save the data to a netcdf file.

        Args:
            data (xarray Dataset or DataArray): The data to be saved.
            diagnostic (str): The diagnostic name.
            diagnostic_product (str): The diagnostic product.
            outputdir(str): The path to save the data. Default is '.'.
            rebuild (bool): If True, the netcdf file will be rebuilt. Default is True.
            create_catalog_entry (bool): If True, a catalog entry will be created. Default is False.
            dict_catalog_entry (dict, optional): List of jinja and wildcard variables. Default is None.
                                                 Keys are 'jinjalist' and 'wildcardlist'.

        Keyword Args:
            **kwargs: Additional keyword arguments to be passed to the OutputSaver.save_netcdf method.
        """
        if isinstance(data, xr.Dataset) is False and isinstance(data, xr.DataArray) is False:
            self.logger.error('Data to save as netcdf must be an xarray Dataset or DataArray')

        outputsaver = OutputSaver(diagnostic=diagnostic, 
                                  catalog=self.catalog, model=self.model, exp=self.exp,
                                  realization=self.realization,
                                  outputdir=outputdir, loglevel=self.loglevel)


        outputsaver.save_netcdf(dataset=data, diagnostic_product=diagnostic_product, rebuild=rebuild,
                                create_catalog_entry=create_catalog_entry, dict_catalog_entry=dict_catalog_entry,
                                **kwargs)

    @staticmethod
    def _retrieve(model: str, exp: str, source: str, var: str = None, catalog: str = None,
                  startdate: str = None, enddate: str = None, regrid: str = None,
                  months_required: int | None = None,
                  reader_kwargs: dict = {}, loglevel: str = 'WARNING'):
        """
        Static method to retrieve data and return everything instead of updating class
        attributes. Used internally by the retrieve method

        Args:
            model (str): model of the dataset to retrieve.
            exp (str): exp of the dataset to retrieve.
            source (str): source of the dataset to retrieve.
            var (str or list): variable to retrieve. If None all are retrieved.
            catalog (str): catalog of the dataset to retrieve.
            startdate (str): The start date of the data to be retrieved.
                             If None, all available data will be retrieved.
            enddate (str): The end date of the data to be retrieved.
                           If None, all available data will be retrieved.
            regrid (str): The target grid to be used for regridding. If None, no regridding will be done.
            months_required (int or None): The minimal amount of months to have results. If they are not met, a NotEnoughDataError will be raised.
            reader_kwargs (dict): Additional keyword arguments to be passed to the Reader.
            loglevel (str): The log level to be used. Default is 'WARNING'.

        Returns:
            data (xarray Dataset or DataArray): The data retrieved from the model.
            reader (aqua.Reader): The reader object used to retrieve the data.
            catalog (str): The catalog used to retrieve the data.
        """
        reader = Reader(catalog=catalog, model=model, exp=exp, source=source,
                        regrid=regrid,
                        loglevel=loglevel, **reader_kwargs)

        data = reader.retrieve(var=var)

        # If the data is empty, raise an error
        if not data:
            raise ValueError(f"No data found for {model} {exp} {source} with variable {var}")
        
        data = data.sel(time=slice(startdate, enddate))
        
        # If there is a month requirement we infer the data frequency,
        # then we check how many months are available in the data
        # and finally raise an error if the requirement is not met.
        if months_required is not None:
            timedelta = xarray_to_pandas_freq(data)
            freq = pandas_freq_to_string(timedelta)
            factor = {
                'hourly': 1/(24*30),
                'daily': 1/30,
                'weekly': 1/4,
                'monthly': 1,
                'seasonal': 3,
                'annual': 12
            }
            # We automatically raise an error if the frequency is not pandas compliant
            months = len(data['time']) * factor.get(freq, 0)

            if months < months_required:
                raise NotEnoughDataError(f"Not enough months of data found for {model} {exp} {source}, at least {months_required} months required, only {months} found.")

        if catalog is None:
            catalog = reader.catalog

        if regrid is not None:
            data = reader.regrid(data)

        return data, reader, catalog

    def _check_data(self, data: xr.DataArray, var: str, units: str):
        """
        Make sure that the data is in the correct units.

        Args:
            data (xarray DataArray): The data to be checked.
            var (str): The variable to be checked.
            units (str): The units to be checked.
        """
        final_units = units
        initial_units = data.units

        conversion = convert_units(initial_units, final_units)

        factor = conversion.get('factor', 1)
        offset = conversion.get('offset', 0)

        if factor != 1 or offset != 0:
            self.logger.debug('Converting %s from %s to %s',
                              var, initial_units, final_units)
            data = data * factor + offset
            data.attrs['units'] = final_units

        return data

    def _get_default_regions_file(self, diagnostic):
        """
        Get the default path to the regions file for the given diagnostic.

        Args:
            diagnostic (str): The diagnostic name. Used for creating the diagnostic file paths.

        Returns:
            str: The path to the regions file.
        """
        regions_file = ConfigPath().get_config_dir()
        regions_file = os.path.join(regions_file, 'tools', diagnostic, 'definitions', 'regions.yaml')
        if os.path.exists(regions_file):
            return regions_file
        else:
            raise FileNotFoundError(f'Region file path not found at: {regions_file}')

    def _read_regions_file(self, regions_file: str):
        """
        Read the regions list from the regions file.

        Args:
            regions_file (str): The path to the regions file.

        Returns:
            dict: A dictionary containing the regions and their properties form parsed YAML file.
        """
        return load_yaml(regions_file)

    def _load_regions_from_file(self, diagnostic: str = None, regions_file_path: str = None) -> dict:
        """
        Retrieve the regions dictionary from the specified or default regions file.

        Args:
            diagnostic (str): The diagnostic name.
            regions_file_path (str, optional): Path to a custom regions file. 
                If None, the default path for the diagnostic will be used.

        Returns:
            dict: A dictionary containing the regions and their properties.
        """
        if regions_file_path is None:
            regions_file_path = self._get_default_regions_file(diagnostic)

        return self._read_regions_file(regions_file_path)

    def _set_region(self, diagnostic: str, region: str = None, regions_file_path: str = None,
                    lon_limits: list = None, lat_limits: list = None):
        """
        Set the region to be used.

        Args:
            diagnostic (str): The diagnostic name. Used for creating the diagnostic file paths.
            region (str): The region to select. This will define the lon and lat limits.
            regions_file_path (str): The path to the regions file. If None, the default regions file will be used.
            lon_limits (list): The longitude limits to be used. Overridden by region.
            lat_limits (list): The latitude limits to be used. Overridden by region.

        Returns:
            region (str): The region name to be used.
            lon_limits (list): The longitude limits to be used.
            lat_limits (list): The latitude limits to be used.
        """
        if region is not None:
            regions_file = self._load_regions_from_file(diagnostic, regions_file_path)

            if region in regions_file['regions']:
                lon_limits = regions_file['regions'][region].get('lon_limits', None)
                lat_limits = regions_file['regions'][region].get('lat_limits', None)
                region = regions_file['regions'][region].get('longname', region)
                self.logger.info(f'Region {region} found, using lon: {lon_limits}, lat: {lat_limits}')
            else:
                self.logger.error(f'Region {region} not found')
                raise ValueError(f'Region {region} not found')
        else:
            region = None
            self.logger.info(f'No region provided, using lon_limits: {lon_limits}, lat_limits: {lat_limits}')

        return region, lon_limits, lat_limits

    def select_region(self, region: str = None, diagnostic: str = None, drop: bool = True):
        """
        Selects a geographic region from the dataset and updates self.data accordingly.

        If a region name is provided, the method filters the data using the region's
        predefined latitude and longitude bounds. The selected region name is stored
        in the dataset attributes.

        It uses the `_select_region` method to perform the selection on the `self.data` attribute.
        Use the hidden `_select_region` method if you want to select a region on a different dataset.

        Args:
            region (str, optional): Name of the region to select. If None, no filtering is applied.
            diagnostic (str, optional): Diagnostic category used to determine region bounds.
            drop (bool, optional): Whether to drop coordinates outside the selected region. Default is True.

        Returns:
            tuple: (region, lon_limits, lat_limits)
        """
        res_dict = self._select_region(data=self.data, region=region, diagnostic=diagnostic, drop=drop)
        return res_dict['region'], res_dict['lon_limits'], res_dict['lat_limits']

    def _select_region(self, data: xr.Dataset, region: str = None, diagnostic: str = None, drop: bool = True, **kwargs):
        """
        Select a geographic region from the dataset. Used when selection is not on the self.data attribute.

        Args:
            data (xarray Dataset or DataArray): The dataset to select the region from.
            region (str): The region to select.
            lon_limits (list): The longitude limits to select.
            lat_limits (list): The latitude limits to select.
            drop (bool): Whether to drop coordinates outside the selected region.
            **kwargs: Additional keyword arguments passed to the select_area reader method.

        Returns:
            dict: A dictionary containing the modified dataset and region information.
            The dictionary contains:
                - 'data': The modified dataset with the selected region.
                - 'region': The name of the selected region.
                - 'lon_limits': The longitude limits of the selected region.
                - 'lat_limits': The latitude limits of the selected region.
        """
        original_name = data.name if isinstance(data, xr.DataArray) else None

        if region is not None and diagnostic is not None:
            region, lon_limits, lat_limits = self._set_region(region=region, diagnostic=diagnostic)
            self.logger.info(f"Applying area selection for region: {region}")
            data = self.reader.select_area(
                data=data, lat=lat_limits, lon=lon_limits, drop=drop, **kwargs
            )
            data.attrs['AQUA_region'] = region

            if original_name is not None:
                data.name = original_name
        else:
            region, lon_limits, lat_limits = None, None, None
            self.logger.warning(
                "Since region name is not specified, processing whole region in the dataset"
            )

        res_dict = {
            'data': data,
            'region': region,
            'lon_limits': lon_limits,
            'lat_limits': lat_limits
        }
        return res_dict
