import os
import xarray as xr
from aqua import Reader
from aqua.logger import log_configure
from aqua.util import ConfigPath
from aqua.util import load_yaml, convert_units
from aqua.util import area_selection
from .output_saver import OutputSaver


class Diagnostic():

    def __init__(self, catalog: str = None, model: str = None,
                 exp: str = None, source: str = None, regrid: str = None,
                 startdate: str = None, enddate: str = None, loglevel: str = 'WARNING'):
        """
        Initialize the diagnostic class. This is a general purpose class that can be used
        by the diagnostic classes to retrieve data from a single model and to save the data
        to a netcdf file. It is not a working diagnostic class by itself.

        Args:
            catalog (str): The catalog to be used. If None, the catalog will be determined by the Reader.
            model (str): The model to be used.
            exp (str): The experiment to be used.
            source (str): The source to be used.
            regrid (str): The target grid to be used for regridding. If None, no regridding will be done.
            startdate (str): The start date of the data to be retrieved.
                             If None, all available data will be retrieved.
            enddate (str): The end date of the data to be retrieved.
                           If None, all available data will be retrieved.
            loglevel (str): The log level to be used. Default is 'WARNING'.
        """

        self.logger = log_configure(log_name='Diagnostic', log_level=loglevel)

        self.loglevel = loglevel
        self.catalog = catalog
        self.model = model
        self.exp = exp
        self.source = source

        if self.model is None or self.exp is None or self.source is None:
            raise ValueError('Model, experiment and source must be provided')

        self.regrid = regrid
        self.startdate = startdate
        self.enddate = enddate

        # Data to be retrieved
        self.data = None

    def retrieve(self, var: str = None, reader_kwargs: dict = {}):
        """
        Retrieve the data from the model.

        Args:
            var (str): The variable to be retrieved. If None, all variables will be retrieved.
            reader_kwargs (dict): Additional keyword arguments to be passed to the Reader.

        Attributes:
            self.data: The data retrieved from the model. If return_data is True, the data will be returned.
            self.catalog: The catalog used to retrieve the data if no catalog was provided.
        """
        self.data, self.reader, self.catalog = self._retrieve(model=self.model, exp=self.exp, source=self.source,
                                                              var=var, catalog=self.catalog, startdate=self.startdate,
                                                              enddate=self.enddate, regrid=self.regrid,
                                                              loglevel=self.logger.level, reader_kwargs=reader_kwargs)
        if self.regrid is not None:
            self.logger.info(f'Regridded data to {self.regrid} grid')
        if self.startdate is None:
            self.startdate = self.data.time.values[0]
            self.logger.debug(f'Start date: {self.startdate}')
        if self.enddate is None:
            self.enddate = self.data.time.values[-1]
            self.logger.debug(f'End date: {self.enddate}')

    def save_netcdf(self, data, diagnostic: str, diagnostic_product: str = None,
                    outdir: str = '.', rebuild: bool = True, **kwargs):
        """
        Save the data to a netcdf file.

        Args:
            data (xarray Dataset or DataArray): The data to be saved.
            diagnostic (str): The diagnostic name.
            diagnostic_product (str): The diagnostic product.
            outdir(str): The path to save the data. Default is '.'.
            rebuild (bool): If True, the netcdf file will be rebuilt. Default is True.

        Keyword Args:
            **kwargs: Additional keyword arguments to be passed to the OutputSaver.save_netcdf method.
        """
        if isinstance(data, xr.Dataset) is False and isinstance(data, xr.DataArray) is False:
            self.logger.error('Data to save as netcdf must be an xarray Dataset or DataArray')

        outputsaver = OutputSaver(diagnostic=diagnostic, 
                                  catalog=self.catalog, model=self.model, exp=self.exp,
                                  outdir=outdir, loglevel=self.logger.level)

        outputsaver.save_netcdf(dataset=data, diagnostic_product=diagnostic_product, **kwargs)

    @staticmethod
    def _retrieve(model: str, exp: str, source: str, var: str = None, catalog: str = None,
                  startdate: str = None, enddate: str = None, regrid: str = None,
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
            reader_kwargs (dict): Additional keyword arguments to be passed to the Reader.
            loglevel (str): The log level to be used. Default is 'WARNING'.

        Returns:
            data (xarray Dataset or DataArray): The data retrieved from the model.
            reader (aqua.Reader): The reader object used to retrieve the data.
            catalog (str): The catalog used to retrieve the data.
        """
        reader = Reader(catalog=catalog, model=model, exp=exp, source=source,
                        regrid=regrid, startdate=startdate, enddate=enddate,
                        loglevel=loglevel, **reader_kwargs)

        data = reader.retrieve(var=var)

        # If the data is empty, raise an error
        if not data:
            raise ValueError(f"No data found for {model} {exp} {source} with variable {var}")

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

    def _set_region(self, diagnostic: str, region: str = None, lon_limits: list = None, lat_limits: list = None):
        """
        Set the region to be used.

        Args:
            diagnostic (str): The diagnostic name. Used for creating file paths.
            region (str): The region to select. This will define the lon and lat limits.
            lon_limits (list): The longitude limits to be used. Overridden by region.
            lat_limits (list): The latitude limits to be used. Overridden by region.

        Returns:
            region (str): The region name to be used.
            lon_limits (list): The longitude limits to be used.
            lat_limits (list): The latitude limits to be used.
        """
        if region is not None:
            region_file = ConfigPath().get_config_dir()
            region_file = os.path.join(region_file, 'diagnostics',
                                       diagnostic, 'definitions', 'regions.yaml')
            if os.path.exists(region_file):
                regions = load_yaml(region_file)
                if region in regions['regions']:
                    lon_limits = regions['regions'][region].get('lon_limits', None)
                    lat_limits = regions['regions'][region].get('lat_limits', None)
                    region = regions['regions'][region].get('longname', region)
                    self.logger.info(f'Region {region} found, using lon: {lon_limits}, lat: {lat_limits}')
                else:
                    self.logger.error('Region %s not found', region)
                    raise ValueError(f'Region {region} not found')
            else:
                self.logger.error('Region file not found')
                raise FileNotFoundError('Region file not found')
        else:
            region = None
            self.logger.info('No region provided, using lon_limits: %s, lat_limits: %s', lon_limits, lat_limits)

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
            data (xarray Dataset): The dataset to select the region from.
            region (str): The region to select.
            lon_limits (list): The longitude limits to select.
            lat_limits (list): The latitude limits to select.
            drop (bool): Whether to drop coordinates outside the selected region.
            **kwargs: Additional keyword arguments passed to the area_selection function.

        Returns:
            dict: A dictionary containing the modified dataset and region information.
            The dictionary contains:
                - 'data': The modified dataset with the selected region.
                - 'region': The name of the selected region.
                - 'lon_limits': The longitude limits of the selected region.
                - 'lat_limits': The latitude limits of the selected region.
        """
        if region is not None and diagnostic is not None:
            region, lon_limits, lat_limits = self._set_region(region=region, diagnostic=diagnostic)
            self.logger.info(f"Applying area selection for region: {region}")
            data = area_selection(
                data=data, lat=lat_limits, lon=lon_limits, drop=drop, loglevel=self.loglevel, **kwargs
            )
            data.attrs['AQUA_region'] = region
            self.logger.info(f"Modified longname of the region: {region}")
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