import pandas as pd
import xarray as xr
from aqua.logger import log_configure
from aqua.diagnostics.core import Diagnostic
from aqua.util import area_selection
from .util import predefined_regions, _get_std_anomaly
from itertools import product

xr.set_options(keep_attrs=True)


class Hovmoller(Diagnostic):
    """
    A class for generating Hovmoller diagrams from ocean model data.

    This class provides methods to retrieve, process, and save netCDF files
    for Hovmoller diagrams. It inherits from the `Diagnostic` class.

    Attributes:
        logger (Logger): Logger instance for the class.
        outputdir (str): Directory to save the output files.
        region (str): Region for area selection.
        var (list): List of variables to process.
        stacked_data (xarray.Dataset): Processed data for Hovmoller diagrams.
    """

    def __init__(
        self,
        catalog: str = None,
        model: str = None,
        exp: str = None,
        source: str = None,
        regrid: str = None,
        startdate: str = None,
        enddate: str = None,
        loglevel: str = "WARNING",
    ):
        """
        Initializes the Hovmoller class.

        Args:
            catalog (str, optional): Path to the catalog file.
            model (str, optional): Model name.
            exp (str, optional): Experiment name.
            source (str, optional): Data source.
            regrid (str, optional): Regridding method.
            startdate (str, optional): Start date for data retrieval.
            enddate (str, optional): End date for data retrieval.
            loglevel (str, optional): Logging level. Defaults to "WARNING".
        """
        super().__init__(
            catalog=catalog,
            model=model,
            exp=exp,
            source=source,
            regrid=regrid,
            startdate=startdate,
            enddate=enddate,
            loglevel=loglevel,
        )
        self.logger = log_configure(log_name="Hovmoller", log_level=loglevel)

    def run(self, outputdir: str = None, region: str = None, var: list = None):
        if var is None:
            var = ["thetao", "so"]
        """
        Executes the Hovmoller diagram generation process.

        Args:
            outputdir (str, optional): Directory to save the output files.
            region (str, optional): Region for area selection.
            var (list, optional): List of variables to process. Defaults to ["thetao", "so"].
        """
        self.outputdir = outputdir
        self.region = region
        self.var = var
        self.logger.info("Running Hovmoller diagram generation")
        super().retrieve(var = var)
        self.logger.info("Data retrieved successfully")
        self.area_select()
        self.stacked_data = self._data_process_for_drift(dim_mean=["lat", "lon"])
        self.save_netcdf(diagnostic="Hovmoller", diagnostic_product="Hovmoller")
        self.logger.info("Hovmoller diagram saved to netCDF file")

    def area_select(self):
        """
        Applies area selection to the retrieved data.

        If a region is specified, the data is filtered based on the
        predefined region's latitude and longitude bounds.
        """
        if self.region is not None:
            lon_limits, lat_limits = predefined_regions(self.region)
            self.data = area_selection(
                data=self.data, lat=lat_limits, lon=lon_limits, drop=True
            )
        else:
            self.logger.warning("Since region name is not specified, processing whole region in the dataset")
          
    def _data_process_by_type(self, **kwargs):
        """
        Selects the type of timeseries and colormap based on the given parameters.

        Args:
            data (DataArray): Input data containing temperature (thetao) and salinity (so).
            anomaly (bool, optional): Specifies whether to compute anomalies. Defaults to False.
            standardise (bool, optional): Specifies whether to standardize the data. Defaults to False.
            anomaly_ref (str, optional): Reference for the anomaly computation. Valid options: "t0", "tmean". Defaults to None.

        Returns:
            process_data (Dataset): Processed data based on the selected preprocessing approach.
            type (str): Type of preprocessing approach applied
            cmap (str): Colormap to be used for the plot.
        """
        data = kwargs["data"]
        anomaly = kwargs["anomaly"]
        anomaly_ref = kwargs["anomaly_ref"]
        standardise = kwargs["standardise"]

        data = _get_std_anomaly(data, anomaly_ref, standardise, dim = "time")

        return data


    def _data_process_for_drift(self, dim_mean: None, loglevel="WARNING"):
        """
        Processes input data for drift analysis by applying various transformations 
        and aggregations.

        Args:
            data (xarray.DataArray): The input data to be processed.
            dim_mean (str or None): The dimension along which to compute the mean. 
                If None, no mean is computed.
            loglevel (str): The logging level to use during processing. Defaults to "WARNING".

        Returns:
            xarray.DataArray: A concatenated DataArray containing processed data 
            for different combinations of anomaly, standardization, and anomaly reference types.
        """

        if dim_mean is not None:
            data = self.data.mean(dim=dim_mean)
        data_list = []

        for anomaly, standardise, anomaly_ref in product(
            [False, True], [False, True], ["t0", "tmean"]
        ):
            data_proc = self._data_process_by_type(
                data = data,
                anomaly = anomaly,
                standardise = standardise,
                anomaly_ref = anomaly_ref,
                loglevel = loglevel,
            )
            


            # data_proc = data_proc.expand_dims(dim={"type": [type]})

            data_list.append(data_proc)

        stacked_data = xr.concat(data_list, dim="type")
        return stacked_data
            
    def save_netcdf(self, diagnostic = "Ocean3D", diagnostic_product = "Hovmoller", rebuild=True):
        """
        Saves the processed data to a netCDF file.

        Args:
            diagnostic (str): Name of the diagnostic.
            diagnostic_product (str): Name of the diagnostic product.
            rebuild (bool, optional): Whether to rebuild the netCDF file. Defaults to True.
        """
        super().save_netcdf(
            data = self.stacked_data,
            diagnostic = diagnostic,
            diagnostic_product = diagnostic_product,
            outputdir = self.outputdir,
            rebuild = rebuild,
        )
