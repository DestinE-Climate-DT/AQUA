from itertools import product
import xarray as xr
from aqua.util import to_list
from aqua.logger import log_configure
from aqua.diagnostics.core import Diagnostic

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
        model: str,
        exp: str,
        source: str,
        catalog: str = None,
        regrid: str = None,
        startdate: str = None,
        enddate: str = None,
        diagnostic_name: str = "oceandrift",
        loglevel: str = "WARNING",
    ):
        """
        Initializes the Hovmoller class.

        Args:
            model (str): Model name.
            exp (str): Experiment name.
            source (str): Data source.
            catalog (str, optional): Path to the catalog file.
            regrid (str, optional): Regridding method.
            startdate (str, optional): Start date for data retrieval.
            enddate (str, optional): End date for data retrieval.
            diagnostic_name (str, optional): Name of the diagnostic for filenames. Defaults to "oceandrift".
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
        self.logger = log_configure(log_name="OceanHovmoller", log_level=loglevel)
        self.diagnostic_name = diagnostic_name
        # Initialize the results list. Elements of the list are dataset with different anomanly ref.
        self.processed_data_list = []

    def run(
        self,
        outputdir: str = ".",
        rebuild: bool = True,
        region: str = None,
        var: list = ["thetao", "so"],
        dim_mean=["lat", "lon"],
        anomaly_ref: str = None,
        reader_kwargs: dict = {},
    ):
        """
        Run the Hovmoller diagram generation workflow.

        This method retrieves the specified variables, applies region selection if provided,
        computes Hovmoller diagrams with optional mean and anomaly processing, and saves the
        results to netCDF files.

        Args:
            outputdir (str, optional): Directory to save the output files. Defaults to ".".
            rebuild (bool, optional): Whether to rebuild the netCDF file. Defaults to True.
            region (str, optional): Region for area selection. Defaults to None (global evaluation).
            var (list, optional): List of variables to process. Defaults to ["thetao", "so"].
            dim_mean (list, optional): List of dimensions over which to compute the mean. Defaults to ["lat", "lon"].
            anomaly_ref (str or None, optional): Reference for anomaly calculation. Can be "t0", "tmean", or None.
            reader_kwargs (dict, optional): Additional keyword arguments for the Reader. Defaults to {}.
        """
        self.logger.info("Running Hovmoller diagram generation")
        # This will populate self.data
        super().retrieve(var=var, reader_kwargs=reader_kwargs, months_required=2)
        # HACK: some LRA datasets have levels in 'NEMO model layers' (also non NEMO models due to multi-IO)
        if self.data.level.attrs['units']=='NEMO model layers':
            self.data.level.attrs['units'] = 'm'
        super()._check_data(data= self.data.level, var='level', units='m' )
        self.logger.debug("Data retrieved successfully")
        # If a region is specified, apply area selection to self.data
        if region:
            self.logger.info(f"Selecting region: {region} for diagnostic '{self.diagnostic_name}'.")
            res_dict = super()._select_region(
                data=self.data, region=region, diagnostic="ocean3d", drop=True
            )
            self.region = res_dict["region"]
            self.lat_limits = res_dict["lat_limits"]
            self.lon_limits = res_dict["lon_limits"]
        else:
            self.region = "global"
            self.lat_limits = None
            self.lon_limits = None
        self.stacked_data = self.compute_hovmoller(
            dim_mean=dim_mean, anomaly_ref=anomaly_ref
        )
        
        self.save_netcdf(outputdir=outputdir, rebuild=rebuild, region=self.region)
        self.logger.info("Hovmoller diagram saved to netCDF file")

    def _get_anomaly(
        self, data: xr.DataArray, anomaly_ref: str = None, dim: str = "time"
    ):
        """
        Compute anomaly for the given data along a specified dimension.

        Args:
            data : (xarray.DataArray) The input data array to process.
            anomaly_ref : (str or None, optional) Reference for anomaly calculation. Can be "t0", "tmean", or None.
                If "t0" or "tmean", the anomaly is computed relative to the initial time or the mean, respectively.
                If None, no anomaly is computed.
            dim : (str, optional) The dimension along which to compute the anomaly. Default is "time".

        Returns:
            xarray.DataArray
                The anomaly data array with updated attributes and an added "type" dimension.
        """
        if anomaly_ref is None:
            return data
        if anomaly_ref == "tmean":
            data = data - data.mean(dim=dim)
        elif anomaly_ref == "t0":
            data = data - data.isel({dim: 0})
        else:
            raise ValueError("Invalid anomaly_ref: use 't0', 'tmean', or None")
        return data

    def _get_standardise(self, data, dim="time"):
        """
        Standardise the data along a specified dimension.

        Args:
            data : (xarray.DataArray) The input data array to standardise.
            dim : (str, optional) The dimension along which to standardise. Default is "time".

        Returns:
            xarray.DataArray
                The standardised data array with updated attributes and an added "type" dimension.
        """
        data = data / data.std(dim=dim)
        data.attrs["units"] = "Stand. Units"
        data.attrs["AQUA_standardise"] = f"Standardised with {dim}"
        #type_str = f"Std_{data.attrs.get('AQUA_type', 'full')}"
        return data

    def _get_std_anomaly(
        self,
        data: xr.DataArray,
        anomaly_ref: str = None,
        standardise: bool = False,
        dim: str = "time",
    ):
        """
        Compute anomaly and/or standardised anomaly for the given data along a specified dimension.

        Args:
            data (xarray.DataArray): The input data array to process.
            anomaly_ref (str or None, optional): Reference for anomaly calculation. Can be "t0", "tmean", or None.
                If "t0" or "tmean", the anomaly is computed relative to the initial time or the mean, respectively.
                If None, no anomaly is computed.
            standardise (bool or None, optional): If True, standardise the anomaly. If None or False, no standardisation is applied.
            dim (str, optional): The dimension along which to compute the anomaly and/or standardisation. Default is "time".

        Returns:
            xarray.DataArray: The processed data array with updated attributes and an added "type" dimension indicating
            the type of transformation applied.

        Notes:
            The function updates the 'AQUA_type' attribute of the returned DataArray to indicate
            the type of anomaly and/or standardisation performed.
        """
        if anomaly_ref is not None:
            if anomaly_ref in ["t0", "tmean"]:
                data = self._get_anomaly(data, anomaly_ref, dim)
        if standardise:
            data = self._get_standardise(data, dim)

        Std = "std_" if standardise else ""
        anom = "anom" if anomaly_ref is not None else "full"
        anom_ref = f"_{anomaly_ref}" if anomaly_ref else ""

        type = f"{Std}{anom}{anom_ref}"
        data.attrs["AQUA_ocean_drift_type"] = type
        data.attrs["AQUA_region"] = self.region
        return data

    def compute_hovmoller(self, dim_mean: str = None, anomaly_ref: str | list = None):
        """
        Processes input data for drift analysis by applying various transformations
        and aggregations.

        Args:
            dim_mean (str or None): The dimension along which to compute the mean.
                If None, no mean is computed.
            anomaly_ref (str or list, optional): Reference for anomaly calculation. Can be "t0", "tmean", or None. By default, full values are used.

        Returns:
            xarray.DataArray: A concatenated DataArray containing processed data
            for different combinations of anomaly, standardization, and anomaly reference types.
        """
        anomaly_ref = to_list(anomaly_ref)
        anomaly_ref.append(None)

        if dim_mean is not None:
            self.logger.debug(f"Computing fldmean over dimension: {dim_mean}")
            self.data = self.reader.fldmean(
                self.data,
                dims=dim_mean,
                lat_limits=self.lat_limits,
                lon_limits=self.lon_limits,
            )


        for standardise, anomaly_ref in product([False, True], anomaly_ref):
            if not (standardise is True and anomaly_ref is None):
                self.logger.info(
                    f"Processing data with standardise={standardise}, anomaly_ref={anomaly_ref}"
                )
                processed_data = self._get_std_anomaly(
                    self.data, anomaly_ref, standardise, dim="time"
                )
                self.processed_data_list.append(processed_data)
        self.processed_data_list = sorted(self.processed_data_list, key=self.sort_key)

    def sort_key(self, data):
        type = data.attrs["AQUA_ocean_drift_type"]
        if type == "full":
            return (0, type)
        elif type.startswith("anom"):
            return (1, type)
        elif type.startswith("std"):
            return (2, type)

    def save_netcdf(
        self,
        diagnostic_product: str = "hovmoller",
        region: str = None,
        outputdir: str = ".",
        rebuild: bool = True,
    ):
        """
        Saves the processed data to a netCDF file.

        Args:
            diagnostic_product (str): Name of the diagnostic product.
            region (str): Region for area selection. Defaults to None.
            outputdir (str): Directory to save the output files. Defaults to '.'.
            rebuild (bool, optional): Whether to rebuild the netCDF file. Defaults to True.
        """

        for processed_data in self.processed_data_list:
            super().save_netcdf(
                data=processed_data,
                diagnostic=self.diagnostic_name,
                diagnostic_product=f"{diagnostic_product}",
                outputdir=outputdir,
                rebuild=rebuild,
                extra_keys={"region": self.region,
                            'ocean_drift_type': processed_data.attrs['AQUA_ocean_drift_type']}
            )
