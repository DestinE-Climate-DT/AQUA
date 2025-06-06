from itertools import product
import xarray as xr
from aqua.logger import log_configure
from aqua.diagnostics.core import Diagnostic
from aqua.util import to_list


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
    ):
        """
        Run the Hovmoller diagram generation workflow.

        This method retrieves the specified variables, applies region selection if provided,
        computes Hovmoller diagrams with optional mean and anomaly processing, and saves the
        results to netCDF files.

        Args:
            outputdir (str, optional): Directory to save the output files. Defaults to ".".
            rebuild (bool, optional): Whether to rebuild the netCDF file. Defaults to True.
            region (str, optional): Region for area selection. Defaults to None.
            var (list, optional): List of variables to process. Defaults to ["thetao", "so"].
            dim_mean (list, optional): List of dimensions over which to compute the mean. Defaults to ["lat", "lon"].
            anomaly_ref (str or None, optional): Reference for anomaly calculation. Can be "t0", "tmean", or None.
        """
        self.logger.info("Running Hovmoller diagram generation")
        # This will populate self.data
        super().retrieve(var=var)
        self.logger.info("Data retrieved successfully")
        # If a region is specified, apply area selection to self.data
        super().select_region(region=region, diagnostic="ocean3d")
        self.stacked_data = self.compute_hovmoller(
            dim_mean=dim_mean, anomaly_ref=anomaly_ref
        )
        self.save_netcdf(outputdir=outputdir, rebuild=rebuild, region=region)
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
        data.attrs["AQUA_anomaly_ref"] = anomaly_ref
        data.attrs["AQUA_cmap"] = "coolwarm"
        type_str = f"anom_{anomaly_ref}"
        data.attrs["AQUA_type"] = type_str
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
        type_str = f"Std_{data.attrs.get('AQUA_type', 'full')}"
        data.attrs["AQUA_type"] = type_str
        data = data.expand_dims(dim={"type": [type_str]})
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
        return data

    def compute_hovmoller(self, dim_mean: str = None, anomaly_ref: str|list = None):
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
            self.logger.debug(f"Computing mean over dimension: {dim_mean}")
            self.data = self.data.mean(dim=dim_mean)

        for standardise, anomaly_ref in product([False, True], anomaly_ref):
            self.logger.info(
                f"Processing data with standardise={standardise}, anomaly_ref={anomaly_ref}"
            )
            processed_data = self._get_std_anomaly(
                self.data, anomaly_ref, standardise, dim="time"
            )
            self.processed_data_list.append(processed_data)

    def save_netcdf(
        self,
        diagnostic: str = "ocean_drift",
        diagnostic_product: str = "hovmoller",
        region: str = None,
        outputdir: str = ".",
        rebuild: bool = True,
    ):
        """
        Saves the processed data to a netCDF file.

        Args:
            diagnostic (str): Name of the diagnostic.
            diagnostic_product (str): Name of the diagnostic product.
            region (str): Region for area selection. Defaults to None.
            outputdir (str): Directory to save the output files. Defaults to '.'.
            rebuild (bool, optional): Whether to rebuild the netCDF file. Defaults to True.
        """
        save_kwargs = {}

        if region is not None:
            save_kwargs["region"] = region

        for processed_data in self.processed_data_list:
            super().save_netcdf(
                data=processed_data,
                diagnostic=diagnostic,
                diagnostic_product=f"{diagnostic_product}_{processed_data.attrs["AQUA_ocean_drift_type"]}",
                default_path=outputdir,
                rebuild=rebuild,
                **save_kwargs,
            )
