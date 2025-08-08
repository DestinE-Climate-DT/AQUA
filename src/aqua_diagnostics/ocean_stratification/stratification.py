import xarray as xr
from aqua.logger import log_configure
from aqua.diagnostics.core import Diagnostic
from .compute_mld import compute_mld_cont
from .compute_rho import compute_rho
from .convert_variables import convert_so, convert_thetao

xr.set_options(keep_attrs=True)


class Stratification(Diagnostic):
    """
    Diagnostic class for analyzing ocean stratification.

    Parameters
    ----------
    catalog : str, optional
        Path to the data catalog (e.g., intake-esm catalog).
    model : str, optional
        Name of the climate model to analyze.
    exp : str, optional
        Experiment name (e.g., 'historical', 'ssp585').
    source : str, optional
        Data source (e.g., 'CMIP6', 'OBS').
    regrid : str, optional
        Regridding method or target grid (e.g., '1x1', 'nearest').
    startdate : str, optional
        Start date of the analysis period (format: 'YYYY-MM-DD').
    enddate : str, optional
        End date of the analysis period (format: 'YYYY-MM-DD').
    loglevel : str, optional
        Logging level (default is "WARNING").

    Attributes
    ----------
    logger : logging.Logger
        Configured logger for the diagnostic.
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
        self.logger = log_configure(log_name="Stratification", log_level=loglevel)

    def run(
        self,
        outputdir: str = ".",
        rebuild: bool = True,
        region: str = None,
        var: list = ["thetao", "so"],
        dim_mean=None,
        climatology: str = "month",
        reader_kwargs: dict = {},
        mld: bool = False,
    ):
        """
        Run the stratification diagnostic workflow.

        This method retrieves the required variables, optionally selects a region, averages over specified dimensions,
        computes stratification (climatology and density), optionally computes mixed layer depth (MLD), and saves the results to a netCDF file.

        Parameters
        ----------
        outputdir : str, optional
            Directory to save the output netCDF file. Default is current directory.
        rebuild : bool, optional
            Whether to rebuild the output file if it exists. Default is True.
        region : str, optional
            Region name to select for the diagnostic. If None, no region selection is performed.
        var : list, optional
            List of variable names to retrieve. Default is ["thetao", "so"].
        dim_mean : list or str, optional
            Dimensions over which to average the data. If None, no averaging is performed.
        reader_kwargs : dict, optional
            Additional keyword arguments to pass to the data reader.
        mld : bool, optional
            If True, compute mixed layer depth (MLD) and add to output.

        Returns
        -------
        None
        """
        self.climatology = climatology
        self.logger.info("Starting stratification diagnostic run.")
        super().retrieve(var=var, reader_kwargs=reader_kwargs)
        self.logger.debug(
            f"Variables retrieved: {var}, region: {region}, dim_mean: {dim_mean}"
        )
        if region:
            self.logger.info(f"Selecting region: {region} for diagnostic 'ocean3d'.")
            region, lon_limits, lat_limits = super().select_region(
                region=region, diagnostic="ocean3d"
            )
        if dim_mean:
            self.logger.debug(f"Averaging data over dimensions: {dim_mean}")
            self.data = self.data.mean(dim=dim_mean, keep_attrs=True)
        self.logger.info("Computing stratification.")
        self.compute_stratification()
        if mld:
            self.logger.info("Computing mixed layer depth (MLD).")
            self.compute_mld()
        self.save_netcdf(outputdir=outputdir, rebuild=rebuild, region=region)
        self.logger.info("Stratification diagnostic saved to netCDF file.")

    def compute_stratification(self):
        """
        Compute the stratification by calculating climatology and density.

        This method first computes the climatology (default: seasonal) and then computes the potential density.
        Updates the internal dataset with the results.

        Returns
        -------
        None
        """
        self.logger.debug("Starting computation of climatology and density.")
        self.compute_climatology()
        self.compute_rho()
        self.logger.debug("Stratification computation completed successfully.")

    def compute_climatology(self, climatology: str = "season"):
        """
        Compute climatology for the data.

        Parameters
        ----------
        climatology : str, optional
            Type of climatology to compute (e.g., 'season', 'month'). Default is 'season'.

        Returns
        -------
        None
        """
        self.logger.debug(f"Computing {self.climatology} climatology.")
        self.data = self.data.groupby(f"time.{self.climatology}").mean("time")
        self.logger.debug(
            f"{self.climatology.capitalize()} climatology computed successfully."
        )

    def compute_rho(self):
        """
        Convert variables to absolute salinity and conservative temperature, then compute potential density.

        Updates the internal dataset with the computed potential density anomaly ('rho').

        Returns
        -------
        None
        """
        self.logger.debug(
            "Converting variables to absolute salinity and conservative temperature."
        )
        # Convert practical salinity to absolute salinity
        abs_so = convert_so(self.data['so'])
        self.logger.debug("Practical salinity converted to absolute salinity.")

        # Convert potential temperature to conservative temperature
        cons_thetao = convert_thetao(abs_so, self.data['thetao'])
        self.logger.debug("Potential temperature converted to conservative temperature.")

        # Update the dataset with converted variables
        self.data["thetao"] = cons_thetao
        self.data["so"] = abs_so
        self.logger.info("Variables successfully converted and updated in dataset.")
        
        # self.data = convert_variables(self.data, loglevel=self.loglevel)
        self.logger.debug("Computing potential density at reference pressure 0 dbar.")
        rho = compute_rho(self.data["so"], self.data["thetao"], 0)
        self.data["rho"] = rho - 1000  # Convert to kg/m^3
        self.logger.debug("Added 'rho' (potential density anomaly) to dataset.")

    def compute_mld(self):
        """
        Compute the mixed layer depth (MLD) from the density field.

        Uses the potential density anomaly ('rho') in the dataset to compute MLD and adds it as 'mld'.

        Returns
        -------
        None
        """
        self.logger.debug("Computing mixed layer depth (MLD) from density.")
        mld = compute_mld_cont(self.data[["rho"]], loglevel=self.loglevel)
        self.data["mld"] = mld["mld"]
        self.logger.debug("Added 'mld' (mixed layer depth) to dataset.")

    def save_netcdf(
        self,
        diagnostic: str = "ocean_circulation",
        diagnostic_product: str = "stratification",
        region: str = None,
        outputdir: str = ".",
        rebuild: bool = True,
    ):
        """
        Save the diagnostic output to a NetCDF file.

        Parameters
        ----------
        diagnostic : str, optional
            High-level diagnostic category (default is "ocean_circulation").
        diagnostic_product : str, optional
            Specific diagnostic product name (default is "stratification").
        region : str, optional
            Region name to include in metadata or filename.
        outputdir : str, optional
            Directory where the NetCDF file will be saved (default is current directory).
        rebuild : bool, optional
            If True, force rebuild of NetCDF file even if it exists (default is True).
        """
        self.logger.info(
            f"Saving results to netCDF: diagnostic={diagnostic}, product={diagnostic_product}, outputdir={outputdir}, region={region}"
        )
        super().save_netcdf(
            data=self.data,
            diagnostic=diagnostic,
            diagnostic_product=f"{diagnostic_product}",
            outdir=outputdir,
            rebuild=rebuild,
            extra_keys={"region": region},
        )
        self.logger.info("NetCDF file saved successfully.")
