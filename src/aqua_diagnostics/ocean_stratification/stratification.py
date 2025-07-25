import xarray as xr
from aqua.logger import log_configure
from aqua.diagnostics.core import Diagnostic
from aqua.util import to_list
from .util import *

xr.set_options(keep_attrs=True)

class Stratification(Diagnostic):
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
        self.logger.debug(f"Variables retrieved: {var}, region: {region}, dim_mean: {dim_mean}")
        if region:
            self.logger.info(f"Selecting region: {region} for diagnostic 'ocean3d'.")
            self.data, region, lon_limits, lat_limits = super().select_region(region=region, diagnostic="ocean3d")
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
        self.logger.debug(f"{self.climatology.capitalize()} climatology computed successfully.")

    def compute_rho(self):
        """
        Convert variables to absolute salinity and conservative temperature, then compute potential density.

        Updates the internal dataset with the computed potential density anomaly ('rho').

        Returns
        -------
        None
        """
        self.logger.debug("Converting variables to absolute salinity and conservative temperature.")
        self.data = convert_variables(self.data, loglevel=self.loglevel)
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
        self.logger.info(f"Saving results to netCDF: diagnostic={diagnostic}, product={diagnostic_product}, outputdir={outputdir}, region={region}")
        super().save_netcdf(
            data=self.data,
            diagnostic=diagnostic,
            diagnostic_product=f"{diagnostic_product}",
            outdir=outputdir,
            rebuild=rebuild,
            extra_keys={"region": region}
        )
        self.logger.info("NetCDF file saved successfully.")
            