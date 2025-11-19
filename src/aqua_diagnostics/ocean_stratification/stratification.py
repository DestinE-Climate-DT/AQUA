import calendar
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
        diagnostic_name: str = "stratification",
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
        self.diagnostic_name = diagnostic_name

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

        This method orchestrates the complete diagnostic process:
        1. Reads the required variables from the input source.
        2. Optionally selects a specified region.
        3. Optionally computes mean values over given dimensions.
        4. Computes stratification by generating climatology and potential density.
        5. Optionally computes mixed layer depth (MLD).
        6. Saves the processed dataset to a NetCDF file.

        Parameters
        ----------
        outputdir : str, optional
            Directory where the output NetCDF file will be saved. Default is the current directory (" . ").
        rebuild : bool, optional
            If True, overwrite the existing output file. Default is True.
        region : str, optional
            Name of the region to select for analysis. If None, no region selection is applied.
        var : list of str, optional
            Names of variables to retrieve. Default is ["thetao", "so"].
        dim_mean : list of str or str, optional
            Dimensions over which to average the data. If None, no averaging is applied.
        climatology : str, optional
            Type of climatology to compute ("month", "year", "season", "total"). Default is "month".
        reader_kwargs : dict, optional
            Additional keyword arguments passed to the data reader.
        mld : bool, optional
            If True, compute mixed layer depth (MLD) and include it in the output.

        Returns
        -------
        None
        """
        self.climatology = climatology
        self.logger.info("Starting stratification diagnostic run.")
        super().retrieve(var=var, reader_kwargs=reader_kwargs)
        if "lev" in self.data.dims:
            self.data = self.data.rename({"lev": "level"})
        self.logger.debug(
            f"Variables retrieved: {var}, region: {region}, dim_mean: {dim_mean}"
        )
        # If a region is specified, apply area selection to self.data
        if region != None:
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
        self.data.attrs["AQUA_region"] = self.region
        if dim_mean is not None:
            self.logger.debug(f"Computing fldmean over dimension: {dim_mean}")
            self.data = self.reader.fldmean(
                self.data,
                dims=dim_mean,
                lat_limits=self.lat_limits,
                lon_limits=self.lon_limits,
            )
        else:
            self.data = res_dict['data']
        self.logger.info("Computing stratification.")
        self.compute_stratification()
        if mld:
            self.logger.info("Computing mixed layer depth (MLD).")
            self.compute_mld()
        self.compute_climatology(climatology=self.climatology)
        self.logger.debug("Loading data in memory.")
        self.data.load()
        self.logger.debug("Loaded data in memory.")
        self.save_netcdf(outputdir=outputdir, rebuild=rebuild, region=self.region)
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
        self.calculate_rho()
        self.logger.debug("Stratification computation completed successfully.")

    def compute_climatology(self, climatology: str = "season"):
        """
        Compute climatology for the dataset based on the specified period type.

        Depending on the value of `self.climatology`, the method will:
        - Group and average the data along the corresponding time accessor if
        `self.climatology` is not one of ["month", "year", "season"].
        - Compute the overall mean across the time dimension if `self.climatology` is "total".

        Parameters
        ----------
        climatology : str, optional
            Type of climatology to compute. Expected values:
            - "month"   : Monthly climatology
            - "year"    : Yearly climatology
            - "season"  : Seasonal climatology
            - "total"   : Mean over all available time steps
            - Other     : Groups data by `time.<self.climatology>` and averages
            Default is "season".

        Returns
        -------
        None
        """
        self.logger.debug(f"Computing {self.climatology} climatology.")
        month_list = list(calendar.month_name)[1:]
        season_list = ["DJF", "MAM", "JJA", "SON"]
        month_season_list = month_list + season_list

        if self.climatology in month_list:
            self.clim_type = "month"
        elif self.climatology in season_list:
            self.clim_type = "season"
        else:
            self.clim_type = "Total"

        if self.clim_type:
            if self.clim_type in ["month", "year", "season"]:
                self.data = self.data.groupby(f"time.{self.clim_type}").mean("time")
                self.data = self.data.rename({f"{self.clim_type}": "time"})
                if self.clim_type == "month":
                    self.data = self.data.assign_coords(
                        time=[calendar.month_name[m] for m in self.data["time"].values]
                    )
                self.data = self.data.sel(time=self.climatology)
        elif self.climatology == "Total":
            self.data = self.data.mean("time", keep_attrs=True)
        self.data.attrs["AQUA_stratification_climatology"] = self.climatology
        self.logger.debug(
            f"{self.climatology.capitalize()} climatology computed successfully."
        )

    def calculate_rho(self):
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
        abs_so = convert_so(self.data["so"])
        self.logger.debug("Practical salinity converted to absolute salinity.")

        # Convert potential temperature to conservative temperature
        data_thetao = super()._check_data(
            data=self.data["thetao"], var="thetao", units="degreeC"
        )
        cons_thetao = convert_thetao(abs_so, data_thetao)
        self.logger.debug(
            "Potential temperature converted to conservative temperature."
        )

        # Update the dataset with converted variables
        # self.data["cons_thetao"] = cons_thetao
        # self.data["so"] = abs_so
        self.logger.info("Variables successfully converted and updated in dataset.")

        # self.data = convert_variables(self.data, loglevel=self.loglevel)
        self.logger.debug("Computing potential density at reference pressure 0 dbar.")
        rho = compute_rho(abs_so, cons_thetao, 0)
        self.data["rho"] = rho - 1000  # Convert to kg/m^3
        self.data["rho"].attrs["long_name"] = "Potential Density"
        self.data["rho"].attrs["units"] = "kg/m^3"
        self.data["rho"].attrs["standard_name"] = "sea_water_potential_density"
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
            diagnostic=self.diagnostic_name,
            diagnostic_product=f"{diagnostic_product}",
            outputdir=outputdir,
            rebuild=rebuild,
            extra_keys={"region": region},
        )
        self.logger.info("NetCDF file saved successfully.")
