import xarray as xr
from aqua.diagnostics.core import Diagnostic, OutputSaver
from aqua.logger import log_configure
# from aqua.util import pandas_freq_to_string

xr.set_options(keep_attrs=True)


class BaseMixin(Diagnostic):
    """The BaseMixin class is used to save the outputs from the ssh module."""

    def __init__(
        self,
        diagnostic_name: str = "sshVariability",
        catalog: str = None,
        model: str = None,
        exp: str = None,
        source: str = None,
        startdate: str = None,
        enddate: str = None,
        region: str = None,
        regrid: str = None,
        lon_limits: list[float] = None,
        lat_limits: list[float] = None,
        outputdir: str = "./",
        reader_kwargs: dict = {},
        var: str = None,
        long_name: str = None,
        short_name: str = None,
        units: str = None,
        rebuild: bool = True,
        loglevel: str = "WARNING",
    ):
        """
        Initialize the diagnostic base class.

        This constructor sets up the diagnostic configuration, including data source,
        model, experiment, region, regridding options, output directory, and logging.

        Args:
            diagnostic_name (str): Name of the diagnostic (default: 'sshVariability').
                Used for configuring the logger and output files.
            catalog (str, optional): Catalog to use. If None, determined by the Reader.
            model (str, optional): Model to be used.
            exp (str, optional): Experiment to be used.
            source (str, optional): Data source to be used.
            startdate (str, optional): Start date of the data to retrieve. If None, all available data is retrieved.
            enddate (str, optional): End date of the data to retrieve. If None, all available data is retrieved.
            region (str, optional): Named region for selecting data. Overrides lon_limits and lat_limits.
            regrid (str, optional): Target grid for regridding. If None, no regridding is applied.
            lon_limits (list of float, optional): Longitude limits. Overridden by region.
            lat_limits (list of float, optional): Latitude limits. Overridden by region.
            outputdir (str, optional): Directory to save output files (default: './').
            reader_kwargs (dict, optional): Additional keyword arguments for the Reader (default: {}).
            var (str, optional): Variable name to process.
            long_name (str, optional): Long name of the variable.
            short_name (str, optional): Short name of the variable.
            units (str, optional): Units of the variable.
            rebuild (bool, optional): If True, rebuild data from original files (default: True).
            loglevel (str, optional): Logging level (default: 'WARNING').

        Keyword Args:
            zoom (int, optional): HEALPix grid zoom level (e.g., zoom=10 corresponds to h1024).
            realization (int, optional): Ensemble realization number, included in the output filename.
            **kwargs: Additional arbitrary keyword arguments to pass to the intake catalog entry.
        """

        super().__init__(catalog=catalog, model=model, exp=exp, source=source, startdate=startdate, enddate=enddate, regrid=regrid, loglevel=loglevel)

        # Log name is the diagnostic name with the first letter capitalized
        self.logger = log_configure(log_level=loglevel, log_name=diagnostic_name.capitalize())
        self.diagnostic_name = diagnostic_name

        # We want to make sure we retrieve the required amount of data with a single Reader instance
        # self.startdate, self.enddate = start_end_dates(startdate=startdate, enddate=enddate)
        # self.logger.debug(f"Retrieve start date: {self.startdate}, End date: {self.enddate}")

        if region is not None and lon_limits is not None and lat_limits is not None:
            # Set the region based on the region name or the lon and lat limits
            self.region, self.lon_limits, self.lat_limits = self._set_region(
                region=region, diagnostic="sshVariability", lon_limits=lon_limits, lat_limits=lat_limits
            )
            self.logger.debug(f"Region: {self.region}, Lon limits: {self.lon_limits}, Lat limits: {self.lat_limits}")

        self.outputdir = outputdir
        self.logger.info(f"Outputs will be saved at {self.outputdir}.")

        self.var = var
        self.units = units
        self.long_name = long_name
        self.short_name = short_name
        self.rebuild = rebuild
        self.reader_kwargs = reader_kwargs
        self.region = region

    def _check_data(self, var: str, units: str):
        """
        Make sure that the data is in the correct units.

        Args:
            var (str): The variable to be checked.
            units (str): The units to be checked.
        """
        self.data[self.var] = super()._check_data(data=self.data[self.var], var=var, units=units)

    def retrieve(self):
        """
        Retrieve the data for the given variable.
        """
        
        super().retrieve(var=self.var, reader_kwargs=self.reader_kwargs)

        if self.data is None:
            raise ValueError(f"Variable {self.var} not found in the data. " "Check the variable name and the data source.")
        # Get the xr.DataArray to be aligned with the formula code
        self.data = self.data[self.var]

        # Customization of the data, expecially needed for formula
        if self.units is not None:
            self._check_data(self.var, self.units)
        else:
            self.units = self.data.attrs.get("units")
        if self.long_name is not None:
            self.data.attrs["long_name"] = self.long_name
        # We want to be sure that a long_name is always defined for description setup
        elif self.data.attrs.get("long_name") is None:
            self.data.attrs["long_name"] = self.var
        # We use the short_name as the name of the variable
        # to be always used in plots
        if self.short_name is not None:
            self.data.attrs["short_name"] = self.short_name
            self.data.name = self.short_name
        else:
            self.data.attrs["short_name"] = self.var
        if self.region:
            self.data.attrs["region"] = self.region
            self.data.attrs["lon_limits"] = self.lon_limits
            self.data.attrs["lat_limits"] = self.lat_limits

    def netcdf_save(
        self,
        data,
        diagnostic_product: str = "sshVariability",
        freq: str = None,
        create_catalog_entry: bool = False,
        dict_catalog_entry: dict = {"jinjalist": ["freq", "realization", "region"], "wildcardlist": ["var"]},
    ):
        """
        Save the data to a netcdf file.

        Args:
            data (xarray.DataArray): Input data array
            diagnostic_product (str): The product name to be used in the filename 'sshVariability'.
            freq (str): The frequency of the data. It is set to 'None' for this release of code.
            outputdir (str): The directory to save the data.
            rebuild (bool): If True, rebuild the data from the original files.
            create_catalog_entry (bool): If True, create a catalog entry for the data. Default is False.
            dict_catalog_entry (dict): A dictionary with catalog entry information. Default is {'jinjalist': ['freq', 'region', 'realization'], 'wildcardlist': ['var']}.
        """
        # TODO:
        # the 'freq' variable will be updated in depends on the frequency of the data.
        # the idea is to implement the formula of 'variance of variances'. In this case, this variable will be used.

        # str_freq = pandas_freq_to_string(freq)

        # if str_freq == 'hourly':
        #    data = self.hourly if self.hourly is not None else self.logger.error('No hourly data available')
        #    data_std = self.std_hourly if self.std_hourly is not None else None
        # elif str_freq == 'daily':
        #    data = self.daily if self.daily is not None else self.logger.error('No daily data available')
        #    data_std = self.std_daily if self.std_daily is not None else None
        # elif str_freq == 'monthly':
        #    data = self.monthly if self.monthly is not None else self.logger.error('No monthly data available')
        #    data_std = self.std_monthly if self.std_monthly is not None else None
        # elif str_freq == 'annual':
        #    data = self.annual if self.annual is not None else self.logger.error('No annual data available')
        #    data_std = self.std_annual if self.std_annual is not None else None

        var = getattr(data, "short_name", None)
        # extra_keys = {'var': var, 'freq': str_freq}
        extra_keys = {"var": var}

        if data.name is None:
            data.name = var

        # In order to have a catalog entry we want to have a key region even in the global case
        region = self.region.replace(" ", "").lower() if self.region is not None else "global"
        extra_keys.update({"region": region})
        extra_keys.update({"startdate": self.startdate})
        extra_keys.update({"enddate": self.enddate})

        # self.logger.info('Saving %s data for %s to netcdf in %s', str_freq, diagnostic_product, outputdir)
        self.logger.info("Saving output data for %s to netcdf in %s", diagnostic_product, self.outputdir)

        super().save_netcdf(
            data=data,
            diagnostic=self.diagnostic_name,
            diagnostic_product=diagnostic_product,
            outputdir=self.outputdir,
            rebuild=self.rebuild,
            extra_keys=extra_keys,
            create_catalog_entry=create_catalog_entry,
            dict_catalog_entry=dict_catalog_entry,
        )


class PlotBaseMixin:
    """PlotBaseMixin class is used for the PlotSSHVariability."""

    def __init__(
        self,
        diagnostic_name: str = "sshVariability",
        loglevel: str = "WARNING",
    ):
        """
        Initialize the PlotBaseMixin class.

        Args:
            diagnostic_name (str): The name of the diagnostic 'ssh'.
                                   This will be used to configure the logger and the output files.
            loglevel (str): The log level to be used. Default is 'WARNING'.
        """
        # Data info initalized as empty
        self.loglevel = loglevel
        self.diagnostic_name = diagnostic_name
        log_name = "Plot" + diagnostic_name.capitalize()
        self.logger = log_configure(log_level=loglevel, log_name=log_name)

    def save_plot(
        self,
        fig,
        var: str = None,
        description: str = None,
        rebuild: bool = True,
        outputdir: str = "./",
        dpi: int = 600,
        format: str = "png",
        diagnostic_product: str = "sshVariability",
        catalog: str = None,
        model: str = None,
        exp: str = None,
        region: str = None,
        startdate: str = None,
        enddate: str = None,
        long_name: str = None,
        short_name: str = None,
        units: str = None,
    ):
        """
        Save a matplotlib figure to file with metadata for diagnostics.

        This function saves the given figure in the specified format and resolution,
        embedding metadata such as variable name, experiment details, region, and time range
        for better traceability of diagnostic plots.

        Args:
            fig (matplotlib.figure.Figure): The figure object to be saved.
            var (str, optional): Variable name associated with the plot (e.g., ``'zos'``).
            description (str, optional): Additional description of the plot.
            rebuild (bool, optional): If ``True``, overwrite and rebuild the file even if it exists. Default is ``True``.
            outputdir (str, optional): Directory where the figure will be saved. Default is current directory ``'./'``.
            dpi (int, optional): Resolution of the saved figure in dots per inch. Default is ``600``.
            format (str, optional): File format for saving the figure (e.g., ``'png'``, ``'pdf'``). Default is ``'png'``.
            diagnostic_product (str, optional): Diagnostic product name. Default is ``'sshVariability'``.
            catalog (str, optional): Catalog identifier for the dataset. (Mandatory for proper labeling)
            model (str, optional): Model name associated with the dataset. (Mandatory for proper labeling)
            exp (str, optional): Experiment name. (Mandatory for proper labeling)
            region (str, optional): Geographic region identifier for the plot.
            startdate (str, optional): Start date of the dataset used in the plot (used in metadata and title).
            enddate (str, optional): End date of the dataset used in the plot (used in metadata and title).
            long_name (str, optional): Long descriptive name of the variable (for labeling/metadata).
            short_name (str, optional): Short variable name (for labeling/metadata).
            units (str, optional): Units of the variable (e.g., ``'m'`` for meters).

        Returns:
            str: Full path to the saved plot file.

        Raises:
            ValueError: If mandatory arguments (``catalog``, ``model``, or ``exp``) are missing.
            OSError: If the output directory does not exist and cannot be created.
        """
        outputsaver = OutputSaver(
            diagnostic=self.diagnostic_name, catalog=catalog, model=model, exp=exp, outputdir=outputdir, loglevel=self.loglevel
        )
        if description is None:
            description = "sshVariability diagnostic"
        description = description + f" ({startdate}-{enddate}) "
        metadata = {"Description": description, "dpi": dpi}
        extra_keys = {"diagnostic_product": diagnostic_product}

        if short_name is not None:
            extra_keys.update({"var": short_name})
        if region is not None:
            region = region.replace(" ", "").lower()
            extra_keys.update({"region": region})

        if format == "png":
            outputsaver.save_png(
                fig, diagnostic_product=diagnostic_product, rebuild=rebuild, extra_keys=extra_keys, metadata=metadata
            )
        elif format == "pdf":
            outputsaver.save_pdf(
                fig, diagnostic_product=diagnostic_product, rebuild=rebuild, extra_keys=extra_keys, metadata=metadata
            )
        else:
            raise ValueError(f"Format {format} not supported. Use png or pdf.")

    def save_diff_plot(
        self,
        fig,
        var: str = None,
        description: str = None,
        rebuild: bool = True,
        outputdir: str = "./",
        dpi: int = 600,
        format: str = "png",
        diagnostic_product: str = "sshVariability_Difference",
        catalog: str = None,
        model: str = None,
        exp: str = None,
        startdate: str = None,
        enddate: str = None,
        catalog_ref: str = None,
        model_ref: str = None,
        exp_ref: str = None,
        startdate_ref: str = None,
        enddate_ref: str = None,
        region: str = None,
        long_name: str = None,
        short_name: str = None,
        units: str = None,
    ):
        """
        Save the plot of SSH variability differences between a reference dataset and a model.

        This function saves a figure illustrating the difference in sea surface height (SSH)
        variability between a reference dataset and a model, including metadata for reproducibility
        and traceability.

        Args:
            fig (matplotlib.figure.Figure): The matplotlib figure object to be saved.
            var (str, optional): Variable name associated with the plot (e.g., ``'zos'``).
            description (str, optional): Additional description to include in the saved file metadata.
            rebuild (bool, optional): If ``True``, overwrite the plot file even if it already exists. Default is ``True``.
            outputdir (str, optional): Directory where the plot file will be saved. Default is current directory ``'./'``.
            dpi (int, optional): Resolution of the saved figure in dots per inch. Default is ``600``.
            format (str, optional): Output file format (e.g., ``'png'``, ``'pdf'``). Default is ``'png'``.
            diagnostic_product (str, optional): Diagnostic product identifier. Default is ``'sshVariability_Difference'``.
            catalog (str, optional): Catalog name for the model dataset. (Mandatory for labeling)
            model (str, optional): Model name for the dataset. (Mandatory for labeling)
            exp (str, optional): Experiment identifier for the dataset. (Mandatory for labeling)
            startdate (str, optional): Start date for the model dataset. Used in plot title/metadata.
            enddate (str, optional): End date for the model dataset. Used in plot title/metadata.
            catalog_ref (str, optional): Catalog name for the reference dataset.
            model_ref (str, optional): Model name for the reference dataset.
            exp_ref (str, optional): Experiment identifier for the reference dataset.
            startdate_ref (str, optional): Start date for the reference dataset.
            enddate_ref (str, optional): End date for the reference dataset.
            region (str, optional): Geographic region identifier for the plot.
        Returns:
            str: The full path to the saved plot file.

        Raises:
            ValueError: If required arguments (``catalog``, ``model``, ``exp``) are missing.
            OSError: If the output directory does not exist and cannot be created.
        """
        # TODO:
        # Test if the sshVariability is computed in healpix/native grid then compte the difference will be an issue.
        # Therefore perform regridding via Regridding class.
        outputsaver = OutputSaver(
            diagnostic=self.diagnostic_name,
            catalog=catalog,
            model=model,
            exp=exp,
            catalog_ref=catalog_ref,
            model_ref=model_ref,
            exp_ref=exp_ref,
            outputdir=outputdir,
            loglevel=self.loglevel,
        )
        if description is None:
            description = "sshVariability difference"
        description = description + f" model time: ({startdate}-{enddate}) and reference time: ({startdate_ref}-{enddate_ref})"
        metadata = {"Description": description, "dpi": dpi}
        extra_keys = {"diagnostic_product": diagnostic_product}

        if short_name is not None:
            extra_keys.update({"var": short_name})
        if region is not None:
            region = region.replace(" ", "").lower()
            extra_keys.update({"region": region})

        if format == "png":
            outputsaver.save_png(
                fig, diagnostic_product=diagnostic_product, rebuild=rebuild, extra_keys=extra_keys, metadata=metadata
            )
        elif format == "pdf":
            outputsaver.save_pdf(
                fig, diagnostic_product=diagnostic_product, rebuild=rebuild, extra_keys=extra_keys, metadata=metadata
            )
        else:
            raise ValueError(f"Format {format} not supported. Use png or pdf.")
