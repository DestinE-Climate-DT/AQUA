from aqua.logger import log_configure
from aqua.fixer import EvaluateFormula
from aqua.diagnostics.core import Diagnostic
from aqua.histogram import histogram

class Histogram(Diagnostic):
    """
    Class to compute histograms and probability density functions (PDFs) of a variable 
    over a specified region. Retrieves data from catalog, computes histograms/PDFs 
    for the entire period, and saves results to netcdf files.
    """
    def __init__(self, model: str, exp: str, source: str,
                 catalog: str = None, regrid: str = None,
                 startdate: str = None, enddate: str = None,
                 region: str = None, lon_limits: list = None, lat_limits: list = None,
                 regions_file_path: str = None,
                 bins: int = 100, range: tuple = None, weighted: bool = True,
                 diagnostic_name: str = 'histogram',
                 loglevel: str = 'WARNING'):
        """
        Initialize the Histogram diagnostic class.

        Args:
            model (str): Model to be used for data retrieval.
            exp (str): Experiment to be used for data retrieval.
            source (str): Source to be used for data retrieval.
            catalog (str, optional): Catalog for data retrieval.
            regrid (str, optional): Regridding method.
            startdate (str, optional): Start date of data to retrieve.
            enddate (str, optional): End date of data to retrieve.
            region (str, optional): Region for data retrieval.
            lon_limits (list, optional): Longitude limits of region.
            lat_limits (list, optional): Latitude limits of region.
            regions_file_path (str, optional): Path to regions file.
            bins (int, optional): Number of bins for histogram. Default 100.
            range (tuple, optional): Range for histogram bins (min, max).
            weighted (bool, optional): Use latitudinal weights. Default True.
            diagnostic_name (str, optional): Name of diagnostic. Default 'histogram'.
            loglevel (str, optional): Log level.
        """
        super().__init__(catalog=catalog, model=model, exp=exp, source=source, 
                        regrid=regrid, loglevel=loglevel)
        
        self.diagnostic_name = diagnostic_name
        self.logger = log_configure(log_level=loglevel, log_name='Histogram')

        # Simple date management - no std dates needed
        self.startdate = startdate
        self.enddate = enddate

        self.logger.debug(f"Period: {self.startdate} to {self.enddate}")

        # Region setup using parent class method
        self.region, self.lon_limits, self.lat_limits = self._set_region(
            region=region, diagnostic='histogram',
            regions_file_path=regions_file_path,
            lon_limits=lon_limits, lat_limits=lat_limits)

        # Histogram parameters
        self.bins = bins
        self.range = range
        self.weighted = weighted

        # Results storage - only longterm histogram
        self.histogram_data = None

    def retrieve(self, var: str, formula: bool = False, long_name: str = None,
                 units: str = None, standard_name: str = None,
                 reader_kwargs: dict = {}):
        """
        Retrieve data for the specified variable using the parent Diagnostic class.

        Args:
            var (str): Variable to retrieve.
            formula (bool): Whether to use formula for variable.
            long_name (str): Long name of variable.
            units (str): Units of variable.
            standard_name (str): Standard name of variable.
            reader_kwargs (dict): Additional Reader kwargs.
        """
        self.logger.info('Retrieving data for variable %s', var)

        if formula:
            # Call parent retrieve without var to get all variables needed for formula
            super().retrieve(reader_kwargs=reader_kwargs, months_required=12)
            self.logger.debug("Evaluating formula %s", var)
            self.data = EvaluateFormula(data=self.data, formula=var, long_name=long_name,
                                       short_name=standard_name, units=units,
                                       loglevel=self.loglevel).evaluate()
            if self.data is None:
                raise ValueError(f'Error evaluating formula {var}')
        else:
            # Call parent retrieve with the specific variable
            super().retrieve(var=var, reader_kwargs=reader_kwargs, months_required=12)
            if self.data is None:
                raise ValueError(f'Variable {var} not found')
            self.data = self.data[var]

        # Set dates if not specified
        if self.startdate is None:
            self.startdate = self.data.time.min().values
        if self.enddate is None:
            self.enddate = self.data.time.max().values

        # Customize data attributes
        if units is not None:
            self.data = self._check_data(data=self.data, var=var, units=units)
        if long_name is not None:
            self.data.attrs['long_name'] = long_name
        if standard_name is not None:
            self.data.attrs['standard_name'] = standard_name
            self.data.name = standard_name
        else:
            self.data.attrs['standard_name'] = var

    def compute_histogram(self, box_brd: bool = True, density: bool = True):
        """
        Compute histogram of the data for the entire period.

        Args:
            box_brd (bool): Include box boundaries in area selection.
            density (bool): If True, returns PDF normalized to integrate to 1.
        """
        self.logger.info('Computing histogram')
        
        # Select data for specified period
        data = self.data.sel(time=slice(self.startdate, self.enddate))
        
        # Select region if specified
        if self.lon_limits is not None or self.lat_limits is not None:
            data = self.reader.select_area(data, lon=self.lon_limits, 
                                          lat=self.lat_limits, box_brd=box_brd)

        # If range is not specified, compute it from the data
        hist_range = self.range
        if hist_range is None:
            self.logger.debug('Computing range from data')
            # Compute min and max from data (this will trigger computation if using dask)
            data_min = float(data.min().values)
            data_max = float(data.max().values)
            # Add small buffer to avoid edge effects
            buffer = (data_max - data_min) * 0.01
            hist_range = (data_min - buffer, data_max + buffer)
            self.logger.debug(f'Computed range: {hist_range}')

        # Compute histogram using the histogram function directly
        hist_data = histogram(
            data,
            bins=self.bins,
            range=hist_range,
            weighted=self.weighted,
            density=density,
            loglevel=self.loglevel
        )
        
        # Add region metadata
        if self.region is not None:
            hist_data.attrs['AQUA_region'] = self.region
        
        self.histogram_data = hist_data

    def save_netcdf(self, outputdir: str = './', rebuild: bool = True):
        """
        Save histogram data to netcdf file.

        Args:
            outputdir (str): Output directory.
            rebuild (bool): Rebuild if file exists.
        """
        if self.histogram_data is None:
            self.logger.error('No histogram data available')
            return
        
        var = getattr(self.histogram_data, 'standard_name', 'unknown')
        extra_keys = {'var': var}
        if self.region is not None:
            region = self.region.replace(' ', '').lower()
            extra_keys['AQUA_region'] = region
        
        self.logger.info('Saving histogram for variable %s', var)
        super().save_netcdf(data=self.histogram_data, diagnostic=self.diagnostic_name,
                           diagnostic_product='histogram',
                           outputdir=outputdir, rebuild=rebuild,
                           extra_keys=extra_keys)

    def run(self, var: str, formula: bool = False, long_name: str = None,
            units: str = None, standard_name: str = None,
            box_brd: bool = True, density: bool = True,
            outputdir: str = './', rebuild: bool = True,
            reader_kwargs: dict = {}):
        """
        Run all steps for histogram computation.

        Args:
            var (str): Variable to retrieve and compute.
            formula (bool): Use formula for variable.
            long_name (str): Long name of variable.
            units (str): Units of variable.
            standard_name (str): Standard name of variable.
            box_brd (bool): Include box boundaries.
            density (bool): Return PDF (normalized) instead of counts.
            outputdir (str): Output directory.
            rebuild (bool): Rebuild existing files.
            reader_kwargs (dict): Additional Reader kwargs.
        """
        self.logger.info('Running Histogram diagnostic for %s', var)
        
        # Retrieve data
        self.retrieve(var=var, formula=formula, long_name=long_name,
                     units=units, standard_name=standard_name,
                     reader_kwargs=reader_kwargs)
        
        # Compute histogram
        self.logger.info('Computing histogram')
        self.compute_histogram(box_brd=box_brd, density=density)
        
        # Save to netcdf
        self.logger.info('Saving histogram to netcdf')
        self.save_netcdf(outputdir=outputdir, rebuild=rebuild)
        
        self.logger.info('Histogram diagnostic computation completed')