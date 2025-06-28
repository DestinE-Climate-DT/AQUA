import xarray as xr

from aqua.logger import log_configure
from aqua.util import to_list
from aqua.graphics import plot_lat_lon_profiles
from aqua.diagnostics.core import OutputSaver

class PlotLatLonProfiles():
    
    def __init__(self, hourly_data=None, daily_data=None,
                 monthly_data=None, annual_data=None,
                 seasonal_annual_data=None,
                 std_hourly_data=None, std_daily_data=None,
                 std_monthly_data=None, std_annual_data=None,
                 loglevel: str = 'WARNING'):
        """
        Initialize the PlotLatLonProfiles class.
        This class is used to plot lat lon profiles data previously processed
        by the LatLonProfiles class.

        Any subset of frequency can be provided, however the order and length
        of the list of data arrays must be the same for each frequency.

        Note: Currently, only monthly and annual data are supported.

        Args:
            hourly_data (list): List of hourly data arrays.
            daily_data (list): List of daily data arrays.
            monthly_data (list): List of monthly data arrays.
            annual_data (list): List of annual data arrays.
            seasonal_annual_data (list): List of seasonal and annual means [DJF, MAM, JJA, SON, Annual].
            std_hourly_data (xr.DataArray): Standard deviation hourly data array.
            std_daily_data (xr.DataArray): Standard deviation daily data array.
            std_monthly_data (xr.DataArray): Standard deviation monthly data array.
            std_annual_data (xr.DataArray): Standard deviation annual data array.
            loglevel (str): Logging level. Default is 'WARNING'.
        """
        self.loglevel = loglevel
        self.logger = log_configure(log_level=loglevel, log_name='PlotLatLonProfiles')

        # TODO: support hourly and daily data
        for data in [hourly_data, daily_data,
                     std_hourly_data, std_daily_data]:
            if data is not None:
                self.logger.warning('Hourly and daily data are not yet supported, they will be ignored')

        self.monthly_data = to_list(monthly_data)
        self.annual_data = to_list(annual_data)
        self.seasonal_annual_data = seasonal_annual_data  # This should be a list of [DJF, MAM, JJA, SON, Annual]

        # self.std_hourly_data = to_list(std_hourly_data)
        # self.std_daily_data = to_list(std_daily_data)
        self.std_monthly_data = (
            std_monthly_data if (std_monthly_data is not None and isinstance(std_monthly_data, xr.DataArray))
            else (std_monthly_data[0] if std_monthly_data is not None else None)
        )

        self.std_annual_data = (
            std_annual_data if (std_annual_data is not None and isinstance(std_annual_data, xr.DataArray))
            else (std_annual_data[0] if std_annual_data is not None else None)
        )

        self.len_data = self._check_data_length()

        # Filling them
        self.get_data_info()

    def set_data_labels(self):
        """
        Set the data labels for the plot.
        The labels are extracted from the data arrays attributes.

        Returns:
            data_labels (list): List of data labels for the plot.
        """
        data_labels = []
        # Handle case where len_data might be 0 or lists might be empty
        if self.len_data > 0 and len(self.models) > 0 and len(self.exps) > 0:
            for i in range(min(self.len_data, len(self.models), len(self.exps))):
                label = f'{self.models[i]} {self.exps[i]}'
                data_labels.append(label)
        else:
            # Fallback label
            data_labels = ['Unknown Model/Experiment']

        self.logger.debug('Data labels: %s', data_labels)
        return data_labels
        
    def set_description(self, region: str = None, diagnostic: str = None):
        """
        Set the caption for the plot.
        The caption is extracted from the data arrays attributes.
        The caption is stored as 'Description' in the metadata dictionary.

        Args:
            region (str): Region to be used in the caption.
            diagnostic (str): Diagnostic name to be used in the caption.

        Returns:
            description (str): Caption for the plot.
        """

        description = f'{diagnostic} '
        if region is not None:
            description += f'for region {region} '

        for i in range(self.len_data):
            description += f'for {self.catalogs[i]} {self.models[i]} {self.exps[i]} '

        if self.std_startdate is not None and self.std_enddate is not None:
            description += f'with standard deviation from {self.std_startdate} to {self.std_enddate} '

        self.logger.debug('Description: %s', description)
        return description
    
    def set_title(self, region: str = None, var: str = None, units: str = None, diagnostic: str = None):
        """
        Set the title for the plot.

        Args:
            region (str): Region to be used in the title.
            var (str): Variable name to be used in the title.
            units (str): Units of the variable to be used in the title.
            diagnostic (str): Diagnostic name to be used in the title.

        Returns:
            title (str): Title for the plot.
        """
        title = f'{diagnostic} '
        if var is not None:
            title += f'for {var} '

        if units is not None:
            title += f'[{units}] '

        if region is not None:
            title += f'[{region}] '

        if self.len_data == 1:
            title += f'for {self.catalogs[0]} {self.models[0]} {self.exps[0]} '

        self.logger.debug('Title: %s', title)
        return title

    def run(self, var: str, units: str = None, region: str = None, outputdir: str = './',
            rebuild: bool = True, dpi: int = 300, format: str = 'png', plot_type: str = 'standard'):
        """
        Run the PlotLatLonProfiles class.

        Args:
            var (str): Variable name to be used in the title and description.
            units (str): Units of the variable to be used in the title.
            region (str): Region to be used in the title and description.
            outputdir (str): Output directory to save the plot.
            rebuild (bool): If True, rebuild the plot even if it already exists.
            dpi (int): Dots per inch for the plot.
            format (str): Format of the plot ('png' or 'pdf'). Default is 'png'.
            plot_type (str): Type of plot ('standard' or 'seasonal'). Default is 'standard'.
        """

        self.logger.info('Running PlotLatLonProfiles')
        data_label = self.set_data_labels()
        description = self.set_description(region=region, diagnostic='lat_lon_profiles')
        title = self.set_title(region=region, var=var, units=units, diagnostic='Lat-Lon Profiles')
        
        if plot_type == 'seasonal':
            if self.seasonal_annual_data is None:
                self.logger.error('Seasonal data not available for seasonal plot')
                return
            fig, _ = self.plot_seasonal_annual_lines(data_labels=data_label, title=title)
        else:
            fig, _ = self.plot_lat_lon_profiles(data_labels=data_label, title=title)
        
        region_short = region.replace(' ', '').lower() if region is not None else None
        diagnostic_suffix = '_seasonal' if plot_type == 'seasonal' else ''
        self.save_plot(fig, var=var, description=description, region=region_short, rebuild=rebuild,
                       outputdir=outputdir, dpi=dpi, format=format, diagnostic=f'lat_lon_profiles{diagnostic_suffix}')
        self.logger.info('PlotLatLonProfiles completed successfully')

    def get_data_info(self):
        """
        We extract the data needed for labels, description etc
        from the data arrays attributes.

        The attributes are:
        - AQUA_catalog
        - AQUA_model
        - AQUA_exp
        - std_startdate
        - std_enddate
        """
        for data in [self.monthly_data, self.annual_data, self.seasonal_annual_data]:
            if data is not None:
                # Handle seasonal_annual_data which is a list of DataArrays
                if data == self.seasonal_annual_data:
                    # Extract attributes from the first seasonal mean (DJF)
                    if len(data) > 0 and hasattr(data[0], 'AQUA_catalog'):
                        self.catalogs = [data[0].AQUA_catalog]
                        self.models = [data[0].AQUA_model]
                        self.exps = [data[0].AQUA_exp]
                        self.len_data = 1  # Set len_data for seasonal data
                    else:
                        self.catalogs = []
                        self.models = []
                        self.exps = []
                else:
                    # Make a list from the data array attributes
                    self.catalogs = [d.AQUA_catalog for d in data]
                    self.models = [d.AQUA_model for d in data]
                    self.exps = [d.AQUA_exp for d in data]
                break
        self.logger.debug(f'Catalogs: {self.catalogs}')
        self.logger.debug(f'Models: {self.models}')
        self.logger.debug(f'Experiments: {self.exps}')

        self.std_startdate = None
        self.std_enddate = None

        for std in [self.std_monthly_data, self.std_annual_data]:
            if std is not None:
                self.std_startdate = std.std_startdate if std.std_startdate is not None else None
                self.std_enddate = std.std_enddate if std.std_enddate is not None else None
                break
        self.logger.debug(f'Standard deviation dates: {self.std_startdate} - {self.std_enddate}')

    def plot_lat_lon_profiles(self, data_labels=None, title=None):
        """
        Plot the lat lon profiles data.

        Args:
            data_labels (list): List of data labels.
            title (str): Title of the plot.

        Returns:
            fig (matplotlib.figure.Figure): Figure object.
            ax (matplotlib.axes.Axes): Axes object.
        """
        fig, ax = plot_lat_lon_profiles(mean_type='zonal',
                                        monthly_data=self.monthly_data, 
                                        annual_data=self.annual_data,
                                        #fig=fig, ax=ax,
                                        data_labels= data_labels,
                                        )

        return fig, ax

    def plot_seasonal_annual_lines(self, data_labels=None, title=None, style=None):
        """
        Plot seasonal and annual means using multiple_lines.py.
        Creates a 5-panel plot with DJF, MAM, JJA, SON and Annual mean.

        Args:
            data_labels (list): List of data labels.
            title (str): Title of the plot.
            style (str): Plotting style.

        Returns:
            fig (matplotlib.figure.Figure): Figure object.
            axs (list): List of axes objects.
        """
        from aqua.graphics.multiple_lines import plot_lines
        
        if self.seasonal_annual_data is None:
            self.logger.error('No seasonal and annual data available. Compute them first.')
            return None, None
        
        self.logger.info('Plotting seasonal and annual means using multiple_lines')
        
        # Prepare data in the format expected by plot_lines
        # Structure: [[DJF], [MAM], [JJA], [SON], [Annual]]
        maps = [[self.seasonal_annual_data[i]] for i in range(5)]
        
        # Use plot_lines to create the 5-panel seasonal plot
        fig, axs = plot_lines(maps=maps,
                             plot_type='seasonal',
                             style=style,
                             loglevel=self.loglevel,
                             data_labels=data_labels)
        
        if title:
            fig.suptitle(title, fontsize=16, y=0.98)
        
        return fig, axs

    def save_plot(self, fig, var: str = None, description: str = None, region: str = None, rebuild: bool = True,
                  outputdir: str = './', dpi: int = 300, format: str = 'png', diagnostic: str = None):
        """
        Save the plot to a file.

        Args:
            fig (matplotlib.figure.Figure): Figure object.
            var (str): Variable name to be used in the title and description.
            description (str): Description of the plot.
            region (str): Region to be used in the title and description.
            rebuild (bool): If True, rebuild the plot even if it already exists.
            outputdir (str): Output directory to save the plot.
            dpi (int): Dots per inch for the plot.
            format (str): Format of the plot ('png' or 'pdf'). Default is 'png'.
            diagnostic (str): Diagnostic name to be used in the filename as diagnostic_product.
        """
        # Check if we have valid metadata, otherwise use defaults
        if len(self.catalogs) > 0 and len(self.models) > 0 and len(self.exps) > 0:
            catalog = self.catalogs[0]
            model = self.models[0] 
            exp = self.exps[0]
        else:
            # Fallback values when metadata is not available
            catalog = 'unknown_catalog'
            model = 'unknown_model'
            exp = 'unknown_exp'
            self.logger.warning('Metadata not available, using default values for saving')

        outputsaver = OutputSaver(diagnostic='timeseries', 
                                  catalog=catalog,
                                  model=model,
                                  exp=exp,
                                  outdir=outputdir,
                                  rebuild=rebuild,
                                  loglevel=self.loglevel)

        metadata = {"Description": description, "dpi": dpi }
        extra_keys = {'diagnostic_product': diagnostic}

        if var is not None:
            extra_keys.update({'var': var})
        if region is not None:
            region = region.replace(' ', '').lower() if region is not None else None
            extra_keys.update({'region': region})

        if format == 'png':
            outputsaver.save_png(fig, diagnostic_product=diagnostic, extra_keys=extra_keys, metadata=metadata)
        elif format == 'pdf':
            outputsaver.save_pdf(fig, diagnostic_product=diagnostic, extra_keys=extra_keys, metadata=metadata)
        else:
            raise ValueError(f'Format {format} not supported. Use png or pdf.')

    def _check_data_length(self):
        """
        Check if all data arrays have the same length.
        If not, raise a ValueError.

        Return:
            data_length (int): Length of the data arrays.
        """
        data_length = 0

        if self.monthly_data and self.annual_data:
            if len(self.monthly_data) != len(self.annual_data):
                raise ValueError('Monthly and annual data list must have the same length')
            else:
                data_length = len(self.monthly_data)

        return data_length