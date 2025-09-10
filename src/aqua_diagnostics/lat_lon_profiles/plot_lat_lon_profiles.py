import xarray as xr
from aqua.graphics import plot_seasonal_lat_lon_profiles
from aqua.logger import log_configure
from aqua.util import to_list
from aqua.graphics import plot_lat_lon_profiles
from aqua.diagnostics.core import OutputSaver

class PlotLatLonProfiles():
    """
    Class for plotting Lat-Lon Profiles diagnostics.
    This class provides methods to set data labels, description, title,
    and to plot the data. It handles data arrays regardless of their original
    temporal frequency, as temporal averaging is handled upstream.
    """
    def __init__(self, data=None, ref_data=None,
                 data_type='annual',
                 ref_std_data=None,
                 mean_type=None,
                 loglevel: str = 'WARNING'):
        """
        Initialise the PlotLatLonProfiles class.
        This class is used to plot lat lon profiles data previously processed
        by the LatLonProfiles class.

        Args:
            data: Can be either:
                - List of temporally-averaged data arrays for annual plots
                - List of seasonal data [DJF, MAM, JJA, SON] for seasonal plots
            ref_data: Reference data (structure matches data based on data_type)
            data_type (str): 'annual' for single/multi-line annual plots, 'seasonal' for 4-panel seasonal plots
            ref_std_data: Reference standard deviation data
            mean_type (str): The type of mean to compute ('zonal' or 'meridional')
            loglevel (str): Logging level. Default is 'WARNING'.
            
        Note:
            data_type determines how 'data' is interpreted:
            - 'annual': data should be list of DataArrays for single plot
            - 'seasonal': data should be [DJF, MAM, JJA, SON] for 4-panel seasonal plots
        """
        self.loglevel = loglevel
        self.logger = log_configure(loglevel, 'PlotLatLonProfiles')

        self.data_type = data_type
        self.mean_type = mean_type

        # Store data based on type
        if data_type == 'annual':
            self.data = to_list(data) if data is not None else []
            self.ref_data = ref_data

        elif data_type == 'seasonal':
            self.data = data if data is not None else []  # Store seasonal data directly in unified interface
            self.ref_data = ref_data  # Store seasonal ref data directly
        else:
            raise ValueError(f"data_type must be 'annual' or 'seasonal', got '{data_type}'")
        
        self.ref_std_data = ref_std_data
        
        self.len_data, self.len_ref = self._check_data_length()
        self.get_data_info()

    def set_data_labels(self):
        """Set the data labels for the plot based on data_type."""
        if not self.data or len(self.data) == 0:
            self.logger.warning('No data available for label generation')
            return []
        
        # Use self.models and self.exps to create labels
        data_labels = []
        num_labels = max(len(self.models), len(self.exps), 1)
        
        for i in range(num_labels):
            if i < len(self.models) and i < len(self.exps):
                data_labels.append(f'{self.models[i]} {self.exps[i]}')
            else:
                data_labels.append(f'Dataset {i+1}')
        
        self.logger.debug('Data labels: %s', data_labels)
        return data_labels
    
    def set_ref_label(self):
        """
        Set the reference label for the plot.
        The label is extracted from the reference data array attributes.

        Returns:
            ref_label (str): Reference label for the plot.
        """
        ref_label = None
        
        if self.ref_data is not None:
            model = self.ref_data.attrs.get('AQUA_model', 'Unknown')
            exp = self.ref_data.attrs.get('AQUA_exp', 'Unknown')
            ref_label = f'{model} {exp}'
        
        self.logger.debug('Reference label: %s', ref_label)
        return ref_label
    
    def get_data_info(self):
        """Extract metadata from data arrays based on data_type."""
        self.catalogs, self.models, self.exps = [], [], []
        self.region = None
        self.short_name = None
        self.standard_name = None

        if not self.data or len(self.data) == 0:
            raise ValueError("No data available for metadata extraction")
        
        # Get all data items to extract metadata from
        data_items = []
        if self.data_type == 'annual':
            data_items = self.data
        elif self.data_type == 'seasonal':
            # For seasonal, use first season's data
            first_season = self.data[0] if self.data else []
            data_items = first_season if isinstance(first_season, list) else [first_season]
        
        # Extract metadata from all data items
        for data_item in data_items:
            if data_item is not None and hasattr(data_item, 'AQUA_catalog'):
                self.catalogs.append(data_item.AQUA_catalog)
                self.models.append(data_item.AQUA_model)
                self.exps.append(data_item.AQUA_exp)

                # Extract region if not already set
                if self.region is None and hasattr(data_item, 'AQUA_region'):
                    self.region = data_item.AQUA_region

                # Extract variable names if not already set
                if self.short_name is None and hasattr(data_item, 'short_name'):
                    self.short_name = data_item.short_name
                if self.standard_name is None and hasattr(data_item, 'standard_name'):
                    self.standard_name = data_item.standard_name
        
        # Set mean_type from first data item if not already set
        if self.mean_type is None and data_items:
            first_data = data_items[0]
            if first_data is not None and hasattr(first_data, 'AQUA_mean_type'):
                self.mean_type = first_data.AQUA_mean_type
        
        self.logger.debug(f'Extracted metadata for {len(self.models)} datasets: {list(zip(self.models, self.exps))}')
        self.logger.debug(f'Extracted region: {self.region}')
        
        # Handle std dates
        if self.ref_std_data is not None:
            self.std_startdate = getattr(self.ref_std_data, 'std_startdate', None)
            self.std_enddate = getattr(self.ref_std_data, 'std_enddate', None)
        else:
            self.std_startdate = None
            self.std_enddate = None

    def plot(self, data_labels=None, ref_label=None, title=None):
        """
        Unified plotting method that handles all plotting scenarios based on data_type.
        
        Args:
            data_labels (list, optional): Labels for the data.
            ref_label (str, optional): Label for the reference data.  
            title (str, optional): Title for the plot.
            
        Returns:
            tuple: Matplotlib figure and axes objects.
        """
        if not self.data or len(self.data) == 0:
            raise ValueError("No data available for plotting")
        
        if self.data_type == 'seasonal':
            # For seasonal plots, delegate to the specialized seasonal method
            return self.plot_seasonal_lines(data_labels=data_labels, title=title)
        
        data_to_plot = self.data
        ref_to_plot = self.ref_data
        
        # Call the graphics function
        return plot_lat_lon_profiles(
            data=data_to_plot,
            ref_data=ref_to_plot,
            ref_std_data=self.ref_std_data,
            data_labels=data_labels,
            ref_label=ref_label,
            title=title,
            loglevel=self.loglevel
        )
    
    def save_plot(self, 
                  fig, 
                  description: str = None, 
                  rebuild: bool = True,
                  outputdir: str = './', 
                  dpi: int = 300, 
                  format: str = 'png', 
                  diagnostic: str = None):
        """
        Save the plot to a file.

        Args:
            fig (matplotlib.figure.Figure): Figure object.
            description (str): Description of the plot.
            rebuild (bool): If True, rebuild the plot even if it already exists.
            outputdir (str): Output directory to save the plot.
            dpi (int): Dots per inch for the plot.
            format (str): Format of the plot ('png' or 'pdf'). Default is 'png'.
            diagnostic (str): Diagnostic name to be used in the filename as diagnostic_product.
        """
        metadata = {
            'catalog': getattr(self, 'catalogs', ['unknown_catalog'])[0],
            'model': getattr(self, 'models', ['unknown_model'])[0], 
            'exp': getattr(self, 'exps', ['unknown_exp'])[0]
        }

        outputsaver = OutputSaver(diagnostic='lat_lon_profiles', outputdir=outputdir,
                                loglevel=self.loglevel, **metadata)
        
        # Use class attributes
        var = getattr(self, 'short_name', None) or getattr(self, 'standard_name', None)
        region = self.region

        # Build extra_keys
        extra_keys = {}
        if var: extra_keys['var'] = var
        if region: extra_keys['region'] = region.replace(' ', '').lower()
        
        # diagnostic_product must match the one used in OutputSaver
        base_diagnostic = diagnostic or 'lat_lon_profiles'
        if self.mean_type:
            diagnostic_product = f"{base_diagnostic}_{self.mean_type}"
        else:
            diagnostic_product = f"{base_diagnostic}_zonal"
            self.logger.warning("mean_type not available in data attributes. "
                                "Using 'zonal' as fallback to match LatLonProfiles default. ")       
        # Save based on format
        if format == 'png':
            outputsaver.save_png(fig, diagnostic_product, extra_keys=extra_keys, 
                            metadata={'Description': description, 'dpi': dpi})
        else:
            outputsaver.save_pdf(fig, diagnostic_product, extra_keys=extra_keys, 
                            metadata={'Description': description, 'dpi': dpi})

    def _check_data_length(self):
        """
        Check the length of the data arrays and reference data based on data_type.
        Returns:
            tuple: (length of data arrays, length of reference data)
        """
        len_data = len(self.data) if self.data else 0
        
        if self.data_type == 'annual':
            len_ref = 1 if self.ref_data is not None else 0
        elif self.data_type == 'seasonal':
            len_ref = len(self.ref_data) if self.ref_data else 0
        else:
            len_ref = 0
        
        self.logger.debug(f'Data type: {self.data_type}, Data length: {len_data}, Reference length: {len_ref}')
        return len_data, len_ref

    def set_title(self, region: str = None, var: str = None, units: str = None):
        """
        Set the title for the plot.
        Specialized for Lat-Lon Profiles diagnostic.

        Args:
            region (str): Region to be used in the title.
            var (str): Variable name to be used in the title.
            units (str): Units of the variable to be used in the title.
            
        Returns:
            title (str): Title for the plot.
        """
        title = 'Lat-Lon Profiles '
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

    def set_description(self, region: str = None):
        """
        Set the caption for the plot.
        Specialized for Lat-Lon Profiles diagnostic.
        """
        description = 'lat_lon_profiles '
        if region is not None:
            description += f'for region {region} '

        # Check if we have enough metadata for all data items
        num_items = min(len(self.catalogs), len(self.models), len(self.exps)) if hasattr(self, 'catalogs') else 0
        
        for i in range(min(self.len_data, num_items)):
            description += f'for {self.catalogs[i]} {self.models[i]} {self.exps[i]} '
        
        # If we don't have enough metadata, add a generic description
        if num_items < self.len_data:
            remaining = self.len_data - num_items
            description += f'and {remaining} additional dataset(s) '

        if hasattr(self, 'std_startdate') and self.std_startdate is not None and hasattr(self, 'std_enddate') and self.std_enddate is not None:
            description += f'with reference data standard deviation bands calculated from {self.std_startdate} to {self.std_enddate} '
            
        self.logger.debug('Description: %s', description)
        return description

    def run(self, 
            var,                         # Can be str or list (required)
            units=None,                  # Can be str or list 
            outputdir='./',
            rebuild=True, 
            dpi=300, 
            format='png', 
            plot_type=None,              # Override data_type if needed
            ref_std_data=None,
            plot_std=False):
        """
        Unified run method that handles all plotting scenarios.
        
        Args:
            var (str or list): Variable name(s) to be plotted.
            units (str or list, optional): Units of the variable(s).
            outputdir (str): Output directory to save the plot.
            rebuild (bool): If True, rebuild the plot even if it already exists.
            dpi (int): Dots per inch for the plot.
            format (str): Format of the plot ('png' or 'pdf'). Default is 'png'.
            plot_type (str): Override data_type ('annual' or 'seasonal').
            ref_std_data (list): Reference standard deviation data for each season.
            plot_std (bool): Whether to plot standard deviation bands.
        """
        self.logger.info('Running PlotLatLonProfiles')
        # Normalize inputs
        variables = var if isinstance(var, list) else [var]
        if units is None:
            units_list = [None] * len(variables)
        elif isinstance(units, list):
            units_list = units
        else:
            units_list = [units] * len(variables)
        
        # Determine actual plot type
        actual_plot_type = plot_type or self.data_type
        
        # Check if this is a multi-variable case
        is_multi_variable = len(variables) > 1
        
        if actual_plot_type == 'seasonal' and is_multi_variable:
            # Multi-variable seasonal case
            return self._run_multi_seasonal(variables, units_list)
        
        elif actual_plot_type == 'seasonal':
            # Single variable seasonal case
            return self._run_seasonal_single(variables[0], units_list[0], 
                                        outputdir, rebuild, dpi, format, ref_std_data)
        
        else:
            # annual single variable case
            return self._run_annual_single(variables[0], units_list[0],
                                        outputdir, rebuild, dpi, format, plot_std)

    def _run_annual_single(self, var, units, outputdir, rebuild, dpi, format, plot_std):
        """Private method for annual single variable plotting."""
        data_label = self.set_data_labels()
        ref_label = self.set_ref_label()
        description = self.set_description(region=self.region)
        title = self.set_title(region=self.region, var=var, units=units)
        
        if plot_std and (self.ref_std_data is not None):
            title += " (±2σ)"
            description += " with standard deviation bands"

        fig, _ = self.plot(data_labels=data_label, ref_label=ref_label, title=title)
        
        self.save_plot(fig, description=description, rebuild=rebuild,
                    outputdir=outputdir, dpi=dpi, format=format, diagnostic='lat_lon_profiles')
        
        self.logger.info('PlotLatLonProfiles completed successfully')

    def _run_seasonal_single(self, var, units, outputdir, rebuild, dpi, format, ref_std_data):
        """Private method for seasonal single variable plotting."""
        data_labels = self.set_data_labels()
        description = self.set_description(region=self.region)
        title = self.set_title(region=self.region, var=var, units=units)
        
        fig, axs = self.plot_seasonal_lines(data_labels=data_labels, 
                                            title=title,
                                            ref_std_data=ref_std_data)
        
        seasonal_diagnostic = 'lat_lon_profiles_seasonal'
        if self.mean_type:
            seasonal_diagnostic = f'lat_lon_profiles_seasonal_{self.mean_type}'

        self.save_plot(fig, description=description, 
                       rebuild=rebuild, outputdir=outputdir, dpi=dpi, format=format, 
                       diagnostic=seasonal_diagnostic)
        
        self.logger.info('PlotLatLonProfiles completed successfully')

    def _run_multi_seasonal(self, variables, units_list):
        """Private method for multi-variable seasonal plotting."""
        # Create combined labels
        data_labels = []
        for i, var_name in enumerate(variables):
            unit = units_list[i] if units_list and i < len(units_list) and units_list[i] else ""
            label = f"{var_name} ({unit})" if unit else var_name
            data_labels.append(label)
        
        # Use the seasonal plotting function
        fig, axs = plot_seasonal_lat_lon_profiles(
            seasonal_data=self.data,                    # Should be [DJF, MAM, JJA, SON] structure
            ref_data=self.ref_data,
            ref_std_data=None,
            data_labels=data_labels,
            title=f"Multi-variable Seasonal Comparison: {', '.join(variables)}",
            loglevel=self.loglevel
        )
        
        self.save_plot(fig, 
                       description=f"Multi-variable seasonal comparison: {', '.join(variables)}",
                       diagnostic='lat_lon_profiles_seasonal_multi')
        
        self.logger.info('PlotLatLonProfiles completed successfully')
        return fig, axs

    def plot_seasonal_lines(self, 
                            data_labels=None, 
                            title=None, 
                            style=None,
                            ref_std_data=None):
        """
        Plot seasonal means using plot_seasonal_lat_lon_profiles.
        Creates a 4-panel plot with DJF, MAM, JJA, SON only (no annual).

        Args:
            data_labels (list): List of data labels.
            title (str): Title of the plot.
            style (str): Plotting style.
            ref_std_data (list): Reference standard deviation data for each season.

        Returns:
            fig (matplotlib.figure.Figure): Figure object.
            axs (list): List of axes objects.
        """
        if self.data_type != 'seasonal':
            raise ValueError("plot_seasonal_lines() can only be used with data_type='seasonal'")
        
        if not self.data or len(self.data) < 4:
            raise ValueError("Seasonal data must contain at least 4 elements: [DJF, MAM, JJA, SON]")
        
        # Use first 4 seasons only (DJF, MAM, JJA, SON)
        seasonal_data_only = self.data[:4]
        seasonal_ref_only = self.ref_data[:4] if self.ref_data and len(self.ref_data) >= 4 else None
        
        # Handle ref std data if available
        seasonal_ref_std_only = None
        if self.ref_std_data:
            seasonal_ref_std_only = self.ref_std_data[:4] if len(self.ref_std_data) >= 4 else self.ref_std_data
        
        self.logger.debug(f'Plotting {len(seasonal_data_only)} seasons')
        
        return plot_seasonal_lat_lon_profiles(
            seasonal_data=seasonal_data_only,
            ref_data=seasonal_ref_only,
            ref_std_data=seasonal_ref_std_only,
            data_labels=None,
            title=title,
            style=style,
            loglevel=self.loglevel
        )