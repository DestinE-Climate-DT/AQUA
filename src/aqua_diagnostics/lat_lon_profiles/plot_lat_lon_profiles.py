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
        
        self.std_data = None
        self.ref_std_data = ref_std_data
        
        self.len_data, self.len_ref = self._check_data_length()
        self.get_data_info()

    def set_data_labels(self):
        """Set the data labels for the plot based on data_type."""
        if not self.data or len(self.data) == 0:
            self.logger.warning('No data available for label generation')
            return []
        
        data_labels = []
        
        if self.data_type == 'annual':
            # For annual plots, use self.models and self.exps for each dataset
            for i in range(len(self.data)):
                if i < len(self.models) and i < len(self.exps):
                    label = f'{self.models[i]} {self.exps[i]}'
                else:
                    # Fallback to generic label if metadata not available
                    label = f'Dataset {i+1}'
                data_labels.append(label)
        
        elif self.data_type == 'seasonal':
            # For seasonal plots, use the first model/exp (since it's one dataset)
            if len(self.models) > 0 and len(self.exps) > 0:
                data_labels.append(f'{self.models[0]} {self.exps[0]}')
            else:
                data_labels.append('Dataset 1')
        
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
        
        if not self.data or len(self.data) == 0:
            self.logger.warning('No data available for metadata extraction')
            return self._set_defaults()
        
        # Get first data item based on data_type
        first_data = self._get_first_data_item()
        
        if first_data is not None and hasattr(first_data, 'AQUA_catalog'):
            self.catalogs = [first_data.AQUA_catalog]
            self.models = [first_data.AQUA_model]
            self.exps = [first_data.AQUA_exp]
            self.logger.debug(f'Metadata extracted: {self.models[0]} {self.exps[0]}')
            
            # Extract mean_type from data if not already set
            if self.mean_type is None and hasattr(first_data, 'mean_type'):
                self.mean_type = first_data.mean_type
        else:
            self.logger.warning('Data has no metadata attributes')
            self._set_defaults()
        
        # Handle std dates
        self.std_startdate = None
        self.std_enddate = None

    def _get_first_data_item(self):
        """Get the first data item based on data_type."""
        if self.data_type == 'annual':
            return self.data[0] if self.data else None
        elif self.data_type == 'seasonal':
            first_season = self.data[0]
            return first_season[0] if isinstance(first_season, list) else first_season
        return None

    def _set_defaults(self):
        """Set default values for metadata."""
        self.catalogs, self.models, self.exps = [], [], []

    def plot(self, data_labels=None, ref_label=None, title=None, clean_data=True):
        """
        Unified plotting method that handles all plotting scenarios based on data_type.
        
        Args:
            data_labels (list, optional): Labels for the data.
            ref_label (str, optional): Label for the reference data.  
            title (str, optional): Title for the plot.
            clean_data (bool, optional): Whether to clean single-timestep dimensions. Default True.
            
        Returns:
            tuple: Matplotlib figure and axes objects.
        """
        if not self.data or len(self.data) == 0:
            raise ValueError("No data available for plotting")
        
        if self.data_type == 'seasonal':
            # For seasonal plots, delegate to the specialized seasonal method
            return self._plot_seasonal(data_labels=data_labels, title=title)
        
        # For annual plots, handle data cleaning if requested
        data_to_plot = self.data
        ref_to_plot = self.ref_data
        
        if clean_data:
            data_to_plot = self._clean_temporal_dims(self.data)
            if self.ref_data is not None:
                ref_to_plot = self._clean_temporal_dims([self.ref_data])[0]
        
        # Call the graphics function
        return plot_lat_lon_profiles(
            data=data_to_plot,
            ref_data=ref_to_plot,
            std_data=self.std_data,
            ref_std_data=self.ref_std_data,
            data_labels=data_labels,
            ref_label=ref_label,
            title=title,
            loglevel=self.loglevel
        )

    def _clean_temporal_dims(self, data_list):
        """
        Clean single-timestep temporal dimensions from data.
        
        Args:
            data_list (list): List of DataArrays to clean
            
        Returns:
            list: List of cleaned DataArrays
        """
        cleaned_data = []
        for i, data_item in enumerate(data_list):
            if data_item is None:
                cleaned_data.append(None)
                continue
            
            # Check if has single time dimension that should be removed
            if 'time' in data_item.dims and data_item.sizes.get('time', 0) == 1:
                cleaned_item = data_item.isel(time=0, drop=True)
                self.logger.debug(f"Removed single time dimension from data {i}")
            else:
                cleaned_item = data_item
            
            cleaned_data.append(cleaned_item)
            self.logger.debug(f"Data {i}: shape={cleaned_item.shape}, dims={cleaned_item.dims}")
        
        return cleaned_data

    def _plot_seasonal(self, data_labels=None, title=None):
        """
        Private method for seasonal plotting (delegates to existing plot_seasonal_lines).
        """
        return self.plot_seasonal_lines(data_labels=data_labels, title=title)
    
    def save_plot(self, 
                  fig, 
                  var: str = None, 
                  description: str = None, 
                  region: str = None, 
                  rebuild: bool = True,
                  outputdir: str = './', 
                  dpi: int = 300, 
                  format: str = 'png', 
                  diagnostic: str = None):
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
        metadata = {
            'catalog': getattr(self, 'catalogs', ['unknown_catalog'])[0],
            'model': getattr(self, 'models', ['unknown_model'])[0], 
            'exp': getattr(self, 'exps', ['unknown_exp'])[0]
        }

        outputsaver = OutputSaver(diagnostic='lat_lon_profiles', outputdir=outputdir,
                                loglevel=self.loglevel, **metadata)
        
        # Build extra_keys
        extra_keys = {}
        if var: extra_keys['var'] = var
        if region: extra_keys['region'] = region.replace(' ', '').lower()
        
        # diagnostic_product must match the one used in OutputSaver
        base_diagnostic = diagnostic or 'lat_lon_profiles'
        if self.mean_type:
            diagnostic_product = f"{base_diagnostic}_{self.mean_type}"
        else:
            diagnostic_product = base_diagnostic
            self.logger.warning("mean_type not available, filename will not distinguish between zonal/meridional profiles")        
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
            description += f'with standard deviation from {self.std_startdate} to {self.std_enddate} '

        self.logger.debug('Description: %s', description)
        return description

    def run(self, 
            var,                         # Can be str or list (required)
            units=None,                  # Can be str or list 
            region=None, 
            outputdir='./',
            rebuild=True, 
            dpi=300, 
            format='png', 
            plot_type=None,              # Override data_type if needed
            plot_std=False):
        """
        Unified run method that handles all plotting scenarios.
        
        Args:
            var (str or list): Variable name(s) to be plotted.
            units (str or list, optional): Units of the variable(s).
            region (str): Region to be used in the title and description.
            outputdir (str): Output directory to save the plot.
            rebuild (bool): If True, rebuild the plot even if it already exists.
            dpi (int): Dots per inch for the plot.
            format (str): Format of the plot ('png' or 'pdf'). Default is 'png'.
            plot_type (str): Override data_type ('annual' or 'seasonal').
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
            return self._run_seasonal_single(variables[0], units_list[0], region, 
                                        outputdir, rebuild, dpi, format)
        
        else:
            # annual single variable case
            return self._run_annual_single(variables[0], units_list[0], region,
                                        outputdir, rebuild, dpi, format, plot_std)

    def _run_annual_single(self, var, units, region, outputdir, rebuild, dpi, format, plot_std):
        """Private method for annual single variable plotting."""
        data_label = self.set_data_labels()
        ref_label = self.set_ref_label()
        description = self.set_description(region=region)
        title = self.set_title(region=region, var=var, units=units)
        
        if plot_std and (self.std_data is not None or self.ref_std_data is not None):
            title += " (±2σ)"
            description += " with standard deviation bands"

        fig, _ = self.plot(data_labels=data_label, ref_label=ref_label, title=title)
        
        region_short = region.replace(' ', '').lower() if region is not None else None
        self.save_plot(fig, var=var, description=description, region=region_short, rebuild=rebuild,
                    outputdir=outputdir, dpi=dpi, format=format, diagnostic='lat_lon_profiles')
        
        self.logger.info('PlotLatLonProfiles completed successfully')

    def _run_seasonal_single(self, var, units, region, outputdir, rebuild, dpi, format):
        """Private method for seasonal single variable plotting."""
        data_labels = self.set_data_labels()
        description = self.set_description(region=region)
        title = self.set_title(region=region, var=var, units=units)
        
        fig, axs = self.plot_seasonal_lines(data_labels=data_labels, 
                                            title=title)
        
        region_short = region.replace(' ', '').lower() if region is not None else None

        seasonal_diagnostic = 'lat_lon_profiles_seasonal'
        if self.mean_type:
            seasonal_diagnostic = f'lat_lon_profiles_seasonal_{self.mean_type}'

        self.save_plot(fig, var=var, description=description, region=region_short, 
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
        
        # Use the existing seasonal plotting function
        fig, axs = plot_seasonal_lat_lon_profiles(
            seasonal_data=self.data,                    # Should be [DJF, MAM, JJA, SON] structure
            ref_data=self.ref_data,
            std_data=None,                     # Could be added later if needed
            ref_std_data=None,
            data_labels=data_labels,
            title=f"Multi-variable Seasonal Comparison: {', '.join(variables)}",
            loglevel=self.loglevel
        )
        
        self.save_plot(fig, var='_'.join(variables), 
                    description=f"Multi-variable seasonal comparison: {', '.join(variables)}",
                    diagnostic='lat_lon_profiles_seasonal_multi')
        
        self.logger.info('PlotLatLonProfiles completed successfully')
        return fig, axs

    def plot_seasonal_lines(self, 
                            data_labels=None, 
                            title=None, 
                            style=None):
        """
        Plot seasonal means using plot_seasonal_lat_lon_profiles.
        Creates a 4-panel plot with DJF, MAM, JJA, SON only (no annual).

        Args:
            data_labels (list): List of data labels.
            title (str): Title of the plot.
            style (str): Plotting style.
            std_data (list): Standard deviation data for each season.
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
            std_data=None,
            ref_std_data=seasonal_ref_std_only,
            data_labels=None,
            title=title,
            style=style,
            loglevel=self.loglevel
        )