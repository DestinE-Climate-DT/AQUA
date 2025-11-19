import xarray as xr
import matplotlib.pyplot as plt
from aqua.logger import log_configure
from aqua.diagnostics.core import OutputSaver
from aqua.util import get_realizations
from .multiple_hovmoller import plot_multi_hovmoller
from .multiple_timeseries import plot_multi_timeseries

xr.set_options(keep_attrs=True)


class PlotHovmoller:
    """
    Class for plotting Hovmoller diagrams and timeseries from AQUA ocean drift diagnostics.

    This class provides methods to generate, customize, and save Hovmoller and timeseries plots
    using xarray datasets and AQUA conventions. It handles metadata extraction, plot styling,
    and output file management.
    """
    def __init__(self,
                 data: list[xr.Dataset],
                 diagnostic_name: str = "oceandrift",
                 outputdir: str = ".",
                 loglevel: str = "WARNING"):
        """
        Initialize the PlotHovmoller class.

        Args:
            data (list[xr.Dataset]): List of xarray datasets containing the data to be plotted
            diagnostic_name (str): Name of the diagnostic, default is "oceandrift"
            outputdir (str): Directory where the output will be saved, default is current directory
            loglevel (str): Logging level, default is "WARNING"
        """
        self.data = data

        self.loglevel = loglevel
        self.logger = log_configure(self.loglevel, "PlotHovmoller")

        self.diagnostic = diagnostic_name
        self.vars = list(self.data[0].data_vars)
        self.logger.debug("Variables in data: %s", self.vars)

        # Getting metadata from the first dataset
        self.catalog = self.data[0][self.vars[0]].AQUA_catalog
        self.model = self.data[0][self.vars[0]].AQUA_model
        self.exp = self.data[0][self.vars[0]].AQUA_exp
        self.region = self.data[0].AQUA_region
        self.levels = None  # To be set when plotting timeseries
        self.realizations = get_realizations(self.data[0][self.vars[0]])

        self.outputsaver = OutputSaver(
            diagnostic=self.diagnostic,
            catalog=self.catalog,
            model=self.model,
            exp=self.exp, 
            outputdir=outputdir, 
            realization=self.realizations,
            loglevel=self.loglevel)

    def plot_hovmoller(self, rebuild: bool = True, save_pdf: bool = True,
                       save_png: bool = True, dpi: int = 300):
        """
        Plot the Hovmoller diagram for the given data.

        This method sets the title, description, vmax, vmin, and texts for the plot.
        It then calls the `plot_multi_hovmoller` function to create the plot and
        saves it using the `OutputSaver`.

        Args:
            rebuild (bool): Whether to rebuild the output, default is True.
            save_pdf (bool): Whether to save the plot as a PDF, default is True.
            save_png (bool): Whether to save the plot as a PNG, default is True.
            dpi (int): Dots per inch for the saved figure. Default is 300.

        Returns:
            None
        """
        self.set_suptitle(content="Hovmöller")
        self.set_title()
        self.set_description(content="Hovmöller plot of spatially averaged")
        self.set_data_type()
        self.set_texts()
        self.set_vmax_vmin()
        self.logger.debug("Plotting Hovmöller for variables: %s", self.vars)
        fig = plot_multi_hovmoller(
            maps=self.data,
            variables=self.vars,
            loglevel=self.loglevel,
            title=self.suptitle,
            titles=self.title_list,
            vmax=self.vmax,
            vmin=self.vmin,
            cmap=self.cmap,
            text=self.texts
        )
        formats = []
        if save_pdf:
            formats.append('pdf')
        if save_png:
            formats.append('png')

        for format in formats:
            self.save_plot(fig, diagnostic_product="hovmoller", metadata={"description": self.description},
                           rebuild=rebuild, dpi=dpi, format=format, extra_keys={'region': self.region})

    def plot_timeseries(self,
                        levels: list = None,
                        rebuild: bool = True, save_pdf: bool = True,
                        save_png: bool = True, dpi: int = 300):
        """
        Plot the timeseries for the given data.

        This method sets the title, description, vmax, vmin, and texts for the plot.
        It then calls the `plot_multi_timeseries` function to create the plot and
        saves it using the `OutputSaver`.

        Args:
            levels (list, optional): List of levels to plot. Default is None.
            rebuild (bool): Whether to rebuild the output, default is True.
            save_pdf (bool): Whether to save the plot as a PDF, default is True.
            save_png (bool): Whether to save the plot as a PNG, default is True.
            dpi (int): Dots per inch for the saved figure. Default is 300.

        Returns:
            None
        """
        self.levels = levels
        self.set_levels()
        self.set_data_for_levels()
        self.set_suptitle(content="Timeseries")
        self.set_title()
        self.set_description(content="Timeseries of spatially averaged")
        self.set_data_type()
        self.set_texts()
        self.set_vmax_vmin()
        self.set_line_plot_colours()
        self.logger.debug("Plotting Timeseries for variables: %s", self.vars)
        fig = plot_multi_timeseries(
            maps=self.data,
            levels=self.timeseries_labels,
            line_plot_colours=self.line_plot_colours,
            variables=self.vars,
            loglevel=self.loglevel,
            title=self.suptitle,
            titles=self.title_list,
            vmax=self.vmax,
            vmin=self.vmin,
            cmap=self.cmap,
            text=self.texts
        )
        formats = []
        if save_pdf:
            formats.append('pdf')
        if save_png:
            formats.append('png')

        for format in formats:
            self.save_plot(fig, diagnostic_product="timeseries", metadata={"description": self.description},
                           rebuild=rebuild, dpi=dpi, format=format, extra_keys={'region': self.region})

    def set_levels(self):
        """
        Set the levels and corresponding labels for timeseries plots.
        If no levels are provided, use a default set of standard ocean depths.
        """
        level_unit = self.data[0].level.attrs['units']
        if self.levels is None:
            self.levels = [0, 100, 300, 600, 1000, 2000, 4000]
        self.timeseries_labels = [f"{level} {level_unit}" for level in self.levels]

    def set_data_for_levels(self):
        """
        Set the data for the specified levels.
        This method extracts the data at the specified levels from the original data.
        """
        self.logger.debug("Setting data for levels: %s", self.levels)
        new_data_list = []
        for _, data in enumerate(self.data):
            new_data_level_list = []
            for level in self.levels:
                self.logger.debug("Extracting data for level: %s", level)
                # Interpolate the data to the specified levels
                if level == 0:
                    new_data = data.isel(level=0)
                else: 
                    new_data = data.interp(level=level, method='nearest')
                new_data_level_list.append(new_data)
            merged_data = xr.concat(new_data_level_list, dim='level', coords='different')
            new_data_list.append(merged_data)
        self.data = new_data_list

    def set_line_plot_colours(self):
        """
        Set the color list for line plots based on the number of levels.
        """
        nlev = len(self.levels)
        cmap = plt.cm.plasma_r
        self.line_plot_colours = [cmap(0.3 + 0.7*i/(nlev-1)) for i in range(nlev)]

    def set_suptitle(self, content: str = None):
        """Set the suptitle for the Hovmoller plot."""
        self.suptitle = f"{content} plot in the {self.region} - {self.catalog} {self.model} {self.exp}"
        self.logger.debug(f"Suptitle set to: {self.suptitle}")

    def set_title(self):
        """
        Set the title for the Hovmoller plot.
        This method can be extended to set specific titles based on the data.
        """
        self.title_list = []
        for j in range(len(self.data)):
            for _, var in enumerate(self.vars):
                if j == 0:
                    title = f"{var} ({self.data[j][var].attrs.get('units')})"
                else:
                    title = None
                self.title_list.append(title)
        self.logger.debug("Title list set to: %s", self.title_list)

    def set_description(self, content: str = None):
        """Set the description for the Hovmoller plot."""

        self.description = f'{content} {self.region} region for experiment {self.catalog} {self.model} {self.exp}'

    def set_vmax_vmin(self):
        """
        Set the vmax and vmin for the Hovmoller plot.
        This method can be extended to set specific vmax and vmin values.
        """
        self.logger.debug("Setting vmax and vmin")
        hovmoller_plot_dic = {
            'thetao' :
                {
                    'full': {'vmax': 40, 'vmin': 10 },
                    'anom_t0': {'vmax': 6, 'vmin': -6, 'cbar': 'coolwarm'},
                    'std_anom_t0': {'vmax': 5, 'vmin': -5, 'cbar': 'coolwarm'},
                    'anom_tmean': {'vmax': 6, 'vmin': -6, 'cbar': 'coolwarm'},
                    'std_anom_tmean': {'vmax': 5, 'vmin': -5, 'cbar': 'coolwarm'},
                },
            'so' :
                {
                    'full': {'vmax': 38, 'vmin': 33, 'cbar': 'coolwarm'},
                    'anom_t0': {'vmax': 0.9, 'vmin': -0.3, 'cbar': 'coolwarm'},
                    'std_anom_t0': {'vmax': 5, 'vmin': -6, 'cbar': 'coolwarm'},
                    'anom_tmean': {'vmax': 5, 'vmin': -5, 'cbar': 'coolwarm'},
                    'std_anom_tmean': {'vmax': 1, 'vmin': -1, 'cbar': 'coolwarm'},
                }
        }
        self.vmax = []
        self.vmin = []
        self.cmap = []
        for type in self.data_type:
            for var in self.vars:
                self.vmax.append(hovmoller_plot_dic[var][type].get('vmax'))
                self.vmin.append(hovmoller_plot_dic[var][type].get('vmin'))
                self.cmap.append(hovmoller_plot_dic[var][type].get('cbar', 'jet'))      

    def set_data_type(self):
        """
        Set the data type list for the Hovmoller plot based on dataset attributes.
        This method can be extended to set specific data types.
        """
        self.logger.debug("Setting data types")
        self.data_type = []
        for data in self.data:
            type = data.attrs.get('AQUA_ocean_drift_type', 'NA')
            self.data_type.append(type)

    def set_texts(self):
        """
        Set the texts for the Hovmoller plot.
        This method can be extended to set specific texts.
        """
        type_label_mapping = {
            'full': 'Full values',
            'anom_t0': 'Anomalies from t0',
            'std_anom_t0': 'Standardized anomalies from t0',
            'anom_tmean': 'Anomalies from time mean',
            'std_anom_tmean': 'Standardized anomalies from time mean'
        }

        self.texts = []
        for _, data in enumerate(self.data):
            for j, _ in enumerate(self.vars):
                if j == 0:
                    odrift_type = data.attrs.get('AQUA_ocean_drift_type', 'NA')
                    descr_label = type_label_mapping.get(odrift_type, odrift_type)
                    self.texts.append(descr_label)
                else:
                    self.texts.append(None)
        self.logger.debug("Texts set to: %s", self.texts)

    def save_plot(self, fig, diagnostic_product: str = None, extra_keys: dict = None,
                  rebuild: bool = True,
                  dpi: int = 300, format: str = 'png', metadata: dict = None):
        """
        Save the plot to a file.

        Args:
            fig (matplotlib.figure.Figure): The figure to be saved.
            diagnostic_product (str): The name of the diagnostic product. Default is None.
            extra_keys (dict): Extra keys to be used for the filename (e.g. season). Default is None.
            rebuild (bool): If True, the output files will be rebuilt. Default is True.
            dpi (int): The dpi of the figure. Default is 300.
            format (str): The format of the figure. Default is 'png'.
            metadata (dict): The metadata to be used for the figure. Default is None.
                             They will be complemented with the metadata from the outputsaver.
                             We usually want to add here the description of the figure.
        """
        if format == 'png':
            result = self.outputsaver.save_png(fig, diagnostic_product=diagnostic_product, rebuild=rebuild,
                                               extra_keys=extra_keys, metadata=metadata, dpi=dpi)
        elif format == 'pdf':
            result = self.outputsaver.save_pdf(fig, diagnostic_product=diagnostic_product, rebuild=rebuild,
                                               extra_keys=extra_keys, metadata=metadata)
        self.logger.info(f"Figure saved as {result}")
