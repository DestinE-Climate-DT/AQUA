""" PlotSeaIce doc """
import os
import xarray as xr
from matplotlib import pyplot as plt

from aqua.diagnostics.core import OutputSaver
from aqua.exceptions import NoDataError, NotEnoughDataError
from aqua.logger import log_configure, log_history
from aqua.graphics import plot_timeseries, plot_seasonalcycle, ConfigStyle
from aqua.configurer import ConfigPath
from aqua.util import get_realizations
from collections import defaultdict
from .util import defaultdict_to_dict, extract_dates, _check_list_regions_type

xr.set_options(keep_attrs=True)

class PlotSeaIce:
    """ 
    A class for processing and visualizing timeseries of integrated sea ice extent or volume.
    It is designed to work with AQUA-computed outputs (from the `SeaIce` diagnostic)
    repacking them into a unified format for easy comparison, labeling, and plotting.

    Args:
        monthly_models (xr.Dataset | list[xr.Dataset] | None, optional): 
            Monthly model datasets to be processed. Defaults to None.
        annual_models (xr.Dataset | list[xr.Dataset] | None, optional): 
            Annual model datasets to be processed. Defaults to None.
        monthly_ref (xr.Dataset | list[xr.Dataset] | None, optional): 
            Monthly reference datasets for comparison. Defaults to None.
        annual_ref (xr.Dataset | list[xr.Dataset] | None, optional): 
            Annual reference datasets for comparison. Defaults to None.
        monthly_std_ref (str, optional): Monthly standard deviation reference dataset identifier. Defaults to None.
        annual_std_ref (str, optional): Annual standard deviation reference dataset identifier. Defaults to None.
        model (str, optional): Name of the model associated with the dataset. Defaults to None.
        exp (str, optional): Experiment name related to the dataset. Defaults to None.
        source (str, optional): Source of the dataset. Defaults to None.
        catalog (str, optional): Catalog name of the dataset. Defaults to None.
        regions_to_plot (list, optional): 
            List of region names to be plotted (e.g., ['arctic', 'antarctic']). 
            If None, all available regions are plotted. Defaults to None.
        outputdir (str, optional): Directory to save output plots. Defaults to './'.
        rebuild (bool, optional): Whether to rebuild (overwrite) figure outputs if they already exist. Defaults to True.
        (overwrite) figure outputs if exists. (list, optional): 
            List of keys to include in the output filenames. If None, all keys are included. Defaults to None.
        dpi (int, optional): Resolution of saved figures (dots per inch). Defaults to 300.
        loglevel (str, optional): Logging level for debugging and information messages. Defaults to 'WARNING'.
    """

    def __init__(self, monthly_models=None, annual_models=None,
                 monthly_ref=None, annual_ref=None,
                 monthly_std_ref: str = None, annual_std_ref: str = None,
                 model: str = None, exp: str = None, source: str = None, catalog: str = None,
                 regions_to_plot: list = ['Arctic', 'Antarctic'], # this is a list of strings with the region names to plot
                 outputdir='./',
                 rebuild=True,
                 filename_keys=None,  # List of keys to keep in the filename. Default is None, which includes all keys.
                 dpi=300, loglevel='WARNING'):

        # logging setup
        self.loglevel = loglevel
        self.logger = log_configure(log_level=self.loglevel, log_name='PlotSeaIce')

        self.model = model
        self.exp = exp
        self.source = source
        self.catalog = catalog
        self.realizations = get_realizations(monthly_models) # TO BE UPDATED when also annual analysis will be implemented


        self.regions_to_plot = _check_list_regions_type(regions_to_plot, logger=self.logger)

        # define and check data types
        self.repacked_dict = self.repack_datasetlists(monthly_models=monthly_models, 
                                                      annual_models=annual_models, 
                                                      monthly_ref=monthly_ref, 
                                                      annual_ref=annual_ref, 
                                                      monthly_std_ref=monthly_std_ref, 
                                                      annual_std_ref=annual_std_ref)
        # Output & saving settings
        self.outputdir = outputdir
        self.rebuild = rebuild
        self.dpi = dpi
    
    def _check_as_datasets_list(self, datain) -> list[xr.Dataset | None] :
        """ Check that the input (`datain`) is either:
            - A single `xarray.Dataset` (which is converted into a list).
            - A list of `xarray.Dataset` objects (which may contain None values).
            - `None` (which is returned as is).
        Args:
            datain (xr.Dataset | list[xr.Dataset] | None): The input dataset(s) to check.
        """
        if datain is None:
            return datain
        elif isinstance(datain, xr.Dataset):
            return [datain]
        elif isinstance(datain, list):
            if not all((ds is None or isinstance(ds, xr.Dataset)) for ds in datain):
                raise ValueError("All elements of the list must be xarray.Dataset instances or None.")
            return datain
        else:
            raise ValueError(f"Invalid type: {type(datain)}. Expected xr.Dataset, list of xr.Dataset, or None.")

    def _get_region_name_in_datarray(self, da: xr.DataArray) -> str:
        """
        Get the region variable from the dataset or derive it from the variable name.
        
        Args:
            da (xr.DataArray): The data array to get the region name from.

        Returns:
            str: The region name.
        """
        if da is None:
            self.logger.error(f"DataArray is None. Cannot determine region without a valid DataArray.")
            raise KeyError(f"DataArray is None. Cannot determine region without a valid DataArray.")

        region = da.attrs.get("AQUA_region")

        # check if 'region' exists as an attribute of da
        if region:
            return region
        else:
            self.logger.warning("Region name attr not found. Try to get this from last part of xr.dataVariable name")
            var_name = da.name if da.name is not None else ""

            if var_name:
                region_from_name = var_name.split("_")[-1].capitalize()
                return region_from_name
            else:
                errmsg = (f"Dataset {da.attrs.get('name', 'Unnamed Dataset')} has no 'region' attribute "
                          f"and region could not be derived from the variable name.")
                self.logger.error(errmsg)
                raise KeyError(errmsg)

    def repack_datasetlists(self, **kwargs) -> dict:
        """
        Repack input datasets into a nested dictionary organized by method and region.
        The output dictionary is structured as::

            { method: { region: { str_data: [list of data arrays] }}}

        where: 'method' is extracted from the dataset attributes (defaulting to "Unknown").
        'region' is determined by self._get_region(dataset, data_var).
        'str_data' is the keyword with the data in input, and each value is a list of data arrays corresponding to that keyword.
        
        Args:
            **kwargs (dict): Keyword arguments, where each str_data is linked to 
                the kwargs in plot_timeseries() and each value is a list of xr.Dataset objects.
        
        Returns:
            dict: A nested dict containing the repacked data arrays.
        """
        # initialize repacked_defdict as a nested defaultdict with following structure:
        # method -> region -> str_data -> list of data arrays (can be of size one)
        repacked_defdict = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

        for str_data, dataset_list in kwargs.items():
            # normalize the input to a list of datasets (or None)
            dataset_list = self._check_as_datasets_list(dataset_list)

            # if the list is None, skip to the next str_data
            if dataset_list is None:
                continue
            
            for dataset in dataset_list:
                if dataset is None:
                    self.logger.warning("Warning: Found dataset as None in dataset_list during repacking data, skipping...")
                    continue

                method = dataset.attrs.get("AQUA_method", "Unknown")

                # process each data variable in the dataset
                for var_name, data_array in dataset.data_vars.items():
                    data_array.name = data_array.name or var_name
                    
                    # validate the region for the current data variable
                    region = self._get_region_name_in_datarray(data_array)

                    if self.regions_to_plot and (region not in self.regions_to_plot):
                        # if region is not in regions_to_plot, record as None
                        continue
                    else:
                        # in the nested defaultdicts an empty list is configured by default, thus directly append
                        repacked_defdict[method][region][str_data].append(data_array)

        # convert the nested defaultdicts to plain dictionaries recursively
        repacked_dict = defaultdict_to_dict(repacked_defdict)
        
        self.logger.info("Sea ice data repacked")
        return repacked_dict
    
    def _gen_str_from_attributes(self, datain: xr.DataArray | None) -> str:
        """
        Generate a string from the attributes of the input data.

        Args:
            datain (xr.DataArray): The data array to generate a string from.

        Returns:
            str: The string generated from the attributes of the input data.
        """
        if datain is None:
            return None
        
        required_attrs = ["AQUA_model", "AQUA_exp", "AQUA_source"]
        missing_attrs = [attr for attr in required_attrs if attr not in datain.attrs]

        if missing_attrs:
            self.logger.warning(f"These dataset global attrs is missing: {', '.join(missing_attrs)}.")

        # join the strs to make label
        return " ".join(str(datain.attrs[attr]) for attr in required_attrs if attr in datain.attrs)
    
    def _gen_labelname(self, datain: xr.DataArray | list[xr.DataArray] | None) -> str | list[str] | None:
        """Extract 'model', 'exp', 'source', and 'catalog' from attributes in input data and 
           generate a label or list of labels for each xr.dataArray to be used in the legend plot. 

        Args:
            datain (xr.DataArray | list[xr.DataArray] | None):
                - A single xr.DataArray: Generates a label from its attributes.
                - A list of xr.DataArray: Generates a list of labels for each data array.
                - None: Returns None.

        Returns:
            str | list[str] | None:
                - A single string if datain is a single xarray.DataArray.
                - A list of strings if datain is a list of xarray.DataArray objects.
                - None if datain is None.
        """
        if datain is None:
            return None
        if isinstance(datain, xr.DataArray):
            return self._gen_str_from_attributes(datain)
        if isinstance(datain, list) and all(isinstance(da, xr.DataArray) for da in datain):
            return [self._gen_str_from_attributes(da) for da in datain]

    def _getdata_fromdict(self, data_dict: dict, dkey: str) -> xr.DataArray | list[xr.DataArray] | None:
        """Retrieves data from a dictionary and returns either None, a single DataArray or a list of them

        Args:
            data_dict (dict): Dictionary containing the data (list of xr.DataArray or single xr.DataArray or None)
            dkey (str): The key to retrieve data from data_dict

        Returns:
            - A single xr.DataArray if the list contains only one element (reference data case)
            - A list of xr.DataArray if multiple elements are found (model data case)
            - `None` if the key is missing or the value is not a valid list of xr.DataArray 
        """
        values = data_dict.get(dkey, None)

        if isinstance(values, xr.DataArray):
            return values

        if isinstance(values, list):
            valid_values = [v for v in values if v is not None]

            if not valid_values:
                self.logger.error(f"No valid DataArrays found for key: {dkey}")
                return None

            if all(isinstance(v, xr.DataArray) for v in valid_values):
                return valid_values[0] if len(valid_values) == 1 else valid_values

            self.logger.error(f"Some elements in {dkey} are not DataArrays")

        self.logger.info(f"Returning 'None' for key: {dkey}")
        return None
    
    def _update_description(self, method, region, data_dict, region_idx):
        """
        Create the caption description from attributes returning the updated string
        
        Args:
            method (str): The method used to compute the data.
            region (str): The region to plot.
            data_dict (dict): The data dictionary.
            region_idx (int): The index of the region.
        """
        # initialise string if _description doesn't exist
        if not hasattr(self, '_description'):
            self._description = ''
        
        # generate dynamic string for regions
        if region not in self._description:
            if not hasattr(self, 'region_str'):
                self.region_str = region  # start with first region
            else:
                if region_idx == self.num_regions - 1:
                    self.region_str += f" and {region} regions"
                else:
                    self.region_str += f", {region}"
        
        # generate dynamic string for model data
        if hasattr(self, "data_labels") and self.data_labels:
            # remove duplicates while keeping order
            unique_labels = list(dict.fromkeys(self.data_labels))

            # extract model data from current dictionary
            model_data_dict = self._getdata_fromdict(data_dict, 'monthly_models')

            # Build per-model date string
            model_startdate_list = []
            if isinstance(model_data_dict, xr.DataArray):
                stdate, endate = extract_dates(model_data_dict)
                model_startdate_list = [f"{label} from {stdate} to {endate}" for label in unique_labels]
                self._description += f" {method} data from {stdate} to {endate} for {region}."
            elif isinstance(model_data_dict, list):
                for model_data in model_data_dict:
                    stdate, endate = extract_dates(model_data)
                    model_startdate_list.extend([f"{label} from {stdate} to {endate}" for label in unique_labels])
                    self._description += f" {method} data from {stdate} to {endate} for {region}."

            # build the model data string
            self.model_labels_str = (f"{', '.join(model_startdate_list)} "
                                     f"{'are' if len(model_startdate_list) > 1 else 'is'} "
                                     f"used as {'models' if len(model_startdate_list) > 1 else 'model'} data.")
        else:
            self.model_labels_str = ''

        # generate dynamic string for reference data
        if hasattr(self, "ref_label") and self.ref_label:
            if not hasattr(self, 'ref_label_list'):
                self.ref_label_list = []
            if self.ref_label not in self.ref_label_list:
                self.ref_label_list.append(f"{self.ref_label}")
            # check ref list
            if len(self.ref_label_list) == 1:
                self.ref_label_str = f" {self.ref_label_list[0]} is used as a reference."
            elif len(self.ref_label_list) == 2:
                self.ref_label_str = (f" {self.ref_label_list[0]} and {self.ref_label_list[1]} "
                                      f"are used as reference data for the respective regions.")
            else:
                ref_labels_str = ", ".join(self.ref_label_list[:-1]) + f", and {self.ref_label_list[-1]}"
                self.ref_label_str = f" {ref_labels_str} are used as references."
        else:
            self.ref_label_str = ''

        # generate string for reference std data
        if hasattr(self, "std_label") and self.std_label:
            sdtdata = self._getdata_fromdict(data_dict,'monthly_std_ref')
            std_sdate, std_edate = extract_dates(sdtdata[0]) 
            self.std_label_str = f" Reference data std ranges from {std_sdate} to {std_edate}."
        else:
            self.std_label_str = ''

        # generate plot type name
        if hasattr(self, "plot_type") and self.plot_type:
            if self.plot_type == 'seasonalcycle':
                pl_type = 'Seasonal cycle of the '
            elif self.plot_type == 'timeseries':
                pl_type = 'Time series of the '
            else:
                pl_type = ''
                
        # finally build the string caption (dynamically)
        self._description = ('{}Sea ice {} integrated over {}. {}{}{}').format(pl_type, method, 
                                                                               self.region_str, self.model_labels_str,
                                                                               self.ref_label_str, self.std_label_str)

    def regions_type_plotter(self, region_dict, style, **kwargs):
        """
        Loops over each region in region_dict and plots data either as a timeseries or a seasonal cycle
        depending on plot_type attribute.
        
        Args:
            region_dict (dict): Dictionary of regions and their associated data.
            style (str): Graphic style of the plot.
            **kwargs (dict): Additional keyword arguments passed on to the underlying plotting function.
        
        Returns:
            (fig, axes) : tuple. The figure and axes objects.
        """
        ConfigStyle(style=style, loglevel=self.loglevel)
        
        self.num_regions = len(region_dict)

        fig_height = 6 if self.plot_type == 'seasonalcycle' else 10

        fig, axes = plt.subplots(nrows=self.num_regions, ncols=1, 
                                 figsize=(fig_height, 4 * self.num_regions), squeeze=False)
        axes = axes.flatten()

        self.logger.debug("Start looping over sea ice regions")

        for region_idx, (ax, (region, data_dict)) in enumerate(zip(axes, region_dict.items())):

            self.logger.info(f"Processing {self.plot_type} for region: {region}")

            monthly_models = self._getdata_fromdict(data_dict, 'monthly_models')
            annual_models  = self._getdata_fromdict(data_dict, 'annual_models')
            monthly_ref    = self._getdata_fromdict(data_dict, 'monthly_ref')
            annual_ref     = self._getdata_fromdict(data_dict, 'annual_ref')
            monthly_std    = self._getdata_fromdict(data_dict, 'monthly_std_ref')
            annual_std     = self._getdata_fromdict(data_dict, 'annual_std_ref')

            # create labels
            if monthly_models is not None:
                self.data_labels = [self._gen_labelname(da) for da in monthly_models]
            else:
                self.data_labels = None
            self.ref_label = self._gen_labelname(monthly_ref)
            self.std_label = self._gen_labelname(monthly_std)

            # call the appropriate plotting function
            if self.plot_type == 'timeseries':
                fig, ax = plot_timeseries(monthly_data=monthly_models,
                                          annual_data=annual_models,
                                          ref_monthly_data=monthly_ref,
                                          ref_annual_data=annual_ref,
                                          std_monthly_data=monthly_std,
                                          std_annual_data=annual_std,
                                          data_labels=self.data_labels,
                                          ref_label=self.ref_label,
                                          style=style,
                                          fig=fig,
                                          ax=ax,
                                          **kwargs)

            elif self.plot_type == 'seasonalcycle':
                fig, ax = plot_seasonalcycle(data=monthly_models,
                                             ref_data=monthly_ref,
                                             std_data=monthly_std,
                                             data_labels=self.data_labels,
                                             ref_label=self.ref_label,
                                             style=style,
                                             fig=fig,
                                             ax=ax,
                                             **kwargs)
            else:
                raise ValueError(f"Unknown plot_type function name: {self.plot_type}")

            self._update_description(self.method, region, data_dict, region_idx)

            ax.set_title(f"Sea ice {self.method}: region {region}")

        return fig, axes

    def plot_seaice(self, plot_type='timeseries', save_pdf=True, save_png=True, style=None, **kwargs):
        """
        Plot sea ice data for each region, either as timeseries or seasonal cycle.
        
        Args:
            plot_type (str, optional): Type of plot to generate. Options are 
                `'timeseries'` or `'seasonalcycle'`. Defaults to `'timeseries'`.
            save_pdf (bool, optional): Whether to save the figure as a PDF. Defaults to True.
            save_png (bool, optional): Whether to save the figure as a PNG. Defaults to True.
            style (str, optional): Override the plotting style. Default to None (which will get the style from config file or fallback to'aqua').
            **kwargs: Additional keyword arguments passed to the region-specific plotting function.
        """
        self.plot_type = plot_type

        self.logger.info(f"Plotting sea ice {self.plot_type}")

        valid_type_plots = ['timeseries', 'seasonalcycle']

        if self.plot_type not in valid_type_plots:
            raise ValueError(f"Invalid plot_type. Allowed plots are: {valid_type_plots}")

        for method, region_dict in self.repacked_dict.items():

            self.method = method

            self.logger.info(f"Processing method: {self.method}")

            # plot per-region using loop on the same fig
            fig, axes = self.regions_type_plotter(region_dict, style, **kwargs)
            
            plt.tight_layout()
            self.logger.info(f"Plotting of all regions for method '{self.method}' completed")

            metadata = {"Description": self._description}
            self.logger.debug(f"Description: {self._description}")

            self.save_fig(fig, save_png, save_pdf,
                          metadata=metadata,
                          region_dict=region_dict)

    def save_fig(self, fig, save_png: bool, save_pdf: bool,
                 metadata: dict = None, region_dict: dict = None):
        """
        Save a matplotlib figure in PNG and/or PDF format with associated metadata.
        
        Args:
            fig (matplotlib.figure.Figure): The figure object to be saved.
            save_png (bool): Whether to save the figure as a PNG file.
            save_pdf (bool): Whether to save the figure as a PDF file.
            metadata (dict, optional): Metadata such as description to be saved. Defaults to None.
            region_dict (dict, optional): Dictionary of regions plotted. Used to generate output filename. Defaults to None.
        """
        if save_png or save_pdf:
            self.logger.debug(f"Saving figure as format(s): {', '.join(fmt for fmt, flag in [('PNG', save_png), ('PDF', save_pdf)] if flag)}")
            output_saver = OutputSaver(diagnostic='seaice', catalog=self.catalog, model=self.model, exp=self.exp,
                                        loglevel=self.loglevel, outputdir=self.outputdir, realization=self.realizations)

            diagnostic_product = self.plot_type
            
            extra_keys = {'method': self.method,
                          'region': '_'.join(region_dict.keys())}
            
            if save_pdf: 
                output_saver.save_pdf(fig=fig, diagnostic_product=diagnostic_product, metadata=metadata,
                                      rebuild=self.rebuild, extra_keys=extra_keys)
            if save_png: 
                output_saver.save_png(fig=fig, diagnostic_product=diagnostic_product, metadata=metadata,
                                      rebuild=self.rebuild, extra_keys=extra_keys)
