""" PlotSeaIce doc """
import os
import xarray as xr
from matplotlib import pyplot as plt

from aqua.exceptions import NoDataError, NotEnoughDataError
from aqua.logger import log_configure, log_history
from aqua.util import ConfigPath, OutputSaver
from aqua.graphics import plot_timeseries
from collections import defaultdict
from .util import defaultdict_to_dict

xr.set_options(keep_attrs=True)

class PlotSeaIce:
    """ A class for processing and visualizing timeseries of integrated sea ice extent or volume. """

    ALLOWED_METHODS = ['extent', 'volume']

    def __init__(self, monthly_models=None, annual_models=None,
                 monthly_ref=None, annual_ref=None,
                 monthly_std_ref: str = None, annual_std_ref: str = None,
                 model: str = None, exp: str = None, source: str = None, catalog: str = None,
                 regions_to_plot: list = None, # ['Arctic', 'Antarctic'], # this is a list of strings with the region names to plot
                 outdir='./', rebuild=True, 
                 filename_keys=None,  # List of keys to keep in the filename. Default is None, which includes all keys.
                 dpi=300, loglevel='WARNING'):
        """
        Initializes the PlotSeaIce class.
        This constructor sets up logging, initializes input datasets, processes data, and configures output settings
        for plotting sea ice extent and volume.
        Args:
            monthly_models (xr.Dataset | list[xr.Dataset] | None, optional): 
                Monthly model datasets to be processed. Defaults to None.
            annual_models (xr.Dataset | list[xr.Dataset] | None, optional): 
                Annual model datasets to be processed. Defaults to None.
            monthly_ref (xr.Dataset | list[xr.Dataset] | None, optional): 
                Monthly reference datasets for comparison. Defaults to None.
            annual_ref (xr.Dataset | list[xr.Dataset] | None, optional): 
                Annual reference datasets for comparison. Defaults to None.
            monthly_std_ref (str, optional): 
                Monthly standard deviation reference dataset identifier. Defaults to None.
            annual_std_ref (str, optional): 
                Annual standard deviation reference dataset identifier. Defaults to None.
            model (str, optional): 
                Name of the model associated with the dataset. Defaults to None.
            exp (str, optional): 
                Experiment name related to the dataset. Defaults to None.
            source (str, optional): 
                Source of the dataset. Defaults to None.
            catalog (str, optional): 
                Catalog name of the dataset. Defaults to None.
            regions_to_plot (list, optional): 
                List of region names to be plotted (e.g., `['Arctic', 'Antarctic']`). 
                If None, all available regions are plotted. Defaults to None.
            outdir (str, optional): 
                Directory to save output plots. Defaults to './'.
            rebuild (bool, optional): 
                Whether to rebuild (overwrite) figure outputs if they already exist. Defaults to True.
            (overwrite) figure outputs if exists. (list, optional): 
                List of keys to include in the output filenames. If None, all keys are included. Defaults to None.
            dpi (int, optional): 
                Resolution of saved figures (dots per inch). Defaults to 300.
            loglevel (str, optional): 
                Logging level for debugging and information messages. Defaults to 'WARNING'.
        """
        # logging setup
        self.loglevel = loglevel
        self.logger = log_configure(log_level=self.loglevel, log_name='PlotSeaIce')

        self.model = model
        self.exp = exp
        self.source = source
        self.catalog = catalog

        self.regions_to_plot = self._check_list_regions_type(regions_to_plot)

        # define and check data types
        self.repacked_dict = self.repack_datasetlists(monthly_models=monthly_models, 
                                                      annual_models=annual_models, 
                                                      monthly_ref=monthly_ref, 
                                                      annual_ref=annual_ref, 
                                                      monthly_std_ref=monthly_std_ref, 
                                                      annual_std_ref=annual_std_ref)
        # Output & saving settings
        self.outdir  = outdir
        self.rebuild = rebuild
        self.dpi = dpi

    def _check_list_regions_type(self, regions_to_plot):
        """Ensures regions_to_plot is a list of strings before assigning it."""
        if regions_to_plot is None:
            self.logger.warning("Expected regions_to_plot to be a list, but got None. Plotting all available regions in data.")
            return None

        if not isinstance(regions_to_plot, list):
            self.logger.error(f"Expected regions_to_plot to be a list, but got {type(regions_to_plot).__name__}.")
            raise TypeError(  f"Expected regions_to_plot to be a list, but got {type(regions_to_plot).__name__}.")
        
        if not all(isinstance(region, str) for region in regions_to_plot):
            invalid_types = [type(region).__name__ for region in regions_to_plot]
            self.logger.error(f"Expected a list of strings, but found element types: {invalid_types}.")
            raise TypeError(  f"Expected a list of strings, but found element types: {invalid_types}.")
        return regions_to_plot
    
    def _check_as_datasets_list(self, datain) -> list[xr.Dataset] | None :
        """ Check that the input (`datain`) is either:
            - A single `xarray.Dataset` (which is converted into a list).
            - A list of `xarray.Dataset` objects.
            - `None` (which is returned as is).
        Args:
            datain (xr.Dataset | list[xr.Dataset] | None): The input dataset(s) to check.
        Returns:
            list[xr.Dataset] | None: None if datain is None
        """
        if isinstance(datain, xr.Dataset):
            # if a single Dataset is passed, wrap it in a list
            return [datain]
        elif datain is None or (isinstance(datain, list) and all(isinstance(ds, xr.Dataset) for ds in datain)):
            return datain
        else:
            self.logger.error(f"Invalid data type: {type(datain)}. Expected xr.Dataset, list of xr.Dataset, or None.")
            raise ValueError( f"Invalid data type: {type(datain)}. Expected xr.Dataset, list of xr.Dataset, or None.")

    def _get_region_name_in_datarray(self, da: xr.DataArray) -> str:
        """Get the region variable from the dataset or derive it from the variable name."""
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
        """Repack input datasets into a nested dictionary organized by method and region.
        The output dictionary is structured as:
            { method: { region: { str_data: [list of data arrays] }}}
        where:
        - 'method' is extracted from the dataset attributes (defaulting to "Unknown")
        - 'region' is determined by self._get_region(dataset, data_var)
        - 'str_data' is the keyword with the data in input, and each value is a list of 
           data arrays corresponding to that keyword
        Args:
            **kwargs: A dictionary of keyword arguments, where each str_data is
                      linked to the kwargs in plot_timeseries() and each value is a list of xr.Dataset objects.
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
                method = dataset.attrs.get("AQUA_method", "Unknown")

                # process each data variable in the dataset
                for var_name, data_array in dataset.data_vars.items():
                    data_array.name = data_array.name or var_name
                    
                    # validate the region for the current data variable
                    region = self._get_region_name_in_datarray(data_array)

                    if self.regions_to_plot and (region not in self.regions_to_plot):
                        # if region is not in regions_to_plot, record as None
                        repacked_defdict[method][region][str_data] = None
                    else:
                        # in the nested defaultdicts an empty list is configured by default, thus directly append
                        repacked_defdict[method][region][str_data].append(data_array)

        # convert the nested defaultdicts to plain dictionaries recursively
        repacked_dict = defaultdict_to_dict(repacked_defdict)

        return repacked_dict
    
    def _gen_str_from_attributes(self, datain: xr.DataArray | None) -> str:
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
        if isinstance(values, list) and all(isinstance(da, xr.DataArray) for da in values):
            return values if len(values) > 1 else values[0]
        return None
    
    def _update_description(self, method, region, data_dict, region_idx):
        """ Create the caption description from attributes
        Returns:
            the updated string 
        """
        # initialise string if _description doesn't exist
        if not hasattr(self, '_description'):
            self._description = ''
        
        # --- generate dynamic string for regions
        if region not in self._description:
            if not hasattr(self, 'region_str'):
                self.region_str = region  # start with first region
            else:
                if region_idx == self.num_regions - 1:
                    self.region_str += f" and {region} regions"
                else:
                    self.region_str += f", {region}"
        
        # --- generate dynamic string for model data
        if self.data_labels:
            # remove duplicates while keeping order
            unique_labels = list(dict.fromkeys(self.data_labels))

            # extract model data from current dictionary
            model_data_dict = self._getdata_fromdict(data_dict, 'monthly_models')

            # extract startdate and enddate for each unique model
            stdate = model_data_dict.attrs.get('AQUA_startdate', 'Unknown startdate')
            endate = model_data_dict.attrs.get('AQUA_enddate', 'Unknown enddate')
            model_startdate_list = [f"{label} from {stdate} to {endate}" for label in unique_labels]

            # build the model data string
            self.model_labels_str = (f"{', '.join(model_startdate_list)} {'are' if len(model_startdate_list) > 1 else 'is'} "
                                     f"used as {'models' if len(model_startdate_list) > 1 else 'model'} data.")
        else:
            self.model_labels_str = ''

        # --- generate dynamic string for reference data
        if self.ref_label:
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

        # --- generate string for reference std data
        if self.std_label:
            sdtdata = self._getdata_fromdict(data_dict,'monthly_std_ref')
            std_sdate = sdtdata.attrs.get("AQUA_startdate", "No startdate found")
            std_edate = sdtdata.attrs.get("AQUA_enddate",   "No enddate found")
            self.std_label_str = f" Reference data std is evaluated from {std_sdate} to {std_edate}."
        else:
            self.std_label_str = ''
                
        # finally build the string caption (dynamically)
        self._description = ('Time series of the Sea ice {} integrated over {}. {}{}{}').format(method, self.region_str, 
                                                                                                self.model_labels_str,
                                                                                                self.ref_label_str, self.std_label_str)

    def plot_seaice_timeseries(self, save_pdf=True, save_png=True, **kwargs):
        """ Plot data by iterating over dict and calling plot_timeseries"""
        # iterate over the methods in the dictionary
        for method, region_dict in self.repacked_dict.items():
            self.logger.info(f"Processing method: {method}")

            self.num_regions = len(region_dict)

            # create a figure and an array of axes for each region
            fig, axes = plt.subplots(nrows=self.num_regions, ncols=1, figsize=(10, 4 * self.num_regions), squeeze=False)
            # flatten the axes array for easier iteration when there's only one column
            axes = axes.flatten()
            
            for region_idx, (ax, (region, data_dict)) in enumerate(zip(axes, region_dict.items())):
                self.logger.info(f"Processing region: {region}")

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
                
                fig, ax = plot_timeseries(monthly_data=monthly_models,
                                          annual_data=annual_models,
                                          ref_monthly_data=monthly_ref,
                                          ref_annual_data=annual_ref,
                                          std_monthly_data=monthly_std,
                                          std_annual_data=annual_std,
                                          data_labels=self.data_labels,
                                          ref_label=self.ref_label,
                                          std_label=None,  # don't plot std in legend in any case
                                          fig=fig,
                                          ax=ax,
                                          **kwargs)

                # after plotting, append text about what we did:
                self._update_description(method, region, data_dict, region_idx)

                # optionally, customize the subplot (e.g., add a title)
                ax.set_title(f"Sea ice {method}: region {region}")
            
            plt.tight_layout()

            # save figure            
            if save_png or save_pdf:

                # store description
                metadata = {"Description": self._description}

                # create outputsaver object
                output_saver = OutputSaver(diagnostic='PlotSeaIce', catalog=self.catalog, model=self.model, exp=self.exp,
                                           diagnostic_product=f"seaice_{method}_{'_'.join(region_dict.keys())}",
                                           loglevel=self.loglevel, default_path=self.outdir, rebuild=self.rebuild)

            if save_pdf: output_saver.save_pdf(fig=fig, path=self.outdir, metadata=metadata)
            if save_png: output_saver.save_png(fig=fig, path=self.outdir, metadata=metadata)
