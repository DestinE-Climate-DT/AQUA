"""
OutputSaver class for saving diagnostic outputs 
in various formats (netcdf, pdf, png) for basic
AQUA diagnostics.
"""

import os
from typing import Optional, Union

import xarray as xr
from matplotlib.figure import Figure

from aqua.lock import SafeFileLock
from aqua.logger import log_configure, log_history
from aqua.util import create_folder, add_pdf_metadata, add_png_metadata, update_metadata
from aqua.util import dump_yaml, load_yaml
from aqua.util import replace_intake_vars, replace_urlpath_jinja, replace_urlpath_wildcard
from aqua.configurer import ConfigPath
from aqua.util import format_realization
from aqua.util.string import clean_filename

class OutputSaver:
    """
    Class to manage saving outputs, including NetCDF, PDF, and PNG files, with
    customized naming based on provided parameters and metadata.
    """

    def __init__(self, diagnostic: str,
                 catalog: Optional[Union[str, list]] = None, model: Optional[Union[str, list]] = None, 
                 exp: Optional[Union[str, list]] = None, realization: Optional[Union[str, list]] = None,
                 catalog_ref: Optional[Union[str, list]] = None, model_ref: Optional[Union[str, list]] = None, 
                 exp_ref: Optional[Union[str, list]] = None,
                 outputdir: str = '.', loglevel: str = 'WARNING'):
        """
        Initialize the OutputSaver with diagnostic parameters and output directory.
        All the catalog, model, and experiment can be both a string or a list of strings.

        Args:
            diagnostic (str): Name of the diagnostic.
            catalog (str, list, optional): Catalog name.
            model (str, list, optional): Model name.
            exp (str, list, optional): Experiment name.
            realization (str, list, optional): Realization name, can be a string or a integer.
                                         'r' is appended if it is an integer.
            catalog_ref (str, list, optional): Reference catalog name.
            model_ref (str, list, optional): Reference model name.
            exp_ref (str, list, optional): Reference experiment name.
            outputdir (str, optional): Output directory. Defaults to current directory.
            loglevel (str, optional): Logging level. Defaults to 'WARNING'.
        """
        self.loglevel = loglevel
        self.logger = log_configure(log_level=self.loglevel, log_name='OutputSaver')

        self.diagnostic = diagnostic

        # Unpack single element list so that we can handle both single strings and lists
        self.catalog = self.unpack_list(catalog)
        self.model = self.unpack_list(model)
        self.exp = self.unpack_list(exp)
        self.catalog_ref = self.unpack_list(catalog_ref)
        self.model_ref = self.unpack_list(model_ref)
        self.exp_ref = self.unpack_list(exp_ref)

        # Format realization to ensure it is a string or list of strings
        self.realization = format_realization(realization)

        # Verify that catalog, model, and exp are either all strings or all lists of the same length
        self._verify_arguments(['catalog', 'model', 'exp'])
        self._verify_arguments(['catalog_ref', 'model_ref', 'exp_ref'])

        self.logger.debug('Complete initialization with parameters: %s', {
            'diagnostic': self.diagnostic,
            'catalog': self.catalog,
            'model': self.model,
            'exp': self.exp,
            'realization': self.realization,
            'catalog_ref': self.catalog_ref,
            'model_ref': self.model_ref,
            'exp_ref': self.exp_ref
        })

        self.outputdir = outputdir

    @staticmethod
    def unpack_list(value: Optional[Union[str, list]]) -> Optional[Union[str, list]]:
        """
        Unpack a value that can be a string, list, or None.

        Args:
            value: The value to unpack. Can be string, list, or None.

        Returns:
            - If value is a single-item list and special is None: returns the single item
            - Otherwise: returns value as-is

        """
        if isinstance(value, list):
            if len(value) == 1:
                return value[0]
            if len(value) == 0:
                return None
        return value

    def _verify_arguments(self, attr_names):
        """
        Verify that the given attributes on obj are lists of the same length.

        Args:
            obj: The object to inspect.
            attr_names (list of str): Names of attributes to verify.

        Raises:
            ValueError if attributes are not all lists or lengths differ.
        """
        values = [getattr(self, name, None) for name in attr_names]

        # all strings, no problem
        if all(isinstance(value, (str, type(None))) for value in values):
            return True

        # all list, verify lengths
        if all(isinstance(value, (list, type(None))) for value in values):
            list_values = [v for v in values if isinstance(v, list)]
            first_len = len(list_values[0])

            if all(len(v) == first_len for v in list_values):
                return True
            raise ValueError(f"Attributes {attr_names} are lists of different lengths.")

        # mixed case, does not work
        self.logger.debug("Attributes values: %s", values)
        raise ValueError(f"Attributes {attr_names} must be either all strings or all lists of the same length.")

    def generate_name(self, diagnostic_product: str, extra_keys: Optional[dict] = None) -> str:
        """
        Generate a filename based on provided parameters and additional user-defined keywords

        Args:
            diagnostic_product (str, optional): Product of the diagnostic analysis.
            extra_keys (dict, optional): Dictionary of additional keys to include in the filename.

        Returns:
            str: A string representing the generated filename.
        """

        if not self.catalog or not self.model or not self.exp:
            raise ValueError("Catalog, model, and exp must be specified to generate a filename.")

        # handle multimodel/multiref case
        model_value = 'multimodel' if isinstance(self.model, list) and len(self.model) > 1 else self.model
        model_ref_value = 'multiref' if isinstance(self.model_ref, list) and len(self.model_ref) > 1 else self.model_ref

        # build dictionary
        parts_dict = {
            'diagnostic': self.diagnostic,
            'diagnostic_product': diagnostic_product,
            'catalog': self.catalog if model_value != "multimodel" else None,
            'model': model_value,
            'exp': self.exp if model_value != "multimodel" else None,
            'realization': self.realization if model_value != "multimodel" else None,
            'catalog_ref': self.catalog_ref if model_ref_value != "multiref" else None,
            'model_ref': model_ref_value,
            'exp_ref': self.exp_ref if model_ref_value != "multiref" else None,
        }

        # Add additional filename keys if provided
        if extra_keys:
            parts_dict.update(extra_keys)
 
        # Remove None values and check selected parts
        parts = [clean_filename(str(value)) if key not in 
                 ['catalog', 'model', 'exp', 'catalog_ref', 'model_ref', 'exp_ref'] 
                 else value for key, value in parts_dict.items() if value is not None]

        # Join all parts
        filename = '.'.join(parts)

        self.logger.debug("Generated filename: %s", filename)
        return filename

    def _core_save(self, diagnostic_product: str, file_format: str,
                   extra_keys: Optional[dict] = None):
        """
        Core method to handle the common logic for saving files, including checking if the file exists.
        """

        if file_format not in ['pdf', 'png', 'nc']:
            raise ValueError("file_format must be either 'pdf',  'png' or 'nc'")

        filename = self.generate_name(
            diagnostic_product=diagnostic_product, extra_keys=extra_keys
        ) + f'.{file_format}'
        dir_format = 'netcdf' if file_format == 'nc' else file_format
        folder = os.path.join(self.outputdir, dir_format)
        create_folder(folder=str(folder), loglevel=self.loglevel)
        return os.path.join(folder, filename)

    def save_netcdf(self, dataset: xr.Dataset, diagnostic_product: str,
                    rebuild: bool = True, extra_keys: Optional[dict] = None,
                    metadata: Optional[dict] = None, create_catalog_entry: bool = False,
                    dict_catalog_entry: Optional[dict] = None):
        """
        Save an xarray Dataset as a NetCDF file with a generated filename.

        Args:
            dataset (xr.Dataset): The xarray Dataset to save.
            diagnostic_product (str): Product of the diagnostic analysis.
            rebuild (bool, optional): Whether to rebuild the output file if it already exists. Defaults to True.
            extra_keys (dict, optional): Dictionary of additional keys to include in the filename.
            metadata (dict, optional): Additional metadata to include in the NetCDF file.
            create_catalog_entry (bool, optional): Whether to create a catalog entry for the NetCDF file. Defaults to False.
            dict_catalog_entry (dict, optional): List of jinja and wildcard variables. Default is none.
        """

        filepath = self._core_save(
            diagnostic_product=diagnostic_product,
            file_format='nc', extra_keys=extra_keys)

        if not rebuild and os.path.exists(filepath):
            self.logger.info("File already exists and rebuild=False, skipping: %s", filepath)
            return filepath

        metadata = self.create_metadata(
            diagnostic_product=diagnostic_product,
            extra_keys=extra_keys, metadata=metadata)

        # If metadata contains a history attribute, log the history
        if 'history' in metadata:
            log_history(data=dataset, msg=metadata['history'])
            # Remove the history attribute from the metadata dictionary
            metadata.pop('history')

        # define a default list of jinja and wildcard variables if not provided
        if not dict_catalog_entry:
            dict_catalog_entry = {
                'jinjalist': ['freq', 'stat', 'region', 'realization'],
                'wildcardlist': ['var']
            }

        dataset.attrs.update(metadata)
        dataset.to_netcdf(filepath)

        # create catalog entry for netcdf file
        if create_catalog_entry:
            self._create_catalog_entry(
                metadata=metadata, filepath=filepath,
                jinjalist=dict_catalog_entry.get('jinjalist', None),
                wildcardlist=dict_catalog_entry.get('wildcardlist', None)
            )

        self.logger.info("Saved NetCDF: %s", filepath)
        return filepath
    
    def generate_folder(self, extension: str = 'pdf'):
        """
        Generate a folder for saving output files based on the specified format.

        Args:
            extension (str): The extension of the output files (e.g., 'pdf', 'png', 'netcdf').
        
        Returns:
            str: The path to the generated folder.
        """
        folder = os.path.join(self.outputdir, extension)
        create_folder(folder=str(folder), loglevel=self.loglevel)
        return folder
    
    def generate_path(self, extension: str, diagnostic_product: str,
                      extra_keys: dict = None) -> str:
        """
        Generate a full file path for saving output files based on the provided parameters.
        Simplified wrapper around `generate_name` and `generate_folder` to include the output directory.
        """
        filename = self.generate_name(diagnostic_product=diagnostic_product,
                                       extra_keys=extra_keys)
        folder = self.generate_folder(extension=extension)
        return os.path.join(folder, filename + '.' + extension)

    def _save_figure_format(self, fig: Figure, diagnostic_product: str, file_format: str,
                            rebuild: bool = True, extra_keys: Optional[dict] = None, metadata: Optional[dict] = None,
                            dpi: Optional[int] = None):
        """
        Internal method to save a Matplotlib figure in a single format with common logic for PDF and PNG.

        Args:
            fig (plt.Figure): The Matplotlib figure to save.
            diagnostic_product (str): Product of the diagnostic analysis.
            file_format (str): 'pdf' or 'png'.
            rebuild (bool): Whether to overwrite existing files.
            extra_keys (dict): Extra keys for filename generation.
            metadata (dict): Metadata to embed.
            dpi (int): DPI setting for raster formats like PNG.
        """

        filepath = self._core_save(
            diagnostic_product=diagnostic_product,
            file_format=file_format, extra_keys=extra_keys)

        if not rebuild and os.path.exists(filepath):
            self.logger.info("File already exists and rebuild=False, skipping: %s", filepath)
            return filepath

        save_kwargs = {'format': file_format, 'bbox_inches': 'tight'}
        if file_format == 'png' and dpi is not None:
            save_kwargs['dpi'] = dpi

        fig.savefig(filepath, **save_kwargs)

        metadata = self.create_metadata(
            diagnostic_product=diagnostic_product,
            extra_keys=extra_keys, metadata=metadata)

        if file_format == 'pdf':
            add_pdf_metadata(filepath, metadata, loglevel=self.loglevel)
        elif file_format == 'png':
            add_png_metadata(filepath, metadata, loglevel=self.loglevel)

        self.logger.info("Saved %s: %s", file_format.upper(), filepath)
        return filepath

    def save_pdf(self, fig: Figure, diagnostic_product: str, rebuild: bool = True,
                 extra_keys: Optional[dict] = None, metadata: Optional[dict] = None):
        """
        Save a Matplotlib figure as a PDF.
        """
        return self._save_figure_format(fig, diagnostic_product, 'pdf', rebuild, extra_keys, metadata)

    def save_png(self, fig: Figure, diagnostic_product: str, rebuild: bool = True,
                 extra_keys: Optional[dict] = None, metadata: Optional[dict] = None, dpi: int = 300):
        """
        Save a Matplotlib figure as a PNG.
        """
        return self._save_figure_format(fig, diagnostic_product, 'png', rebuild, extra_keys, metadata, dpi)

    def save_figure(self, fig: Figure, diagnostic_product: str,
                    extra_keys: Optional[dict] = None,
                    metadata: Optional[dict] = None,
                    save_pdf: bool = False,
                    save_png: bool = True,
                    rebuild: bool = True,
                    dpi: int = 300):
        """
        Save a matplotlib figure in the specified format(s).
        
        This method handles the format selection logic and delegates to
        save_pdf() and/or save_png() as needed.
        
        Args:
            fig: Matplotlib figure to save.
            diagnostic_product (str): Name of the diagnostic product.
            extra_keys (dict): Dictionary of additional keys for filename generation.
            metadata (dict): Dictionary of metadata to embed in the file.
            save_pdf (bool): Whether to save as PDF.
            save_png (bool): Whether to save as PNG.
            rebuild (bool): Whether to rebuild if file exists.
            dpi (int): Resolution for PNG output (ignored for PDF).
        """
        if save_pdf and save_png:
            format = 'both'
        elif save_pdf:
            format = 'pdf'
        elif save_png:
            format = 'png'
        else:
            raise ValueError("At least one of save_pdf or save_png must be True")
        
        if format not in ['png', 'pdf', 'both']:
            raise ValueError(f"format must be 'png', 'pdf', or 'both', got '{format}'")
        
        if format in ['pdf', 'both']:
            self.save_pdf(fig, diagnostic_product, rebuild=rebuild, extra_keys=extra_keys, metadata=metadata)
        
        if format in ['png', 'both']:
            self.save_png(fig, diagnostic_product, rebuild=rebuild,
                         extra_keys=extra_keys, metadata=metadata, dpi=dpi)

    def create_metadata(self, diagnostic_product: str, extra_keys: Optional[dict] = None, metadata: Optional[dict] = None) -> dict:
        """
        Create metadata dictionary for a plot or output file.

        Args:
            diagnostic_product (str): Product of the diagnostic analysis.
            extra_keys (dict, optional): Dictionary of additional keys to include in the filename.
            metadata (dict, optional): Additional metadata to include in the PNG file.
        """
        base_metadata = {
            'diagnostic': self.diagnostic,
            'diagnostic_product': diagnostic_product,
            'catalog': self.catalog,
            'model': self.model,
            'exp': self.exp,
            'realization': self.realization,
            'catalog_ref': self.catalog_ref,
            'model_ref': self.model_ref,
            'exp_ref': self.exp_ref
        }

        # Remove None values
        base_metadata = {k: v for k, v in base_metadata.items() if v is not None}

        # Process extra keys safely
        if extra_keys:
            processed_extra_keys = {
                key: ",".join(map(str, value)) if isinstance(value, list) else str(value)
                for key, value in extra_keys.items()
            }
            base_metadata.update(processed_extra_keys)

        # Merge with provided metadata, ensuring second argument is always a dict
        if metadata is None:
            metadata = {}
        metadata = update_metadata(base_metadata, metadata)
        self.logger.debug("Available metadata: %s", metadata)
        return metadata

    def _create_catalog_entry(self, filepath, metadata, jinjalist=None, wildcardlist=None):
        """
        Creates an entry in the catalog

        Args:
            filepath (str): The file path where the data is stored.
            metadata (dict): Metadata dictionary containing information about the diagnostic.
            jinjalist (list, optional): List of Jinja variables to replace in the URL path.
            wildcardlist (list, optional): List of wildcard variables to replace in the URL path.

        Returns:
            dict: The updated catalog entry block.
        """
        self.logger.info("Creating catalog entry for %s", filepath)
        configpath = ConfigPath(catalog=self.catalog)
        configdir = configpath.configdir
        # find the catalog of the experiment and load it
        catalogfile = os.path.join(configdir, 'catalogs', self.catalog, 'catalog', self.model, self.exp + '.yaml')

        # The following block must be locked because else two diagnostics may attempt to modify the same file at the same time

        self.logger.debug("Locking catalog file %s", catalogfile)
        with SafeFileLock(catalogfile + '.lock', loglevel=self.loglevel):
            cat_file = load_yaml(catalogfile)
            # Remove None values
            urlpath = replace_intake_vars(catalog=self.catalog, path=filepath)
            
            entry_name = f'aqua-{self.diagnostic}-{metadata.get("diagnostic_product")}'
            if entry_name in cat_file['sources']:
                catblock = cat_file['sources'][entry_name]
            else:
                catblock = None

            if catblock is None:
                # if the entry is not there, define the block to be uploaded into the catalog
                catblock = {
                    'driver': 'netcdf',
                    'description': f'AQUA diagnostic {self.diagnostic} data for product {metadata.get("diagnostic_product")}',
                    'args': {
                        'urlpath': urlpath,
                        'chunks': {},
                    },
                    'metadata': {
                        'source_grid_name': False,
                    }
                }
            else:
                # if the entry is there, we just update the urlpath
                catblock['args']['urlpath'] = urlpath

                catblock['args']['xarray_kwargs'] = {
                        'decode_times': True,
                }
            # These variables are replaced from the url as {{ variable }}
            if jinjalist:
                for key in jinjalist:
                    value = metadata.get(key)
                    if value is not None:
                        self.logger.debug("Replacing jinja variable %s with value %s in urlpath", key, value)
                        catblock = replace_urlpath_jinja(catblock, value, key)
            
            if wildcardlist:
               for key in wildcardlist:
                   value = metadata.get(key)
                   if value is not None:
                       self.logger.debug("Replacing wildcard variable %s with value %s in urlpath", key, value)
                       catblock = replace_urlpath_wildcard(catblock, value)

            self.logger.info('Final urlpath: %s', catblock['args']['urlpath'])
            
            cat_file['sources'][entry_name] = catblock

            # dump the update file
            dump_yaml(outfile=catalogfile, cfg=cat_file)

        self.logger.debug("Releasing catalog file %s", catalogfile)
        return catblock # using this in the tests
