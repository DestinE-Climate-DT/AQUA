from aqua.logger import log_configure, log_history
from aqua.util import create_folder, add_pdf_metadata, add_png_metadata, update_metadata, ConfigPath
import os
import xarray as xr
from datetime import datetime
from dateutil.parser import parse
from matplotlib.figure import Figure
import matplotlib.pyplot as plt


class OutputSaver:
    """
    Class to manage saving outputs, including NetCDF, PDF, and PNG files, with
    customized naming based on provided parameters and metadata.
    """

    def __init__(self, diagnostic: str, catalog: str = None, model: str = None, exp: str = None, diagnostic_product: str = None, loglevel: str = 'WARNING',
                 default_path: str = '.', rebuild: bool = True, filename_keys: list = None):
        """
        Initialize the OutputSaver class to manage output file saving.

        Args:
            diagnostic (str): Name of the diagnostic.
            catalog (str, optional): Catalog where the model is stored. Default is None.
            model (str, optional): Model used in the diagnostic. Default is None.
            exp (str, optional): Experiment identifier. Default is None.
            diagnostic_product (str, optional): Product of the diagnostic analysis.
            loglevel (str, optional): Log level for the class's logger.
            default_path (str, optional): Default path where files will be saved.
            rebuild (bool, optional): If True, overwrite the existing files. If False, do not overwrite. Default is True.
            filename_keys (list, optional): List of keys to keep in the filename. Default is None, which includes all keys.
        """
        self.diagnostic = diagnostic
        self.catalog = catalog if catalog is not None else ConfigPath().catalog
        self.model = model
        self.exp = exp
        self.diagnostic_product = diagnostic_product
        self.loglevel = loglevel
        self.default_path = default_path
        self.rebuild = rebuild
        self.all_keys = [
            'diagnostic', 'diagnostic_product', 'catalog', 'model', 'exp',
            'var', 'model_2', 'exp_2', 'catalog_2', 'area', 'time_start', 'time_end', 'time_precision']
        if filename_keys is not None:
            # Validate that filename_keys are part of the allowed keys
            for key in filename_keys:
                if key not in self.all_keys:
                    raise ValueError(f"Invalid key '{key}' in filename_keys. Allowed keys are: {self.all_keys}")
            self.filename_keys = filename_keys
        else:
            self.filename_keys = self.all_keys
        self.logger = log_configure(log_level=self.loglevel, log_name='OutputSaver')
        
    def update_diagnostic_product(self, diagnostic_product: str):
        """
        Update the diagnostic product for the instance.

        Args:
            diagnostic_product (str): The new diagnostic product to be used.
        """
        if diagnostic_product is not None:
            self.diagnostic_product = diagnostic_product

    def generate_name(self, diagnostic_product: str = None, var: str = None, model_2: str = None, exp_2: str = None,
                      time_start: str = None, time_end: str = None, time_precision: str = 'ymd', area: str = None,
                      suffix: str = 'nc', catalog_2: str = None, **kwargs) -> str:
        """
        Generate a filename based on provided parameters and additional user-defined keywords,
        including precise time intervals.

        Args:
            diagnostic_product (str, optional): Product of the diagnostic analysis.
            var (str, optional): Variable of interest.
            model_2 (str, optional): The second model, for comparative studies.
            exp_2 (str, optional): The experiment associated with the second model.
            time_start (str, optional): The start time for the data, in format consistent with time_precision.
            time_end (str, optional): The finish (end) time for the data, in format consistent with time_precision.
            time_precision (str, optional): Precision for time representation ('y', 'ym', 'ymd', 'ymdh', etc.).
            area (str, optional): The geographical area covered by the data.
            suffix (str, optional): The file extension/suffix indicating file type.
            catalog_2 (str, optional): The second catalog, for comparative studies. Default to None will allow for autosearch in the installed catalogs.
            **kwargs: Arbitrary keyword arguments provided by the user for additional customization.

        Returns:
            str: A string representing the generated filename.

        Raises:
            ValueError: If diagnostic_product is not provided or if time format is invalid.
        """
        self.update_diagnostic_product(diagnostic_product)

        # Skip setting diagnostic_product if not required in filename_keys
        if 'diagnostic_product' in self.filename_keys and not self.diagnostic_product:
            raise ValueError("The 'diagnostic_product' parameter is required and cannot be empty.")

        # Handle time formatting based on the specified precision
        time_format = {
            'y': '%Y',
            'ym': '%Y%m',
            'ymd': '%Y%m%d',
            'ymdh': '%Y%m%d%H'
        }

        time_parts = []
        if time_start and time_end:
            try:
                start_time = parse(time_start)
                end_time = parse(time_end)
                if time_precision in time_format:
                    time_parts = [start_time.strftime(time_format[time_precision]),
                                  end_time.strftime(time_format[time_precision])]
                else:
                    raise ValueError(f"Invalid time_precision: {time_precision}")
            except (ValueError, TypeError) as e:
                raise ValueError(f"Invalid date format: {e}")

        parts_dict = {
            'diagnostic': self.diagnostic,
            'diagnostic_product': self.diagnostic_product,
            'catalog': self.catalog,
            'model': self.model,
            'exp': self.exp,
            'var': var,
            'catalog_2': catalog_2,
            'model_2': model_2,
            'exp_2': exp_2,
            'area': area,
            'time_start': time_parts[0] if time_parts else None,
            'time_end': time_parts[1] if time_parts else None,
            'time_precision': time_precision if time_parts else None
        }

        # If filename_keys are specified, only use those keys. Otherwise, use all available keys and kwargs
        if self.filename_keys == self.all_keys:
            additional_parts = [f"{key}_{value}" for key, value in sorted(kwargs.items()) if key not in parts_dict]
        else:
            additional_parts = [f"{key}_{value}" for key, value in sorted(kwargs.items()) if key in self.filename_keys and key not in parts_dict]
        
        # Ensure catalog_2 always comes before model_2 if both are provided
        ordered_keys = []
        for key in self.filename_keys:
            if key == 'model_2' and 'catalog_2' in self.filename_keys and 'catalog_2' not in ordered_keys:
                ordered_keys.append('catalog_2')
            if key not in ordered_keys:
                ordered_keys.append(key)
        
        # Filter parts based on ordered_keys, ensuring to follow the specified order
        parts = [parts_dict[key] for key in ordered_keys if key in parts_dict and parts_dict[key] is not None]
        
        # Append additional parts and suffix
        parts.extend(additional_parts)
        parts.append(suffix)

        filename = '.'.join(parts)
        self.logger.debug(f"Generated filename: {filename}")
        return filename

    def save_netcdf(self, dataset: xr.Dataset, path: str = None, diagnostic_product: str = None, var: str = None,
                    model_2: str = None, exp_2: str = None, time_start: str = None, time_end: str = None,
                    time_precision: str = 'ymd', area: str = None, metadata: dict = None, catalog_2: str = None, **kwargs) -> str:
        """
        Save a netCDF file with a dataset to a specified path, with support for additional filename keywords and
        precise time intervals.

        Args:
            dataset (xr.Dataset): The xarray dataset to be saved as a netCDF file.
            path (str, optional): The absolute path where the netCDF file will be saved.
            diagnostic_product (str, optional): Product of the diagnostic analysis.
            var (str, optional): Variable of interest.
            model_2 (str, optional): The second model, for comparative studies.
            exp_2 (str, optional): The experiment associated with the second model.
            time_start (str, optional): The start time for the data, in format consistent with time_precision.
            time_end (str, optional): The finish (end) time for the data, in format consistent with time_precision.
            time_precision (str, optional): Precision for time representation ('y', 'ym', 'ymd', 'ymdh', etc.).
            area (str, optional): The geographical area covered by the data.
            metadata (dict, optional): Additional metadata to include in the netCDF file.
            catalog_2 (str, optional): The second catalog, for comparative studies. Default to None will allow for autosearch in the installed catalogs.
            **kwargs: Additional keyword arguments for more flexible filename customization.

        Returns:
            str: The absolute path where the netCDF file has been saved.
        """
        filename = self.generate_name(diagnostic_product=diagnostic_product, var=var,
                                      model_2=model_2, exp_2=exp_2, time_start=time_start, time_end=time_end,
                                      time_precision=time_precision, area=area, suffix='nc', catalog_2=catalog_2, **kwargs)

        if path is None:
            path = os.path.join(self.default_path, 'netcdf')
        create_folder(folder=str(path), loglevel=self.loglevel)
        full_path = os.path.join(path, filename)

        if not self.rebuild and os.path.exists(full_path):
            self.logger.info(f"File already exists and rebuild is set to False: {full_path}")
            return full_path

        # Add metadata if provided, including the current time and additional fields
        additional_metadata = {
            'diagnostic': self.diagnostic,
            'model': self.model,
            'experiment': self.exp,
            'diagnostic_product': diagnostic_product,
            'var': var,
            'model_2': model_2,
            'exp_2': exp_2,
            'time_start': time_start,
            'time_end': time_end,
            'time_precision': time_precision,
            'area': area,
            'catalog': self.catalog,
            'catalog_2': catalog_2,
            'rebuild': str(self.rebuild)  # Convert rebuild to a string to make it compatible with NetCDF
        }
        # Include kwargs in additional_metadata
        additional_metadata.update(kwargs)

        # Filter out None values from additional_metadata
        filtered_metadata = {key: value for key, value in additional_metadata.items() if value is not None}

        metadata = update_metadata(metadata, filtered_metadata)

        # If metadata contains a history attribute, log the history
        if 'history' in metadata:
            log_history(data=dataset, msg=metadata['history'])
            # Remove the history attribute from the metadata dictionary
            metadata.pop('history')

        dataset.attrs.update(metadata)
        self.logger.debug(f"Metadata added: {metadata}")

        # Save the dataset to the specified path
        dataset.to_netcdf(full_path, mode='w')

        self.logger.info(f"Saved netCDF file to path: {full_path}")
        return full_path

    def save_pdf(self, fig: Figure, path: str = None, diagnostic_product: str = None, var: str = None,
                 model_2: str = None, exp_2: str = None, time_start: str = None, time_end: str = None,
                 time_precision: str = 'ymd', area: str = None, metadata: dict = None, dpi: int = 300, catalog_2: str = None, **kwargs) -> str:
        """
        Save a PDF file with a matplotlib figure to the provided path, with support for additional filename keywords and
        precise time intervals.

        Args:
            fig (Figure): The matplotlib figure object to be saved as a PDF.
            path (str, optional): The path where the PDF file will be saved.
            diagnostic_product (str, optional): Product of the diagnostic analysis.
            var (str, optional): Variable of interest.
            model_2 (str, optional): The second model, for comparative studies.
            exp_2 (str, optional): The experiment associated with the second model.
            time_start (str, optional): The start time for the data, in format consistent with time_precision.
            time_end (str, optional): The finish (end) time for the data, in format consistent with time_precision.
            time_precision (str, optional): Precision for time representation ('y', 'ym', 'ymd', 'ymdh', etc.).
            area (str, optional): The geographical area covered by the data.
            metadata (dict, optional): Additional metadata to include in the PDF file.
            dpi (int, optional): The resolution of the saved PDF file. Default is 300.
            catalog_2 (str, optional): The second catalog, for comparative studies. Default to None will allow for autosearch in the installed catalogs.
            **kwargs: Additional keyword arguments for more flexible filename customization.

        Returns:
            str: The full path to where the PDF file was saved.

        Raises:
            ValueError: If the provided fig parameter is not a valid matplotlib Figure.
        """
        if path is None:
            path = os.path.join(self.default_path, 'pdf')
        create_folder(folder=str(path), loglevel=self.loglevel)

        filename = self.generate_name(diagnostic_product=diagnostic_product, var=var, model_2=model_2, exp_2=exp_2,
                                      time_start=time_start, time_end=time_end, time_precision=time_precision, area=area,
                                      suffix='pdf', catalog_2=catalog_2, **kwargs)
        full_path = os.path.join(path, filename)

        if not self.rebuild and os.path.exists(full_path):
            self.logger.info(f"File already exists and rebuild is set to False: {full_path}")
            return full_path

        # Ensure fig is a Figure object
        if isinstance(fig, plt.Axes):
            fig = fig.figure
        # Save the figure as a PDF
        if isinstance(fig, (plt.Figure, Figure)):
            fig.savefig(full_path, dpi=dpi)
        else:
            raise ValueError("The provided fig parameter is not a valid matplotlib Figure or pyplot figure.")

        # Add metadata if provided, including the current time and additional fields
        additional_metadata = {
            'diagnostic': self.diagnostic,
            'model': self.model,
            'experiment': self.exp,
            'diagnostic_product': diagnostic_product,
            'var': var,
            'model_2': model_2,
            'exp_2': exp_2,
            'time_start': time_start,
            'time_end': time_end,
            'time_precision': time_precision,
            'area': area,
            'catalog': self.catalog,
            'catalog_2': catalog_2,
            'rebuild': str(self.rebuild)  # Convert rebuild to a string to make it compatible with NetCDF
        }
        # Include kwargs in additional_metadata
        additional_metadata.update(kwargs)

        # Filter out None values from additional_metadata
        filtered_metadata = {key: value for key, value in additional_metadata.items() if value is not None}

        metadata = update_metadata(metadata, filtered_metadata)

        add_pdf_metadata(full_path, metadata, loglevel=self.loglevel)

        self.logger.info(f"Saved PDF file at: {full_path}")
        return full_path

    def save_png(self, fig: Figure, path: str = None, diagnostic_product: str = None, var: str = None,
                 model_2: str = None, exp_2: str = None, time_start: str = None, time_end: str = None,
                 time_precision: str = 'ymd', area: str = None, metadata: dict = None, dpi: int = 300, catalog_2: str = None, **kwargs) -> str:
        """
        Save a PNG file with a matplotlib figure to a provided path, with support for additional filename keywords and
        precise time intervals.

        Args:
            fig (Figure): The matplotlib figure object to be saved as a PNG.
            path (str, optional): The path where the PNG file will be saved.
            diagnostic_product (str, optional): Product of the diagnostic analysis.
            var (str, optional): Variable of interest.
            model_2 (str, optional): The second model, for comparative studies.
            exp_2 (str, optional): The experiment associated with the second model.
            time_start (str, optional): The start time for the data, in format consistent with time_precision.
            time_end (str, optional): The finish (end) time for the data, in format consistent with time_precision.
            time_precision (str, optional): Precision for time representation ('y', 'ym', 'ymd', 'ymdh', etc.).
            area (str, optional): The geographical area covered by the data.
            metadata (dict, optional): Additional metadata to include in the PNG file.
            dpi (int, optional): The resolution of the saved PNG file. Default is 300.
            catalog_2 (str, optional): The second catalog, for comparative studies. Default to None will allow for autosearch in the installed catalogs.
            **kwargs: Additional keyword arguments for more flexible filename customization.

        Returns:
            str: The full path to where the PNG file has been saved.

        Raises:
            ValueError: If the provided fig parameter is not a valid matplotlib Figure.
        """
        filename = self.generate_name(diagnostic_product=diagnostic_product, var=var, model_2=model_2, exp_2=exp_2,
                                      time_start=time_start, time_end=time_end, time_precision=time_precision,
                                      area=area, suffix='png', catalog_2=catalog_2, **kwargs)

        if path is None:
            path = os.path.join(self.default_path, 'png')
        create_folder(folder=str(path), loglevel=self.loglevel)

        full_path = os.path.join(path, filename)

        if not self.rebuild and os.path.exists(full_path):
            self.logger.info(f"File already exists and rebuild is set to False: {full_path}")
            return full_path

        # Ensure fig is a Figure object
        if isinstance(fig, plt.Axes):
            fig = fig.figure
        # Save the figure to the specified path
        if isinstance(fig, (plt.Figure, Figure)):
            fig.savefig(full_path, format='png', dpi=dpi)
        else:
            raise ValueError("The provided fig parameter is not a valid matplotlib Figure or pyplot figure.")

        # Add metadata if provided, including the current time and additional fields
        additional_metadata = {
            'diagnostic': self.diagnostic,
            'model': self.model,
            'experiment': self.exp,
            'diagnostic_product': diagnostic_product,
            'var': var,
            'model_2': model_2,
            'exp_2': exp_2,
            'time_start': time_start,
            'time_end': time_end,
            'time_precision': time_precision,
            'area': area,
            'catalog': self.catalog,
            'catalog_2': catalog_2,
            'rebuild': str(self.rebuild)  # Convert rebuild to a string to make it compatible with NetCDF
        }
        # Include kwargs in additional_metadata
        additional_metadata.update(kwargs)

        # Filter out None values from additional_metadata
        filtered_metadata = {key: value for key, value in additional_metadata.items() if value is not None}

        metadata = update_metadata(metadata, filtered_metadata)

        add_png_metadata(full_path, metadata, loglevel=self.loglevel)

        self.logger.info(f"Saved PNG file to path: {full_path}")
        return full_path
