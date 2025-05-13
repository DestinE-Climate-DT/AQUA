import matplotlib.pyplot as plt
import os
import xarray as xr
from aqua.logger import log_configure
from aqua.util import create_folder, add_pdf_metadata, add_png_metadata, update_metadata


class OutputSaver:
    """
    Class to manage saving outputs, including NetCDF, PDF, and PNG files, with
    customized naming based on provided parameters and metadata.
    """

    def __init__(self, diagnostic: str,
                 catalog: str = None, model: str = None, exp: str = None,
                 catalog_ref: str = None, model_ref: str = None, exp_ref: str = None,
                 outdir: str = '.', rebuild: bool = True, loglevel: str = 'WARNING'):
        """
        Initialize the OutputSaver with diagnostic parameters and output directory.
        All the catalog, model, and experiment can be both a string or a list of strings.

        Args:
            diagnostic (str): Name of the diagnostic.
            catalog (str, optional): Catalog name.
            model (str, optional): Model name.
            exp (str, optional): Experiment name.
            catalog_ref (str, optional): Reference catalog name.
            model_ref (str, optional): Reference model name.
            exp_ref (str, optional): Reference experiment name.
            outdir (str, optional): Output directory. Defaults to current directory.
            rebuild (bool, optional): Whether to rebuild the output directory. Defaults to True.
            loglevel (str, optional): Logging level. Defaults to 'WARNING'.
        """

        self.diagnostic = diagnostic
        self.catalog = catalog
        self.model = model
        self.exp = exp
        self.catalog_ref = catalog_ref
        self.model_ref = model_ref
        self.exp_ref = exp_ref
        self.outdir = outdir
        self.rebuild = rebuild

        self.loglevel = loglevel
        self.logger = log_configure(log_level=self.loglevel, log_name='OutputSaver')

    def generate_name(self, diagnostic_product: str, 
                      catalog: str = None, model: str = None, exp: str = None,
                      catalog_ref: str = None, model_ref: str = None, exp_ref: str = None,
                      extra_keys: dict = None) -> str:
        """
        Generate a filename based on provided parameters and additional user-defined keywords,
        including precise time intervals.

        Args:
            diagnostic_product (str, optional): Product of the diagnostic analysis.
            extra_keys (dict, optional): Dictionary of additional keys to include in the filename.

        Returns:
            str: A string representing the generated filename.
        """

        self.catalog = catalog or self.catalog
        self.model = model or self.model
        self.exp = exp or self.exp
        self.catalog_ref = catalog_ref or self.catalog_ref
        self.model_ref = model_ref or self.model_ref
        self.exp_ref = exp_ref or self.exp_ref

        if not self.catalog or not self.model or not self.exp:
            raise ValueError("Catalog, model, and exp must be specified to generate a filename.")

        # handle multimodel case: we need to check if the 6 keys above are lists
        if isinstance(self.model, list):
            model_value = "multimodel" if len(self.model) > 1 else self.model[0]
            if isinstance(self.catalog, list):
                catalog_value = None if len(self.catalog) > 1 else self.catalog[0]
            if isinstance(self.exp, list):
                exp_value = None if len(self.exp) > 1 else self.exp[0]
        else:
            model_value = self.model
            catalog_value = self.catalog
            exp_value = self.exp

        # handle multiref case
        if isinstance(self.model_ref, list):
            model_ref_value = "multiref" if len(self.model_ref) > 1 else self.model_ref[0]
            if isinstance(self.catalog_ref, list):
                catalog_ref_value = None if len(self.catalog_ref) > 1 else self.catalog_ref[0]
            if isinstance(self.exp_ref, list):
                exp_ref_value = None if len(self.exp_ref) > 1 else self.exp_ref[0]
        else:
            model_ref_value = self.model_ref
            catalog_ref_value = self.catalog_ref
            exp_ref_value = self.exp_ref

        parts_dict = {
            'diagnostic': self.diagnostic,
            'diagnostic_product': diagnostic_product,
            'catalog': catalog_value if model_value != "multimodel" else None,
            'model': model_value,
            'exp': exp_value if model_value != "multimodel" else None,
            'catalog_ref': catalog_ref_value if model_ref_value != "multiref" else None,
            'model_ref': model_ref_value,
            'exp_ref': exp_ref_value if model_ref_value != "multiref" else None,
        }

        # Add additional filename keys if provided
        if extra_keys:
            parts_dict.update(extra_keys)

        # Remove None values
        parts = [str(value) for value in parts_dict.values() if value is not None]

        # Join all parts
        filename = '.'.join(parts)

        self.logger.debug(f"Generated filename: {filename}")
        return filename

    def save_netcdf(self, dataset: xr.Dataset, diagnostic_product: str, extra_keys: dict = None):
        """
        Save an xarray Dataset as a NetCDF file with a generated filename.

        Args:
            dataset (xr.Dataset): The xarray Dataset to save.
            diagnostic_product (str): Product of the diagnostic analysis.
            extra_keys (dict, optional): Dictionary of additional keys to include in the filename.
        """
        filename = self.generate_name(diagnostic_product=diagnostic_product, extra_keys=extra_keys) + '.nc'

        folder = os.path.join(self.outdir, 'netcdf')
        create_folder(folder=str(folder), loglevel=self.loglevel)
        filepath = os.path.join(folder, filename)
        dataset.to_netcdf(filepath)

        self.logger.info(f"Saved NetCDF: {filepath}")
        return filepath

    def save_pdf(self, fig: plt.Figure, diagnostic_product: str, extra_keys: dict = None,  metadata: dict = None):
        """
        Save a Matplotlib figure as a PDF file with a generated filename.

        Args:
            fig (plt.Figure): The Matplotlib figure to save.
            diagnostic_product (str): Product of the diagnostic analysis.
            extra_keys (dict, optional): Dictionary of additional keys to include in the filename.
            metadata (dict, optional): Additional metadata to include in the PDF file.
        """
        filename = self.generate_name(diagnostic_product=diagnostic_product, extra_keys=extra_keys) + '.pdf'

        folder = os.path.join(self.outdir, 'pdf')
        create_folder(folder=str(folder), loglevel=self.loglevel)
        filepath = os.path.join(folder, filename)
        fig.savefig(filepath, format='pdf', bbox_inches='tight')

        metadata = self.create_metadata(diagnostic_product=diagnostic_product, extra_keys=extra_keys, metadata=metadata)
        add_pdf_metadata(filepath, metadata, loglevel=self.loglevel)

        self.logger.info(f"Saved PDF: {filepath}")
        return filepath

    def save_png(self, fig: plt.Figure, diagnostic_product: str, extra_keys: dict = None,  metadata: dict = None,
                 dpi: int = 300):
        """
        Save a Matplotlib figure as a PNG file with a generated filename.

        Args:
            fig (plt.Figure): The Matplotlib figure to save.
            diagnostic_product (str): Product of the diagnostic analysis.
            extra_keys (dict, optional): Dictionary of additional keys to include in the filename.
            metadata (dict, optional): Additional metadata to include in the PNG file.
            dpi (int, optional): Dots per inch for the PNG file.
        """

        filename = self.generate_name(diagnostic_product=diagnostic_product, extra_keys=extra_keys) + '.png'

        folder = os.path.join(self.outdir, 'png')
        create_folder(folder=str(folder), loglevel=self.loglevel)
        filepath = os.path.join(folder, filename)
        fig.savefig(filepath, format='png', dpi=dpi, bbox_inches='tight')

        metadata = self.create_metadata(diagnostic_product=diagnostic_product, extra_keys=extra_keys, metadata=metadata)
        add_png_metadata(filepath, metadata, loglevel=self.loglevel)

        self.logger.info(f"Saved PNG: {filepath}")
        return filepath

    def create_metadata(self, diagnostic_product: str, extra_keys: dict = None, metadata: dict = None) -> dict:
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

        # Merge with provided metadata
        metadata = update_metadata(base_metadata, metadata)
        return metadata
