import xarray as xr
import numpy as np
from aqua.logger import log_configure
from aqua.diagnostics.core import OutputSaver

from aqua.graphics import boxplot


class PlotBoxplots: 
    def __init__(self, 
                 diagnostic,
                 save_pdf=True, save_png=True, 
                 dpi=300, outputdir='./',
                 loglevel='WARNING'):
        """
        Initialize the PlotGlobalBiases class.

        Args:
            diagnostic (str): Name of the diagnostic.
            save_pdf (bool): Whether to save the figure as PDF.
            save_png (bool): Whether to save the figure as PNG.
            dpi (int): Resolution of saved figures.
            outputdir (str): Output directory for saved plots.
            loglevel (str): Logging level.
        """
        self.diagnostic = diagnostic
        self.save_pdf = save_pdf
        self.save_png = save_png
        self.dpi = dpi
        self.outputdir = outputdir
        self.loglevel = loglevel
        self.logger = log_configure(log_level=loglevel, log_name='Boxplots')

    
    def _extract_attrs(self, ds_list, attr):
        """Extract attribute(s) from dataset or list of datasets."""
        if ds_list is None:
            return None
        if isinstance(ds_list, list):
            return [getattr(ds, attr, None) for ds in ds_list]
        return [getattr(ds_list, attr, None)]


    def _save_figure(self, fig,
                     data, data_ref, var, format='png'):
        """
        Handles the saving of a figure using OutputSaver.

        Args:
            fig (matplotlib.Figure): The figure to save.
            data (xarray.Dataset or list of xarray.Dataset): Input dataset(s) containing the fldmeans of the variables to plot.
            data_ref (xarray.Dataset or list of xarray.Dataset, optional): Reference dataset(s) for comparison.
            diagnostic_product (str): Name of the diagnostic product.
            description (str): Description of the figure.
            var (str): Variable name.
            format (str): Format to save the figure ('png' or 'pdf').
        """
        catalog = self._extract_attrs(data, 'catalog')
        model = self._extract_attrs(data, 'model')
        exp = self._extract_attrs(data, 'exp')

        model_ref = self._extract_attrs(data_ref, 'model')
        exp_ref = self._extract_attrs(data_ref, 'exp')

        self.logger.info(f'catalogs: {catalog}, models: {model}, experiments: {exp}')
        self.logger.info(f'ref catalogs: {self._extract_attrs(data_ref, "catalog")}, models: {model_ref}, experiments: {exp_ref}')

        outputsaver = OutputSaver(
            diagnostic=self.diagnostic,
            catalog=catalog,
            model=model,
            exp=exp,
            model_ref=model_ref,
            exp_ref=exp_ref,
            outdir=self.outputdir,
            loglevel=self.loglevel
        )

        all_models = model + (model_ref or [])
        all_exps = exp + (exp_ref or [])
        dataset_info = ', '.join(f'{m} (experiment {e})' for m, e in zip(all_models, all_exps))

        description = f"Boxplot of variables ({', '.join(var) if isinstance(var, list) else var}) for: {dataset_info}"
        metadata = {"Description": description}
        extra_keys = {'var': '_'.join(var) if isinstance(var, list) else var}

        if format == 'pdf':
            outputsaver.save_pdf(fig, diagnostic_product='boxplot', extra_keys=extra_keys, metadata=metadata)
        elif format == 'png':
            outputsaver.save_png(fig, diagnostic_product='boxplot', extra_keys=extra_keys, metadata=metadata)
        else:
            raise ValueError(f'Unsupported format: {format}. Use "png" or "pdf".')


    def plot_boxplots(self, data, data_ref=None, var=None):
        """
        Plot boxplots for specified variables in the dataset.

        Args:
            data (xarray.Dataset or list of xarray.Dataset): Input dataset(s) containing the fldmeans of the variables to plot.
            data_ref (xarray.Dataset or list of xarray.Dataset, optional): Reference dataset(s) for comparison.
            var (str or list of str): Variable name(s) to plot. If None, uses all variables in the dataset.
        """

        fldmeans = data + data_ref if data_ref else data
        model_names = self._extract_attrs(fldmeans, 'model')

        fig, ax = boxplot(fldmeans=fldmeans, model_names=model_names, variables=var, loglevel=self.loglevel)

        if self.save_pdf:
            self._save_figure(fig, data, data_ref, var, format='pdf')
        if self.save_png:
            self._save_figure(fig, data, data_ref, var, format='png')