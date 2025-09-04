import xarray as xr
import numpy as np
from aqua.util import to_list, extract_attrs
from aqua.logger import log_configure
from aqua.diagnostics.core import OutputSaver

from aqua.graphics import boxplot


class PlotBoxplots: 
    def __init__(self, 
                 diagnostic='boxplots',
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
        catalog = extract_attrs(data, 'AQUA_catalog')
        model = extract_attrs(data, 'AQUA_model')
        exp = extract_attrs(data, 'AQUA_exp')

        model_ref = extract_attrs(data_ref, 'AQUA_model')
        exp_ref = extract_attrs(data_ref, 'AQUA_exp')

        self.logger.info(f'catalogs: {catalog}, models: {model}, experiments: {exp}')
        self.logger.info(f'ref catalogs: {extract_attrs(data_ref, "catalog")}, models: {model_ref}, experiments: {exp_ref}')

        outputsaver = OutputSaver(
            diagnostic=self.diagnostic,
            catalog=catalog,
            model=model,
            exp=exp,
            model_ref=model_ref,
            exp_ref=exp_ref,
            outputdir=self.outputdir,
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


    def plot_boxplots(self, data, data_ref=None, var=None, title=None):
        """
        Plot boxplots for specified variables in the dataset.

        Args:
            data (xarray.Dataset or list of xarray.Dataset): Input dataset(s) containing the fldmeans of the variables to plot.
            data_ref (xarray.Dataset or list of xarray.Dataset, optional): Reference dataset(s) for comparison.
            var (str or list of str): Variable name(s) to plot. If None, uses all variables in the dataset.
            title (str, optional): Title for the plot. If None, a default title will be generated.
        """

        data = to_list(data)
        data_ref = to_list(data_ref) if data_ref is not None else []

        fldmeans = data + data_ref if data_ref else data
        model_names = extract_attrs(fldmeans, 'AQUA_model')
        exp_names = extract_attrs(fldmeans, 'AQUA_exp')

        dataset_info = ', '.join(f'{m} (experiment {e})' for m, e in zip(model_names, exp_names))
        description = f"Boxplot of ({', '.join(var) if isinstance(var, list) else var}) for: {dataset_info}"

        long_names = []
        for var_name in to_list(var):
            var_name = var_name[1:] if var_name.startswith('-') else var_name
            long_name = extract_attrs(fldmeans[0][var_name], 'long_name')
            long_names.append(long_name if long_name else var_name)

        fig, ax = boxplot(fldmeans=fldmeans, model_names=model_names, variables=var,
                         variable_names=long_names, title=title, loglevel=self.loglevel)
        
        if self.save_pdf:
            self._save_figure(fig, data, data_ref, var, format='pdf')
        if self.save_png:
            self._save_figure(fig, data, data_ref, var, format='png')