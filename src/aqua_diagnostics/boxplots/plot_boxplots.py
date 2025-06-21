import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
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
        
        catalog = [ds.catalog for ds in data] if isinstance(data, list) else data.catalog
        model = [ds.model for ds in data] if isinstance(data, list) else data.model
        exp = [ds.exp for ds in data] if isinstance(data, list) else data.exp

        if data_ref is not None:
            catalog_ref = [ds.catalog for ds in data_ref] if isinstance(data_ref, list) else data_ref.catalog
            model_ref = [ds.model for ds in data_ref] if isinstance(data_ref, list) else [data_ref.model]
            exp_ref = [ds.exp for ds in data_ref] if isinstance(data_ref, list) else [data_ref.exp]
        else:
            catalog_ref = None
            model_ref = None
            exp_ref = None

        self.logger.info(f'catalogs: {catalog}, models: {model}, experiments: {exp}')
        self.logger.info(f'catalogs: {catalog_ref}, models: {model_ref}, experiments: {exp_ref}')

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

        #models_info = ', '.join(f'{model} (experiment {exp})' for model, exp in zip(model+model_ref, exp+exp_ref)) 
        #description = (
        #    f"Boxplot of variables ({', '.join(var)}). "
        #    f"for: {models_info}"
        #)

        #metadata = {"Description": description}
        extra_keys = {}

        metadata = {}

        if var is not None:
            extra_keys.update({'var': var})

        if format == 'pdf':
            outputsaver.save_pdf(fig, diagnostic_product='boxplot',
                                 extra_keys=extra_keys, metadata=metadata)
        elif format == 'png':
            outputsaver.save_png(fig, diagnostic_product='boxplot',
                                 extra_keys=extra_keys, metadata=metadata)
        else:
            raise ValueError(f'Format {format} not supported. Use png or pdf.')

    def plot_boxplots(self, data, data_ref, var):
        """
        Plot boxplots for specified variables in the dataset.

        Args:
            data (xarray.Dataset or list of xarray.Dataset): Input dataset(s) containing the fldmeans of the variables to plot.
            data_ref (xarray.Dataset or list of xarray.Dataset, optional): Reference dataset(s) for comparison.
            var (str or list of str): Variable name(s) to plot. If None, uses all variables in the dataset.
        """
        fldmeans = data + data_ref if data_ref is not None else data
        models = [ds.model for ds in fldmeans] if isinstance(fldmeans, list) else [fldmeans.model]
        exps = [ds.exp for ds in fldmeans] if isinstance(fldmeans, list) else [fldmeans.exp]

        fig, ax = boxplot(fldmeans=fldmeans, model_names=models, variables=var, loglevel=self.loglevel)
            
        if self.save_pdf:
            self._save_figure(fig=fig, format='pdf', data=data,
                              data_ref=data_ref, var=var)
        if self.save_png:
            self._save_figure(fig=fig, format='png', 
                              data=data,
                              data_ref=data_ref, var=var)