import xarray as xr
import numpy as np
from aqua.util import to_list, extract_attrs, time_to_string, get_realizations
from aqua.logger import log_configure
from aqua.diagnostics.core import OutputSaver
import matplotlib as plt

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

    def _save_figure(self, fig, data, data_ref, var,
                     diagnostic_product='boxplot', description=None, format='png'):
        """
        Handles the saving of a figure using OutputSaver.

        Args:
            fig (matplotlib.Figure): The figure to save.
            data (xarray.Dataset or list of xarray.Dataset): Input dataset(s) containing the fldmeans of the variables to plot.
            data_ref (xarray.Dataset or list of xarray.Dataset, optional): Reference dataset(s) for comparison.
            var (str): Variable name.
            diagnostic_product (str): Name of the diagnostic product.
            description (str): Description of the figure.
            format (str): Format to save the figure ('png' or 'pdf').
        """
        catalog = extract_attrs(data, 'AQUA_catalog')
        model = extract_attrs(data, 'AQUA_model')
        exp = extract_attrs(data, 'AQUA_exp')
        startdates = extract_attrs(data, 'startdate')
        enddates = extract_attrs(data, 'enddate')

        model_ref = extract_attrs(data_ref, 'AQUA_model')
        exp_ref = extract_attrs(data_ref, 'AQUA_exp')
        startdates_ref = extract_attrs(data_ref, 'startdate')
        enddates_ref = extract_attrs(data_ref, 'enddate')

        self.logger.info(f'catalogs: {catalog}, models: {model}, experiments: {exp}')
        self.logger.info(f'ref catalogs: {extract_attrs(data_ref, "catalog")}, models: {model_ref}, experiments: {exp_ref}')

        self.realizations = get_realizations(data)

        outputsaver = OutputSaver(
            diagnostic=self.diagnostic,
            catalog=catalog,
            model=model,
            exp=exp,
            model_ref=model_ref,
            exp_ref=exp_ref,
            realization=self.realizations,
            outputdir=self.outputdir,
            loglevel=self.loglevel
        )

        all_models = model + (model_ref or [])
        all_exps = exp + (exp_ref or [])    
        all_startdates = startdates + (startdates_ref or [] )  
        all_enddates = enddates + (enddates_ref or [] )     
        dataset_info = ', '.join(
            f"{m} (exp: {e}) from {time_to_string(s)} to {time_to_string(en)}"
            for m, e, s, en in zip(all_models, all_exps, all_startdates, all_enddates)
        )
        if not description:
            description = f"Boxplot for: {dataset_info}."

        if self.anomalies:
            ref_name = extract_attrs(data_ref[self.ref_number], 'AQUA_model')
            description += (
                f" Anomalies with respect to {ref_name} mean value are shown. "
                "The dashed line represents the mean value, the solid line the median value, "
                "and the number indicates the absolute mean value."
            )

        metadata = {"Description": description}
        extra_keys = {'var': '_'.join(var) if isinstance(var, list) else var}

        if format == 'pdf':
            outputsaver.save_pdf(fig, diagnostic_product='boxplot', extra_keys=extra_keys, metadata=metadata)
        elif format == 'png':
            outputsaver.save_png(fig, diagnostic_product='boxplot', extra_keys=extra_keys, metadata=metadata)
        else:
            raise ValueError(f'Unsupported format: {format}. Use "png" or "pdf".')


    def plot_boxplots(self, data, data_ref=None, var=None, anomalies=False, add_mean_line=False, 
                      ref_number=0, title=None, description=None):
        """
        Plot boxplots for specified variables in the dataset.

        Args:
            data (xarray.Dataset or list of xarray.Dataset): Input dataset(s) containing the fldmeans of the variables to plot.
            data_ref (xarray.Dataset or list of xarray.Dataset, optional): Reference dataset(s) for comparison.
            var (str or list of str): Variable name(s) to plot. If None, uses all variables in the dataset.
            anomalies (bool): Whether to plot anomalies instead of absolute values.
            add_mean_line (bool): Whether to add dashed lines for means.
            ref_number (int): Position of reference dataset in data_ref list to use when plotting anomalies.
            title (str, optional): Title for the plot. If None, a default title will be generated.
            description(str, optional): Description for the plot. If None, a default description will be generated.
        """

        self.ref_number = ref_number
        self.anomalies = anomalies 
        data = to_list(data)
        data_ref = to_list(data_ref) if data_ref is not None else []

        fldmeans = data + data_ref if data_ref else data
        model_names = extract_attrs(fldmeans, 'AQUA_model')
        exp_names = extract_attrs(fldmeans, 'AQUA_exp')

        base_vars = []
        long_names = []
        for v in to_list(var):
            base_var = v.lstrip('-')
            base_vars.append(base_var)
            long_name = extract_attrs(fldmeans[0][base_var], 'long_name')
            long_names.append(long_name or base_var)

        # Compute anomalies relative to reference

        abs_means = []
        for ds in fldmeans:
            mean_ds = ds.load().mean(dim='time')
            means_dict = {v: mean_ds[v].item() for v in mean_ds.data_vars}
            abs_means.append(means_dict)
        
        if self.anomalies and data_ref:
            self.logger.info(f"Computing anomalies relative to reference dataset {extract_attrs(data_ref[self.ref_number], 'AQUA_model')}")
            ref = data_ref[self.ref_number] 
            fldmeans = [ds - ref.mean('time') for ds in fldmeans]

        if not title:
            model_exp_list = [f"{m} ({e})" for m, e in zip(model_names, exp_names)]
            model_exp_list_unique = list(dict.fromkeys(model_exp_list))
            title = "Boxplot for: " + ", ".join(model_exp_list_unique)

        # Plot boxplot 
        fig, ax = boxplot(fldmeans=fldmeans, model_names=model_names, variables=var, variable_names=long_names, title=title, 
                          add_mean_line=add_mean_line, loglevel=self.loglevel)

        if self.anomalies and data_ref:
            ax.set_ylabel("Anomalies with respect to observation mean (W/m2)")

            if add_mean_line:
                # Annotate absolute median values on the boxplots
                n_vars = len(base_vars)
                n_datasets = len(abs_means)

                for dataset_idx in range(n_datasets):
                    for var_idx, v in enumerate(var):
                        box_index = dataset_idx * n_vars + var_idx
                        try:
                            patch = [p for p in ax.patches if isinstance(p, plt.patches.PathPatch)][box_index]
                        except IndexError:
                            continue  

                        x = patch.get_path().vertices[:, 0].mean() + 0.05
                        base_var = v.lstrip('-')

                        means_dict = abs_means[dataset_idx]
                        if base_var in means_dict:
                            abs_val = means_dict[base_var]  # absolute mean value
                            anom_val = fldmeans[dataset_idx][base_var].mean(dim="time")
                            if v.startswith('-'): 
                                anom_val = -anom_val

                            ax.text(
                                x, anom_val, f"{abs_val:.2f}",
                                ha='center', va='bottom',
                                color='black', fontweight='bold'
                            )


        if self.save_pdf:
            self._save_figure(fig=fig, data=data, data_ref=data_ref, var=var, diagnostic_product='boxplot', format='pdf')
        if self.save_png:
            self._save_figure(fig=fig, data=data, data_ref=data_ref, var=var, diagnostic_product='boxplot', format='png')
