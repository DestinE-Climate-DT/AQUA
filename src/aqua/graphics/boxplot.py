import matplotlib.pyplot as plt
import xarray as xr
import numpy as np
import pandas as pd
import seaborn as sns
from metpy.units import units
from aqua.util import to_list
from aqua.logger import log_configure
from .styles import ConfigStyle

def boxplot(fldmeans: list[xr.Dataset],
            model_names: list[str],
            variables: list[str],
            variable_names: list[str] = None,
            title: str = None,
            style: str = None,
            loglevel: str = 'WARNING'):
    """
    Generate a boxplot of precomputed field-mean values for multiple variables and models.

    Args:
        fldmeans (list of xarray.Dataset): Precomputed fldmean() for each model.
        model_names (list of str): Names corresponding to each fldmean dataset.
        variables (list of str): Variable names to be plotted (as in the fldmean Datasets).
        variable_names (list of str, optional): Display names for the variables.
        title (str, optional): Title for the plot.
        style (str, optional): Style to apply to the plot.
        loglevel (str): Logging level, unused but kept for compatibility.

    Returns:
        tuple: Matplotlib figure and axis.
    """

    logger = log_configure(loglevel, 'boxplot')
    ConfigStyle(style=style, loglevel=loglevel)
    sns.set_palette("pastel")
    fontsize = 18

    fldmeans, model_names, variables = to_list(fldmeans), to_list(model_names), to_list(variables)

    # Map internal variable names to display names
    if variable_names and len(variable_names) == len(variables):
        labels = dict(zip(variables, variable_names))
    else:
        labels = {v: v for v in variables}

    # Gather data
    records = []
    unit_sets = {}

    for ds, model in zip(fldmeans, model_names):
        for var in variables:
            invert = var.startswith('-')
            var_name = var.lstrip('-')

            if var_name not in ds:
                logger.warning(f"Variable '{var_name}' missing in {model}, skipping.")
                continue

            data = ds[var_name].values.flatten()
            data = -data if invert else data
            data = data[np.isfinite(data)]

            # Track units
            unit_attr = ds[var_name].attrs.get('units')
            if unit_attr:
                base = units(unit_attr).to_base_units()
                unit_sets.setdefault(var_name, set()).add(base)

            for val in data:
                records.append({
                    'Variables': labels[var],
                    'Values': val,
                    'Models': model
                })

    df = pd.DataFrame.from_records(records)


    # Plot
    fig, ax = plt.subplots(figsize=(12, 8))
    sns.boxplot(data=df, x='Variables', y='Values', hue='Models', ax=ax)

    if title:
        ax.set_title(title, fontsize=fontsize + 4)
    else:
        vars_str = ', '.join(labels[v] for v in variables)
        models_str = ', '.join(model_names)
        ax.set_title(f'Boxplot of {vars_str}\nAcross Models: {models_str}', fontsize=fontsize + 2)

    ax.set_xlabel('Variables', fontsize=fontsize)

    # Determine y-axis label based on units
    global_units_found = set().union(*unit_sets.values())
    if len(global_units_found) == 1:
        first_var = variables[0][1:] if variables[0].startswith('-') else variables[0]
        ax.set_ylabel(fldmeans[0][first_var].attrs.get('units'), fontsize=fontsize, labelpad=12)    
    else:
        ax.set_ylabel('Values (various units)', fontsize=fontsize, labelpad=12)

    ax.tick_params(axis='x', labelsize=fontsize - 4)
    ax.tick_params(axis='y', labelsize=fontsize - 4)
    ax.legend(loc='upper right', fontsize=fontsize)
    ax.grid(True)
    fig.tight_layout()

    return fig, ax