import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from aqua.util import to_list
from aqua.logger import log_configure
from .styles import ConfigStyle

def boxplot(fldmeans, model_names, variables, variable_names=None, title=None, style=None, loglevel='WARNING'):
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
    label_map = {
        var_name: name for var_name, name in zip(variables, variable_names)
    } if variable_names and len(variable_names) == len(variables) else {var: var for var in variables}

    # Collect data
    boxplot_data = {'Variables': [], 'Values': [], 'Datasets': []}
    for model_ds, model_name in zip(fldmeans, model_names):
        for var_name in variables:
            logger.info(f"Processing variable '{var_name}' for model '{model_name}'")
            var = var_name[1:] if var_name.startswith('-') else var_name  # Remove leading '-' if present
            if var not in model_ds:
                logger.warning(f"Variable '{var}' not found in dataset '{model_name}'. Skipping.")
                continue
            values = model_ds[var].values.flatten()
            if var_name.startswith('-'):
                values = -values
            values = values[np.isfinite(values)]
            boxplot_data['Variables'].extend([label_map[var_name]] * len(values))
            boxplot_data['Values'].extend(values)
            boxplot_data['Datasets'].extend([model_name] * len(values))

    # Check units consistency
    units_for_var = set()
    unit_map = {}  
    for var_name in variables:
        var = var_name[1:] if var_name.startswith('-') else var_name
        for model_ds in fldmeans:
            unit = model_ds[var].attrs.get('units') if var in model_ds else None 
            if unit:
                units_for_var.add(unit)
            else:
                logger.warning(f"No units found for variable '{var}' in a dataset")

        unit_map[var] = units_for_var
        if len(units_for_var) > 1:
            logger.warning(f"Inconsistent units for variable '{var}': {units_for_var}")

    # Build DataFrame
    df = pd.DataFrame(boxplot_data)

    # Plot
    fig, ax = plt.subplots(figsize=(12, 8))
    sns.boxplot(data=df, x='Variables', y='Values', hue='Datasets', ax=ax)

    if title:
        ax.set_title(title, fontsize=fontsize + 4)
    else:
        vars_str = ', '.join(label_map[v] for v in variables)
        models_str = ', '.join(model_names)
        ax.set_title(f'Boxplot of {vars_str}\nAcross Models: {models_str}', fontsize=fontsize + 2)

    ax.set_xlabel('Variables', fontsize=fontsize)

    # Determine y-axis label based on units
    global_units_found = set().union(*unit_map.values())
    if len(global_units_found) == 1:
        ax.set_ylabel(next(iter(global_units_found)), fontsize=fontsize, labelpad=12)    
    else:
        ax.set_ylabel('Values (various units)', fontsize=fontsize, labelpad=12)

    ax.tick_params(axis='x', labelsize=fontsize - 4)
    ax.tick_params(axis='y', labelsize=fontsize - 4)
    ax.legend(loc='upper right', fontsize=fontsize)
    ax.grid(True)
    fig.tight_layout()

    return fig, ax