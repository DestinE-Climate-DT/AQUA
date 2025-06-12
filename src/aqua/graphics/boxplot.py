import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


def boxplot(fldmeans, model_names, variables, variable_names=None, loglevel='WARNING'):
    """
    Generate a boxplot of precomputed field-mean values for multiple variables and models.

    Args:
        fldmeans (list of xarray.Dataset): Precomputed fldmean() for each model.
        model_names (list of str): Names corresponding to each fldmean dataset.
        variables (list of str): Variable names to be plotted (as in the fldmean Datasets).
        variable_names (list of str, optional): Display names for the variables.
        loglevel (str): Logging level, unused but kept for compatibility.

    Returns:
        tuple: Matplotlib figure and axis.
    """

    sns.set_palette("pastel")
    fontsize = 18

    # Map internal variable names to display names
    label_map = {
        var_name: name for var_name, name in zip(variables, variable_names)
    } if variable_names and len(variable_names) == len(variables) else {var: var for var in variables}

    # Collect data
    boxplot_data = {'Variables': [], 'Values': [], 'Datasets': []}
    for model_ds, model_name in zip(fldmeans, model_names):
        for var_name in variables:
            var = var_name[1:] if var_name.startswith('-') else var_name  # Remove leading '-' if present
            if var not in model_ds:
                continue
            values = model_ds[var].values.flatten()
            if var_name.startswith('-'):
                values = -values
            values = values[np.isfinite(values)]
            boxplot_data['Variables'].extend([label_map[var_name]] * len(values))
            boxplot_data['Values'].extend(values)
            boxplot_data['Datasets'].extend([model_name] * len(values))

    # Build DataFrame
    df = pd.DataFrame(boxplot_data)

    # Plot
    fig, ax = plt.subplots(figsize=(12, 8))
    sns.boxplot(data=df, x='Variables', y='Values', hue='Datasets', ax=ax)

    ax.set_xlabel('Variables', fontsize=fontsize)
    ax.set_ylabel('Values', fontsize=fontsize + 2)
    ax.tick_params(axis='x', labelsize=fontsize - 4)
    ax.tick_params(axis='y', labelsize=fontsize)
    ax.legend(loc='upper right', fontsize=fontsize)
    ax.grid(True)
    fig.tight_layout()

    return fig, ax
