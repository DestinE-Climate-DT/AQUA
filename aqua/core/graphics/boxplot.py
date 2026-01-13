import matplotlib.pyplot as plt
import xarray as xr
import numpy as np
import pandas as pd
import seaborn as sns
from metpy.units import units
from aqua.core.util import to_list, unit_to_latex
from aqua.core.logger import log_configure
from .styles import ConfigStyle
from matplotlib import colors as mcolors
import textwrap

def boxplot(fldmeans: list[xr.Dataset],
            model_names: list[str],
            variables: list[str],
            variable_names: list[str] = None,
            add_mean_line: bool = True,
            title: str = None,
            style: str = None,
            loglevel: str = 'WARNING'):
    """
    Generate a boxplot of precomputed field-mean values for multiple variables and models.
    A dashed horizontal line is added to indicate the mean for each box (slightly darker than box color).

    Args:
        fldmeans (list of xarray.Dataset): Precomputed fldmean() for each model.
        model_names (list of str): Names corresponding to each fldmean dataset.
        variables (list of str): Variable names to be plotted (as in the fldmean Datasets).
        variable_names (list of str, optional): Display names for the variables.
        add_mean_line (bool, optional): Whether to add dashed lines for means.
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

    # Create figure
    fig, ax = plt.subplots(figsize=(12, 8))

    # Explicit order for variables and models
    order = [labels[v] for v in variables]
    hue_order = list(model_names)

    # Plot the boxplots
    sns.boxplot(
        data=df, x='Variables', y='Values', hue='Models',
        order=order, hue_order=hue_order, width=0.8, ax=ax
    )

    wrapped_labels = [textwrap.fill(lbl, 14) for lbl in order]
    ax.set_xticks(range(len(wrapped_labels)))
    ax.set_xticklabels(wrapped_labels, fontsize=fontsize)

    # --- Add dashed mean lines for each box ---
    if add_mean_line:
        means = df.groupby(['Variables', 'Models'])['Values'].mean().unstack(fill_value=np.nan)

        n_vars = len(order)
        n_hues = len(hue_order)
        total_box_width = 0.8  # same width passed to sns.boxplot
        if n_hues > 0:
            single_box_width = total_box_width / n_hues
        else:
            single_box_width = total_box_width
        # Offsets for multiple models per variable
        offsets = np.linspace(
            -total_box_width/2 + single_box_width/2,
            total_box_width/2 - single_box_width/2,
            n_hues
        )

        palette = sns.color_palette()

        for i, var in enumerate(order):
            for j, model in enumerate(hue_order):
                if model not in means.columns or var not in means.index:
                    continue
                mean_val = means.loc[var, model]
                if np.isnan(mean_val):
                    continue
                x_center = i + offsets[j]
                x_left = x_center - single_box_width / 2
                x_right = x_center + single_box_width / 2

                # Darken the base color inline
                base_color = palette[j % len(palette)]
                rgb = np.array(mcolors.to_rgb(base_color))
                darker = tuple(rgb * 0.7)  # 0.7 = 30% darker

                ax.hlines(
                    mean_val, x_left, x_right,
                    colors=[darker],
                    linestyles='--', linewidth=2.5, zorder=5
                )

    # Title and labels
    if title:
        ax.set_title(title, fontsize=fontsize + 3)
    else:
        vars_str = ', '.join(labels[v] for v in variables)
        models_str = ', '.join(model_names)
        wrapped_title = textwrap.fill(f'Boxplot of {vars_str}', 70)
        ax.set_title(wrapped_title, fontsize=fontsize+2)
 
    ax.set_xlabel('Variables', fontsize=fontsize)

    # Y-axis label from units
    global_units_found = set().union(*unit_sets.values())
    if len(global_units_found) == 1:
        first_var = variables[0][1:] if variables[0].startswith('-') else variables[0]
        units_str = fldmeans[0][first_var].attrs.get('units', '')
        units_latex = unit_to_latex(units_str) if units_str else ''
        ax.set_ylabel(units_latex, fontsize=fontsize, labelpad=12)    
    else:
        ax.set_ylabel('Values (various units)', fontsize=fontsize, labelpad=12)

    ax.tick_params(axis='x', labelsize=fontsize - 4)
    ax.tick_params(axis='y', labelsize=fontsize - 4)
    ax.legend(loc='upper right', fontsize=fontsize)
    ax.grid(True)
    fig.tight_layout()
    return fig, ax