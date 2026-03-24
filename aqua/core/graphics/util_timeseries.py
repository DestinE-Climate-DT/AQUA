"""Functions to plot monthly and annual data, as well as reference data."""
import matplotlib.pyplot as plt
import xarray as xr
from aqua.core.util import to_list, unit_to_latex


def _plot_lx(da: xr.DataArray, ax: plt.Axes, **plot_kwargs):
    """
    Wrapper around xarray's plot method that temporarily 
    converts units to LaTeX format.
    
    Args:
        da (xr.DataArray): DataArray to plot.
        ax (plt.Axes): Axes to plot on.
        **plot_kwargs: Additional arguments passed to xarray's plot method.
    
    Returns:
        Same return type as xarray's plot method (Line2D or list of Line2D).
    """
    original_units = da.attrs.get('units', None)
    
    if original_units:
        # Create a copy with independent attrs to ensure original data is not modified
        da = da.copy(deep=False)
        # Copy attrs dictionary
        da.attrs = dict(da.attrs)
        units_latex = unit_to_latex(original_units)
        # Set LaTeX units only on the copy
        da.attrs['units'] = units_latex
    
    # Call xarray plot - it will automatically use the LaTeX
    result = da.plot(ax=ax, **plot_kwargs)
    
    return result
    

def plot_timeseries_data(ax: plt.Axes,
                         data: xr.DataArray | list[xr.DataArray],
                         data_labels: str | list[str] = None,
                         lw: float = 1.5,
                         realization: bool = False,
                         kind: str = 'monthly',
                         colors: list[str] = None) -> list[plt.Line2D]:
    """
    Plot time series data (monthly or annual) on the given axis.

    Args:
        ax (matplotlib.pyplot.Axes): Axis object to plot the data on.
        data (xr.DataArray | list[xr.DataArray]): Time series data to plot.
        data_labels (str | list[str], optional): Labels for the data.
        lw (float, optional): Line width. Default is 1.5.
        realization (bool, optional): Whether the data is a realization. Default is False.
        kind (str, optional): 'monthly' or 'annual'. Determines the line style.
        colors (list[str], optional): List of colors to use for the lines.
    
    Returns:
        list[plt.Line2D]: List of Line2D objects representing the plotted lines.
    """
    data = to_list(data)
    data_labels = to_list(data_labels) if data_labels is not None else None

    linestyle = '-' if kind == 'monthly' else '--'
    lines = []

    for i in range(len(data)):
        da = data[i]
        if data_labels and not realization:
            label = data_labels[i]
        else:
            label = None

        plot_kwargs = {
            'ax': ax,
            'label': label,
            'lw': lw,
            'linestyle': linestyle
        }

        if colors and i < len(colors):
            plot_kwargs['color'] = colors[i]

        if realization:
            plot_kwargs.update({
                'color': 'grey',
                'alpha': 0.5
            })

        line = _plot_lx(da, **plot_kwargs)
        # xarray returns a list for multiple lines, but just a Line2D for 1D plots
        if isinstance(line, list):
            lines.extend(line)
        else:
            lines.append(line)

    return lines


def plot_timeseries_ref_data(ax: plt.Axes,
                             ref_data: xr.DataArray | list[xr.DataArray],
                             std_data: xr.DataArray | list[xr.DataArray] = None,
                             ref_label: str | list[str] = None,
                             lw: float = 0.8,
                             kind: str = 'monthly'):
    """
    Plot reference time series data (monthly or annual) on the given axis.

    Args:
        ax (matplotlib.pyplot.Axes): Axis object to plot the data on.
        ref_data (xr.DataArray | list[xr.DataArray]): Reference time series data to plot.
        std_data (xr.DataArray | list[xr.DataArray], optional): Standard deviation of the reference data.
        ref_label (str | list[str] | None): Label for the reference data.
        lw (float, optional): Line width. Default is 0.8.
        kind (str, optional): 'monthly' or 'annual'. Determines label suffix and line style.
    """
    ref_data = to_list(ref_data)
    std_data = to_list(std_data) if std_data is not None else None
    ref_label = to_list(ref_label) if ref_label is not None else None

    linestyle = '-' if kind == 'monthly' else '--'

    colors = ['black', 'darkgrey', 'grey']

    for i in range(len(ref_data)):
        ref_da = ref_data[i]
        if ref_label and isinstance(ref_label, list):
            label = ref_label[i]
        else:
            label = None

        plot_kwargs = {
            'ax': ax,
            'label': label,
            'lw': lw,
            'linestyle': linestyle,
            'color': colors[i % len(colors)]
        }

        if std_data is not None:
            std_da = std_data[i]
            if kind == 'monthly':
                ax.fill_between(ref_da.time,
                                ref_da - 2.*std_da.sel(month=ref_da["time.month"]),
                                ref_da + 2.*std_da.sel(month=ref_da["time.month"]),
                                facecolor='grey', alpha=0.25)
            elif kind == 'annual':
                ax.fill_between(ref_da.time,
                                ref_da - 2.*std_da,
                                ref_da + 2.*std_da,
                                facecolor='black', alpha=0.2)

        _plot_lx(ref_da, **plot_kwargs)


def plot_timeseries_ensemble(ax: plt.Axes,
                             data: xr.DataArray,
                             data_label: str,
                             std_data: xr.DataArray | None = None,
                             lw: float = 1.5,
                             kind: str = 'monthly'):
    """
    Plot ensemble time series data (monthly or annual) on the given axis.

    NOTE: The ensemble module computes the mean and standard deviation Point-wise along the time axis.
          Therefore this function plots: mean(t) +/- 2xSTD(t)

    Args:
        ax (matplotlib.pyplot.Axes): Axis object to plot the data on.
        data (xr.DataArray): Ensemble mean time series data to plot.
        data_label (str, optional): Label for the data.
        std_data (xr.DataArray, optional): Standard deviation of the ensemble data.
        lw (float, optional): Line width. Default is 1.5.
        kind (str, optional): 'monthly' or 'annual'. Determines label suffix and line style.
    """
    linestyle = '-' if kind == 'monthly' else '--'

    plot_kwargs = {
        'ax': ax,
        'label': data_label if data_label else None,
        'lw': lw,
        'linestyle': linestyle,
        'color': "#f89e13" if kind == 'annual' else "#1898e0"
    }

    if std_data is not None:
        if kind == 'monthly':
            ax.fill_between(data.time,
                            data - 2.*std_data, #.sel(month=data["time.month"]), 
                            data + 2.*std_data, #.sel(month=data["time.month"]),                            
                            alpha=0.25, facecolor="#1898e0")
        elif kind == 'annual':
            ax.fill_between(data.time,
                            data - 2.*std_data,
                            data + 2.*std_data,
                            alpha=0.2, facecolor="#f89e13")
            
    _plot_lx(data, **plot_kwargs)
