"""Functions to plot monthly and annual data, as well as reference data."""
import matplotlib.pyplot as plt
import xarray as xr
from aqua.util import to_list


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
        kind (str, optional): 'monthly' or 'annual'. Determines label suffix and line style.
    """
    data = to_list(data)
    data_labels = to_list(data_labels) if data_labels is not None else None

    linestyle = '-' if kind == 'monthly' else '--'
    suffix = f' {kind}' if kind in ['monthly', 'annual'] else ''
    lines = []

    for i in range(len(data)):
        da = data[i]
        if data_labels and not realization:
            label = data_labels[i] + suffix
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

        line = da.plot(**plot_kwargs)
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
    suffix = f' {kind}' if kind in ['monthly', 'annual'] else ''

    colors = ['black', 'darkgrey', 'grey']

    for i in range(len(ref_data)):
        ref_da = ref_data[i]
        if ref_label and isinstance(ref_label, list):
            label = ref_label[i] + suffix
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

        ref_da.plot(**plot_kwargs)

def plot_timeseries_ensemble(ax: plt.Axes,
                             data: xr.DataArray,
                             data_label: str,
                             std_data: xr.DataArray | None = None,
                             lw: float = 1.5,
                             kind: str = 'monthly'):
    """
    Plot ensemble time series data (monthly or annual) on the given axis.

    Args:
        ax (matplotlib.pyplot.Axes): Axis object to plot the data on.
        data (xr.DataArray): Ensemble mean time series data to plot.
        data_label (str, optional): Label for the data.
        std_data (xr.DataArray, optional): Standard deviation of the ensemble data.
        lw (float, optional): Line width. Default is 1.5.
        kind (str, optional): 'monthly' or 'annual'. Determines label suffix and line style.
    """
    linestyle = '-' if kind == 'monthly' else '--'
    suffix = f' {kind}' if kind in ['monthly', 'annual'] else ''

    plot_kwargs = {
        'ax': ax,
        'label': data_label + suffix if data_label else None,
        'lw': lw,
        'linestyle': linestyle,
        'color': "#f89e13"
    }

    if std_data is not None:
        if kind == 'monthly':
            ax.fill_between(data.time,
                            data - 2.*std_data.sel(month=data["time.month"]),
                            data + 2.*std_data.sel(month=data["time.month"]),
                            alpha=0.25, facecolor="#f89e13")
        elif kind == 'annual':
            ax.fill_between(data.time,
                            data - 2.*std_data,
                            data + 2.*std_data,
                            alpha=0.2, facecolor="#f89e13")
            
    data.plot(**plot_kwargs)
