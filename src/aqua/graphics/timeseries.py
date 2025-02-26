"""
Function to plot timeseries and reference data,
both with monthly and annual aggregation options
"""
import xarray as xr
import matplotlib.pyplot as plt
from .plot_timeseries_data import plot_monthly_data, plot_annual_data
from .plot_timeseries_ref_data import plot_ref_monthly_data, plot_ref_annual_data
from aqua.logger import log_configure
from .styles import ConfigStyle


def plot_timeseries(monthly_data=None,
                    annual_data=None,
                    ref_monthly_data=None,
                    ref_annual_data=None,
                    std_monthly_data=None,
                    std_annual_data=None,
                    data_labels: list = None,
                    ref_label: str = None,
                    style: str = None,
                    loglevel: str = 'WARNING',
                    **kwargs):
    """
    monthly_data and annual_data are list of xr.DataArray
    that are plot as timeseries together with their reference
    data and standard deviation.

    Arguments:
        monthly_data (list of xr.DataArray): monthly data to plot
        annual_data (list of xr.DataArray): annual data to plot
        ref_monthly_data (xr.DataArray): reference monthly data to plot
        ref_annual_data (xr.DataArray): reference annual data to plot
        std_monthly_data (xr.DataArray): standard deviation of the reference monthly data
        std_annual_data (xr.DataArray): standard deviation of the reference annual data
        data_labels (list of str): labels for the data
        ref_label (str): label for the reference data
        style (str): style to use for the plot. By default the schema specified in the configuration file is used.
        loglevel (str): logging level

    Keyword Arguments:
        figsize (tuple): size of the figure
        title (str): title of the plot

    Returns:
        fig, ax (tuple): tuple containing the figure and axis objects
    """
    logger = log_configure(loglevel, 'PlotTimeseries')
    ConfigStyle(style=style, loglevel=loglevel)

    fig_size = kwargs.get('figsize', (10, 5))
    fig, ax = plt.subplots(1, 1, figsize=fig_size)


    if monthly_data is not None:
        plot_monthly_data(ax, monthly_data, data_labels, logger)

    if annual_data is not None:
        plot_annual_data(ax, annual_data, data_labels, logger)

    if ref_monthly_data is not None:
        plot_ref_monthly_data(ax, ref_monthly_data, std_monthly_data, ref_label, logger)

    if ref_annual_data is not None:
        plot_ref_annual_data(ax, ref_annual_data, std_annual_data, ref_label, logger)
    ax.legend(fontsize='small')
    ax.grid(True, axis="both")

    title = kwargs.get('title', None)
    if title:
        ax.set_title(title, fontsize=13, fontweight='bold')

    return fig, ax


def plot_seasonalcycle(data=None,
                       ref_data=None,
                       std_data=None,
                       data_labels: list = None,
                       ref_label: str = None,
                       style: str = None,
                       grid=True,
                       loglevel: str = 'WARNING',
                       **kwargs):
    """
    Plot the seasonal cycle of the data and the reference data.

    Arguments:
        data (list of xr.DataArray): data to plot
        ref_data (xr.DataArray): reference data to plot
        std_data (xr.DataArray): standard deviation of the reference data
        data_labels (list of str): labels for the data
        ref_label (str): label for the reference data
        style (str): style to use for the plot. By default the schema specified in the configuration file is used.
        grid (bool): if True, plot grid
        loglevel (str): logging level

    Keyword Arguments:
        figsize (tuple): size of the figure
        title (str): title of the plot

    Returns:
        fig, ax (tuple): tuple containing the figure and axis objects
    """

    logger = log_configure(loglevel, 'PlotSeasonalCycle')
    ConfigStyle(style=style, loglevel=loglevel)

    fig_size = kwargs.get('figsize', (6, 4))
    fig, ax = plt.subplots(1, 1, figsize=fig_size)

    monthsNumeric = range(0, 13 + 1)  # Numeric months
    monthsNames = ["", "J", "F", "M", "A", "M", "J", "J", "A", "S", "O", "N", "D", ""]


    if data is not None:
        if isinstance(data, xr.DataArray):
            data = [data]
        for i in range(len(data)):
            if data_labels:
                label = data_labels[i]
            else:
                label = None
            try:
                mon_data = _extend_cycle(data[i], loglevel)
                mon_data.plot(ax=ax, label=label, lw=3)
                ax.set(xlim=(data.time[0], data.time[-1]))
            except Exception as e:
                logger.debug(f"Error plotting data: {e}")

    if ref_data is not None:
        try:
            ref_data = _extend_cycle(ref_data, loglevel)
            ref_data.plot(ax=ax, label=ref_label, color='black', lw=3)
            if std_data is not None:
                std_data = _extend_cycle(std_data, loglevel)
                std_data.compute()
                ax.fill_between(ref_data.month,
                                ref_data - 2.*std_data,
                                ref_data + 2.*std_data,
                                facecolor='grey', alpha=0.5)
        except Exception as e:
            logger.debug(f"Error plotting std data: {e}")

    ax.legend(fontsize='small')
    ax.set_xticks(monthsNumeric)
    ax.set_xticklabels(monthsNames)
    ax.set_xlim(0.5, 12.5)
    ax.set_axisbelow(True)

    if grid:
        ax.grid(True, axis="both")

    title = kwargs.get('title', None)
    if title is not None:
        ax.set_title(title, fontsize=13, fontweight='bold')

    return fig, ax


def _extend_cycle(data: xr.DataArray = None, loglevel='WARNING'):
    """
    Add december value at the beginning and january value at the end of the data
    for a cyclic plot

    Arguments:
        data (xr.DataArray): data to extend
        loglevel (str): logging level. Default is 'WARNING'

    Returns:
        data (xr.DataArray): extended data (if possible)
    """
    if data is None:
        raise ValueError("No data provided")

    logger = log_configure(loglevel, 'ExtendCycle')

    try:
        left_data = data.isel(month=11)
        right_data = data.isel(month=0)
    except IndexError:
        logger.debug("No data for January or December")
        return data

    left_data['month'] = 0
    right_data['month'] = 13

    return xr.concat([left_data, data, right_data], dim='month')
