"""
Function to plot timeseries and reference data,
both with monthly and annual aggregation options
"""
import numpy as np
import matplotlib.pyplot as plt
from aqua.logger import log_configure


def plot_timeseries(monthly_data=None,
                    annual_data=None,
                    ref_monthly_data=None,
                    ref_annual_data=None,
                    std_monthly_data=None,
                    std_annual_data=None,
                    data_labels: list = None,
                    ref_label: str = None,
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
        loglevel (str): logging level

    Keyword Arguments:
        figsize (tuple): size of the figure
        title (str): title of the plot

    Returns:
        fig, ax (tuple): tuple containing the figure and axis objects
    """
    logger = log_configure(loglevel, 'PlotTimeseries')
    fig_size = kwargs.get('figsize', (10, 5))
    fig, ax = plt.subplots(1, 1, figsize=fig_size)

    color_list = ["#1898e0", "#8bcd45", "#f89e13", "#d24493",
                  "#00b2ed", "#dbe622", "#fb4c27", "#8f57bf",
                  "#00bb62", "#f9c410", "#fb4865", "#645ccc"]

    if monthly_data is not None:
        for i in range(len(monthly_data)):
            color = color_list[i]
            try:
                mon_data = monthly_data[i]
                if data_labels:
                    label = data_labels[i]
                    label += ' monthly'
                mon_data.plot(ax=ax, label=label, color=color)
            except Exception as e:
                logger.debug(f"Error plotting monthly data: {e}")

    if annual_data is not None:
        for i in range(len(annual_data)):
            color = color_list[i]
            try:
                ann_data = annual_data[i]
                if data_labels:
                    label = data_labels[i]
                    label += ' annual'
                ann_data.plot(ax=ax, label=label, color=color, linestyle='--')
            except Exception as e:
                logger.debug(f"Error plotting annual data: {e}")

    if ref_monthly_data is not None:
        try:
            ref_monthly_data.plot(ax=ax, label=ref_label + ' monthly', color='black', lw=0.6)
            if std_monthly_data is not None:
                std_monthly_data.compute()
                ax.fill_between(ref_monthly_data.time,
                                ref_monthly_data - 2.*std_monthly_data.sel(month=ref_monthly_data["time.month"]),
                                ref_monthly_data + 2.*std_monthly_data.sel(month=ref_monthly_data["time.month"]),
                                facecolor='grey', alpha=0.25)
        except Exception as e:
            logger.debug(f"Error plotting monthly std data: {e}")

    if ref_annual_data is not None:
        try:
            ref_annual_data.plot(ax=ax, label=ref_label + ' annual', color='black', linestyle='--', lw=0.6)
            if std_annual_data is not None:
                std_annual_data.compute()
                ax.fill_between(ref_annual_data.time,
                                ref_annual_data - 2.*std_annual_data,
                                ref_annual_data + 2.*std_annual_data,
                                facecolor='black', alpha=0.2)
        except Exception as e:
            logger.debug(f"Error plotting annual std data: {e}")

    ax.legend(fontsize='small')
    ax.grid(axis="x", color="k")
    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)

    title = kwargs.get('title', None)
    if title:
        ax.set_title(title)

    return fig, ax


def plot_seasonalcycle(data=None,
                       ref_data=None,
                       std_data=None,
                       data_labels: list = None,
                       ref_label: str = None,
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
        loglevel (str): logging level

    Keyword Arguments:
        figsize (tuple): size of the figure
        title (str): title of the plot

    Returns:
        fig, ax (tuple): tuple containing the figure and axis objects
    """
    logger = log_configure(loglevel, 'PlotSeasonalCycle')
    fig_size = kwargs.get('figsize', (10, 5))
    fig, ax = plt.subplots(1, 1, figsize=fig_size)

    monthsNumeric = range(1, 12 + 1)  # Numeric months
    monthsNames = ["J", "F", "M", "A", "M", "J", "J", "A", "S", "O", "N", "D"]

    color_list = ["#1898e0", "#8bcd45", "#f89e13", "#d24493",
                  "#00b2ed", "#dbe622", "#fb4c27", "#8f57bf",
                  "#00bb62", "#f9c410", "#fb4865", "#645ccc"]

    if data is not None:
        for i in range(len(data)):
            color = color_list[i]
            try:
                mon_data = data[i]
                mon_data.plot(ax=ax, label=data_labels[i], color=color)
            except Exception as e:
                logger.debug(f"Error plotting data: {e}")

    if ref_data is not None:
        try:
            ref_data.plot(ax=ax, label=ref_label, color='black', lw=0.6)
            if std_data is not None:
                std_data.compute()
                ax.fill_between(ref_data.month,
                                ref_data - 2.*std_data,
                                ref_data + 2.*std_data,
                                facecolor='grey', alpha=0.25)
        except Exception as e:
            logger.debug(f"Error plotting std data: {e}")

    ax.legend(fontsize='small')
    ax.set_xticks(monthsNumeric)
    ax.set_xticklabels(monthsNames)

    title = kwargs.get('title', None)
    if title is not None:
        ax.set_title(title)

    return fig, ax
