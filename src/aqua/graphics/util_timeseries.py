"""Functions to plot monthly and annual data, as well as reference data."""
import xarray as xr


def plot_monthly_data(ax, monthly_data, data_labels, logger, lw=1.5):
    """
    Plot monthly data on the given axis.

    Args:
        ax (matplotlib.axes._subplots.AxesSubplot): axis object to plot the data on
        monthly_data (xr.DataArray or list of xr.DataArray): monthly data to plot
        data_labels (list): list of labels for the data
        logger (logging.Logger): logger
        lw (float): line width, default is 1.5
    """
    # in case monthly_data is not a list yet, make it a list
    if isinstance(monthly_data, xr.DataArray):
        monthly_data = [monthly_data]
    for i in range(len(monthly_data)):
        try:
            mon_data = monthly_data[i]
            if data_labels:
                label = data_labels[i]
                label += ' monthly'
            else:
                label = None
            mon_data.plot(ax=ax, label=label, lw=lw)
        except Exception as e:
            logger.error(f"Error plotting monthly data: {e}")


def plot_annual_data(ax, annual_data, data_labels, logger, lw=1.5):
    """
    Plot annual data on the given axis.

    Args:
        ax (matplotlib.axes._subplots.AxesSubplot): axis object to plot the data on
        annual_data (list): list of xarray.DataArray objects containing the annual data
        data_labels (list): list of labels for the data
        logger (logging.Logger): logger
        lw (float): line width, default is 1.5
    """
    for i in range(len(annual_data)):
        try:
            ann_data = annual_data[i]
            if data_labels:
                label = data_labels[i]
                label += ' annual'
            else:
                label = None
            ann_data.plot(ax=ax, label=label, color='#1898e0', linestyle='--', lw=lw)
        except Exception as e:
            logger.error(f"Error plotting annual data: {e}")


def plot_ref_monthly_data(ax, ref_monthly_data, std_monthly_data, ref_label, logger, lw=0.8):
    """
    Plot reference monthly data on the given axis.

    Args:
        ax (matplotlib.axes._subplots.AxesSubplot): axis object to plot the data on
        ref_monthly_data (xr.DataArray): reference monthly data to plot
        std_monthly_data (xr.DataArray): standard deviation of the reference monthly data
        ref_label (str): label for the reference data
        logger (logging.Logger): logger
        lw (float): line width, default is 0.6
    """
    try:
        if ref_label:
            ref_label_mon = ref_label + ' monthly'
        else:
            ref_label_mon = None
        ref_monthly_data.plot(ax=ax, label=ref_label_mon, color='black', lw=lw)
        if std_monthly_data is not None:
            std_monthly_data.compute()
            ax.fill_between(ref_monthly_data.time,
                            ref_monthly_data - 2.*std_monthly_data.sel(month=ref_monthly_data["time.month"]),
                            ref_monthly_data + 2.*std_monthly_data.sel(month=ref_monthly_data["time.month"]),
                            facecolor='grey', alpha=0.25)
            ax.set(xlim=(ref_monthly_data.time[0], ref_monthly_data.time[-1]))
    except Exception as e:
        logger.error(f"Error plotting monthly std data: {e}")


def plot_ref_annual_data(ax, ref_annual_data, std_annual_data, ref_label, logger, lw=0.8):
    """
    Plot reference annual data on the given axis.

    Args:
        ax (matplotlib.axes._subplots.AxesSubplot): axis object to plot the data on
        ref_annual_data (xr.DataArray): reference annual data to plot
        std_annual_data (xr.DataArray): standard deviation of the reference annual data
        ref_label (str): label for the reference data
        logger (logging.Logger): logger
        lw (float): line width, default is 0.6
    """
    try:
        if ref_label:
            ref_label_ann = ref_label + ' annual'
        else:
            ref_label_ann = None
        ref_annual_data.plot(ax=ax, label=ref_label_ann, color='black', linestyle='--', lw=lw)
        if std_annual_data is not None:
            std_annual_data.compute()
            ax.fill_between(ref_annual_data.time,
                            ref_annual_data - 2.*std_annual_data,
                            ref_annual_data + 2.*std_annual_data,
                            facecolor='black', alpha=0.2)
    except Exception as e:
        logger.error(f"Error plotting annual std data: {e}")
