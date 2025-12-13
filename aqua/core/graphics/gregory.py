from matplotlib import rcParams
import matplotlib.pyplot as plt
import xarray as xr
from aqua.core.logger import log_configure
from aqua.core.util import evaluate_colorbar_limits, to_list, unit_to_latex
from .styles import ConfigStyle


def plot_gregory_monthly(t2m_monthly_data, net_toa_monthly_data,
                         t2m_monthly_ref: xr.DataArray = None,
                         net_toa_monthly_ref: xr.DataArray = None,
                         fig: plt.Figure = None, ax: plt.Axes = None,
                         set_axis_limits: bool = True,
                         labels: list = None, ref_label: str = None,
                         xlabel: str = None,
                         ylabel: str = None,
                         title: str = 'Monthly Mean',
                         style: str = None, loglevel: str = 'WARNING'):
    """"
    Plot a Gregory plot for monthly data.

    Args:
        t2m_monthly_data (list): List of 2 m temperature data for each month.
        net_toa_monthly_data (list): List of net radiation TOA data for each month.
        t2m_monthly_ref (xr.DataArray, optional): Reference 2 m temperature data.
        net_toa_monthly_ref (xr.DataArray, optional): Reference net radiation TOA data.
        fig (plt.Figure, optional): Figure object to plot on.
        ax (plt.Axes, optional): Axes object to plot on.
        set_axis_limits (bool, optional): Whether to set axis limits. Defaults to True.
        labels (list, optional): List of labels for each month.
        ref_label (str, optional): Label for the reference data.
        title (str, optional): Title of the plot. Not used if None
        style (str, optional): Style for the plot. Defaults is the AQUA default style.
        loglevel (str, optional): Log level for logging. Defaults to 'WARNING'.

    Returns:
        tuple: Figure and Axes objects.
    """
    logger = log_configure(loglevel, 'plot_gregory_monthly')
    ConfigStyle(style=style, loglevel=loglevel)
    rcParams['text.usetex'] = False  # Disable LaTeX rendering for speed

    # We load the data for speed
    t2m_monthly_data = to_list(t2m_monthly_data)
    net_toa_monthly_data = to_list(net_toa_monthly_data)
    t2m_monthly_data = [t2m_monthly_data[i].load() for i in range(len(t2m_monthly_data))]
    net_toa_monthly_data = [net_toa_monthly_data[i].load() for i in range(len(net_toa_monthly_data))]
    t2m_monthly_ref = t2m_monthly_ref.load() if t2m_monthly_ref is not None else None
    net_toa_monthly_ref = net_toa_monthly_ref.load() if net_toa_monthly_ref is not None else None

    labels = to_list(labels) if labels else [None for _ in range(len(t2m_monthly_data))]

    if fig is None and ax is None:
        logger.debug("Creating new figure and axis")
        fig, ax = plt.subplots(1, 1, figsize=(6, 6))

    if xlabel is None:
        xlabel = '2 m Temperature [°C]'
    if ylabel is None:
        ylabel = r"Net radiation TOA [W m$^{-2}$]"

    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.axhline(0, color="k")
    ax.grid(True)

    ref = t2m_monthly_ref is not None and net_toa_monthly_ref is not None

    # Create a cycle that is the average of the available ref data
    if ref:
        t2m_ref = t2m_monthly_ref.groupby('time.month').mean(dim='time')
        net_toa_ref = net_toa_monthly_ref.groupby('time.month').mean(dim='time')
        # Add an extra point same as the first one to close the loop
        t2m_ref = xr.concat([t2m_ref, t2m_ref.isel(month=0)], dim='month', coords='different', compat='equals')
        net_toa_ref = xr.concat([net_toa_ref, net_toa_ref.isel(month=0)], dim='month', coords='different', compat='equals')

    if set_axis_limits:
        # We set a fixed x and y range but then we expand it if data
        # goes beyond the limits
        t2m_list = t2m_monthly_data + to_list(t2m_ref) if ref else t2m_monthly_data
        t2m_min, t2m_max = evaluate_colorbar_limits(t2m_list, sym=False)
        t2m_min = min(t2m_min, min(t2m_ref.values)) if ref else 11.5
        t2m_max = max(t2m_max, max(t2m_ref.values)) if ref else 16.5
        t2m_min = min(t2m_min, 11.5)
        t2m_min = t2m_min - 0.5
        t2m_max = max(t2m_max, 16.5)
        t2m_max = t2m_max + 0.5

        net_toa_list = net_toa_monthly_data + to_list(net_toa_ref) if ref else net_toa_monthly_data
        toa_min, toa_max = evaluate_colorbar_limits(net_toa_list, sym=False)
        toa_min = min(toa_min, min(net_toa_ref.values)) if ref else -11.5
        toa_max = max(toa_max, max(net_toa_ref.values)) if ref else 11.5
        toa_min = min(toa_min, -11.5)
        toa_min = toa_min - 0.5
        toa_max = max(toa_max, 11.5)
        toa_max = toa_max + 0.5

        ax.set_xbound(t2m_min, t2m_max)
        ax.set_ybound(toa_min, toa_max)
        ax.set_xlim(t2m_min, t2m_max)
        ax.set_ylim(toa_min, toa_max)

        logger.debug(f"Monthly x-axis limits: {t2m_min} to {t2m_max}")
        logger.debug(f"Monthly y-axis limits: {toa_min} to {toa_max}")

    for i, (t2m_monthly, net_toa_monthly) in enumerate(zip(t2m_monthly_data, net_toa_monthly_data)):
        ax.plot(t2m_monthly, net_toa_monthly, label=labels[i], marker='o')
    if ref:
        ax.plot(t2m_ref, net_toa_ref, label=ref_label, marker='o', color='black', zorder=3)
        ax.scatter(t2m_ref, net_toa_ref, color='black', s=150, zorder=3)

        # Optimized text rendering
        for m, x, y in zip(range(1, 13), t2m_ref.values[:-1], net_toa_ref.values[:-1]):
            ax.annotate(str(m), (x, y), color='white', fontsize=8, ha='center',
                        va='center', fontweight='bold', zorder=4)

    return fig, ax


def plot_gregory_annual(t2m_annual_data, net_toa_annual_data,
                        t2m_annual_ref: xr.DataArray = None,
                        net_toa_annual_ref: xr.DataArray = None,
                        t2m_std: xr.DataArray = None,
                        net_toa_std: xr.DataArray = None,
                        fig: plt.Figure = None, ax: plt.Axes = None,
                        set_axis_limits: bool = True,
                        labels: list = None,
                        xlabel: str = None,
                        ylabel: str = None,
                        title: str = 'Annual Mean',
                        style: str = None, loglevel: str = 'WARNING'):
    """
    Plot a Gregory plot for annual data.

    Args:
        t2m_annual_data (list): List of 2 m temperature data for each year.
        net_toa_annual_data (list): List of net radiation TOA data for each year.
        t2m_annual_ref (xr.DataArray, optional): Reference 2 m temperature data.
        net_toa_annual_ref (xr.DataArray, optional): Reference net radiation TOA data.
        t2m_std (xr.DataArray, optional): Standard deviation of 2 m temperature data.
        net_toa_std (xr.DataArray, optional): Standard deviation of net radiation TOA data.
        fig (plt.Figure, optional): Figure object to plot on.
        ax (plt.Axes, optional): Axes object to plot on.
        set_axis_limits (bool, optional): Whether to set axis limits. Defaults to True.
        labels (list, optional): List of labels for each year.
        ref_label (str, optional): Label for the reference data.
        xlabel (str, optional): Title of the x-axis. Defaults to '2 m Temperature [°C]'.
        ylabel (str, optional): Title of the y-axis. Defaults to "Net radiation TOA [W/m^2]".
        title (str, optional): Title of the plot. Not used if None
        style (str, optional): Style for the plot. Defaults is the AQUA default style.
        loglevel (str, optional): Log level for logging. Defaults to 'WARNING'.

    Returns:
        tuple: Figure and Axes objects.
    """
    logger = log_configure(loglevel, 'plot_gregory_annual')
    ConfigStyle(style=style, loglevel=loglevel)

    labels = to_list(labels) if labels else [None for _ in range(len(t2m_annual_data))]

    # We load the data for speed
    t2m_annual_data = to_list(t2m_annual_data)
    net_toa_annual_data = to_list(net_toa_annual_data)
    t2m_annual_data = [t2m_annual_data[i].load() for i in range(len(t2m_annual_data))]
    net_toa_annual_data = [net_toa_annual_data[i].load() for i in range(len(net_toa_annual_data))]
    t2m_annual_ref = t2m_annual_ref.load() if t2m_annual_ref is not None else None
    net_toa_annual_ref = net_toa_annual_ref.load() if net_toa_annual_ref is not None else None
    t2m_std = t2m_std.load() if t2m_std is not None else None
    net_toa_std = net_toa_std.load() if net_toa_std is not None else None

    if fig is None and ax is None:
        logger.debug("Creating new figure and axis")
        fig, ax = plt.subplots(1, 1, figsize=(6, 6))

    if xlabel is None:
        xlabel = '2 m Temperature [°C]'
    if ylabel is None:
        ylabel = r"Net radiation TOA [W m$^{-2}$]"

    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.axhline(0, color="k")
    ax.grid(True)

    ref = t2m_std is not None and net_toa_std is not None

    if set_axis_limits:
        # We set a fixed x and y range but then we expand it if data
        # goes beyond the limits
        t2m_min, t2m_max = evaluate_colorbar_limits(t2m_annual_data, sym=False)
        t2m_min = min(t2m_min, 13.5)
        t2m_min = t2m_min - 0.5
        t2m_max = max(t2m_max, 14.5)
        t2m_max = t2m_max + 0.5

        toa_min, toa_max = evaluate_colorbar_limits(net_toa_annual_data, sym=False)
        toa_min = min(toa_min, -1.5)
        toa_min = toa_min - 0.5
        toa_max = max(toa_max, 1.5)
        toa_max = toa_max + 0.5

        ax.set_xbound(t2m_min, t2m_max)
        ax.set_ybound(toa_min, toa_max)
        ax.set_xlim(t2m_min, t2m_max)
        ax.set_ylim(toa_min, toa_max)

        logger.debug(f"Annual x-axis limits: {t2m_min} to {t2m_max}")
        logger.debug(f"Annual y-axis limits: {toa_min} to {toa_max}")

    for i, (t2m_annual, net_toa_annual) in enumerate(zip(t2m_annual_data, net_toa_annual_data)):
        ax.plot(t2m_annual, net_toa_annual, label=labels[i], marker='o')

        # We plot the first and last points with different markers
        ax.plot(t2m_annual[0], net_toa_annual[0], marker=">", color="tab:green")
        ax.plot(t2m_annual[-1], net_toa_annual[-1], marker="<", color="tab:red")
        ax.annotate(str(t2m_annual.time.dt.year[0].values), (t2m_annual[0], net_toa_annual[0]),
                    fontsize=8, ha='right')
        ax.annotate(str(t2m_annual.time.dt.year[-1].values), (t2m_annual[-1], net_toa_annual[-1]),
                    fontsize=8, ha='right')
    if ref:
        t2m_mean = t2m_annual_ref.mean(dim='time')
        net_toa_mean = net_toa_annual_ref.mean(dim='time')
        ax.axhspan(net_toa_mean - net_toa_std, net_toa_mean + net_toa_std,
                   color="lightgreen", alpha=0.3, label=r"1 $\sigma$ band")
        ax.axvspan(t2m_mean - t2m_std, t2m_mean + t2m_std,
                   color="lightgreen", alpha=0.3)

    return fig, ax
