from matplotlib import rcParams
import matplotlib.pyplot as plt
import xarray as xr
from aqua.logger import log_configure
from aqua.util import evaluate_colorbar_limits, to_list
from .styles import ConfigStyle


def plot_gregory_monthly(t2m_monthly_data, net_toa_monthly_data,
                         t2m_monthly_ref: xr.DataArray = None,
                         net_toa_monthly_ref: xr.DataArray = None,
                         fig: plt.Figure = None, ax: plt.Axes = None,
                         set_axis_limits: bool = True, legend: bool = True,
                         labels: list = None, ref_label: str = None,
                         xlabel: str = '2 m Temperature [°C]',
                         ylabel: str = "Net radiation TOA [W/m^2]",
                         title: str = 'Monthly Gregory Plot',
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
        legend (bool, optional): Whether to show legend. Defaults to True.
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

    labels = to_list(labels) if labels else [None for _ in range(len(t2m_monthly_data))]

    if fig is None and ax is None:
        logger.debug("Creating new figure and axis")
        fig, ax = plt.subplots(1, 1, figsize=(6, 6))

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
        t2m_ref = xr.concat([t2m_ref, t2m_ref.isel(month=0)], dim='month')
        net_toa_ref = xr.concat([net_toa_ref, net_toa_ref.isel(month=0)], dim='month')

    if set_axis_limits:
        # We set a fixed x and y range but then we expand it if data
        # goes beyond the limits
        t2m_list = to_list(t2m_monthly_data) + to_list(t2m_ref) if ref else to_list(t2m_monthly_data)
        t2m_min, t2m_max = evaluate_colorbar_limits(t2m_list, sym=False)
        t2m_min = min(t2m_min, min(t2m_ref.values))
        t2m_max = max(t2m_max, max(t2m_ref.values))
        t2m_min = min(t2m_min, 11.5)
        t2m_max = max(t2m_max, 16.5)

        net_toa_list = to_list(net_toa_monthly_data) + to_list(net_toa_ref) if ref else to_list(net_toa_monthly_data)
        toa_min, toa_max = evaluate_colorbar_limits(net_toa_list, sym=False)
        toa_min = min(toa_min, min(net_toa_ref.values))
        toa_max = max(toa_max, max(net_toa_ref.values))
        toa_min = min(toa_min, -11.5)
        toa_max = max(toa_max, 11.5)

        ax.set_xbound(t2m_min+0.5, t2m_max+0.5)
        ax.set_ybound(toa_min+0.5, toa_max+0.5)

        logger.debug(f"Monthly x-axis limits: {t2m_min} to {t2m_max}")
        logger.debug(f"Monthly y-axis limits: {toa_min} to {toa_max}")

    for i, (t2m_monthly, net_toa_monthly) in enumerate(zip(t2m_monthly_data, net_toa_monthly_data)):
        ax.plot(t2m_monthly, net_toa_monthly, label=labels[i], marker='o')
    if ref:
        ax.plot(t2m_ref, net_toa_ref, label='Reference', marker='o', color='black', zorder=3)
        ax.scatter(t2m_ref, net_toa_ref, color='black', s=100, zorder=3, label=ref_label)

        # Optimized text rendering
        for m, x, y in zip(range(1, 13), t2m_ref.values[:-1], net_toa_ref.values[:-1]):
            ax.annotate(str(m), (x, y), color='white', fontsize=8, ha='center',
                        va='center', fontweight='bold', zorder=4)

    if legend:
        ax.legend()

    return fig, ax
