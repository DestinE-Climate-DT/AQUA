import matplotlib.pyplot as plt
from aqua.logger import log_configure
from aqua.util import evaluate_colorbar_limits


def plot_gregory_monthly(monthly_data_2t, monthly_data_net_toa,
                         monthly_ref_2t, monthly_ref_net_toa,
                         fig: plt.Figure = None, ax: plt.Axes = None,
                         set_axis_limits: bool = True, legend: bool = True,
                         return_fig: bool = True, loglevel: str = 'WARNING'):

    logger = log_configure(loglevel, 'plot_gregory_monthly')
    if fig is None and ax is None:
        fig, ax = plt.subplots(1, 1, figsize=(6, 6))

    if set_axis_limits:
        # We set a fixed x and y range but then we expand it if data
        # goes beyond the limits
        t2m_min, t2m_max = evaluate_colorbar_limits(monthly_data_2t.append(monthly_ref_2t), sym=False)
        t2m_min = min(t2m_min, 10.5)
        t2m_max = max(t2m_max, 16)

        toa_min, toa_max = evaluate_colorbar_limits(monthly_data_net_toa.append(monthly_ref_net_toa), sym=False)
        toa_min = min(toa_min, -11.5)
        toa_max = max(toa_max, 11.5)

        ax.set_xlim(t2m_min+0.5, t2m_max-0.5)
        ax.set_ylim(toa_min+0.5, toa_max-0.5)

        logger.debug(f"Monthly x-axis limits: {t2m_min} to {t2m_max}")
        logger.debug(f"Monthly y-axis limits: {toa_min} to {toa_max}")

    if legend:
        ax.legend()

    if return_fig:
        return fig, ax
