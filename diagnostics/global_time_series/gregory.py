"""Gregory plot module."""
import matplotlib.pyplot as plt
from aqua import Reader
from aqua.logger import log_configure
from aqua.exceptions import NotEnoughDataError, NoDataError


def plot_gregory(model, exp, source,
                 reader_kw={}, plot_kw={},
                 regrid=None, freq=None,
                 ts_name='2t', toa_name=['mtnlwrf', 'mtnswrf'],
                 loglevel='WARNING', **kwargs):
    """Plot global mean SST against net radiation at TOA.

    Parameters:
        model (str): Model ID.
        exp (str): Experiment ID.
        source (str): Source ID.
        reader_kw (dict): Additional keyword arguments passed to the `aqua.Reader`.
        plot_kw (dict): Additional keyword arguments passed to the plotting function.
        regrid (str): Optional regrid resolution. Default is None.
        freq (str): frequency for timmean applied to data, default is None.
        ts (str): variable name for 2m temperature, default is '2t'.
        toa (list): list of variable names for net radiation at TOA, default is ['mtnlwrf', 'mtnswrf'].
        loglevel (str): Logging level. Default is WARNING.

    Raises:
        NotEnoughDataError: if there are not enough data to plot.
        NoDataError: if the variable is not found.
    """
    logger = log_configure(loglevel, 'Gregory plot')

    retrieve_list = [ts_name] + toa_name
    logger.debug(f"Retrieving {retrieve_list}")

    try:
        reader = Reader(model, exp, source, regrid=regrid, loglevel=loglevel, **reader_kw)
        data = reader.retrieve(var=retrieve_list)
    except Exception as e:
        logger.error(f"Error: {e}")
        raise NoDataError(f"Could not retrieve data for {model}-{exp}. No plot will be drawn.") from e

    if len(data.time) < 2:
        raise NotEnoughDataError("There are not enough data to proceed. Global time series diagnostic requires at least two data points.")

    if freq:
        logger.debug(f"Resampling data to {freq}")
        data = reader.timmean(data=data, freq=freq)
    else:
        logger.warning("No frequency given, be aware that the data is not resampled for the monthly plot.")
    if regrid:
        logger.debug(f"Regridding data to {regrid}")
        data = reader.regrid(data)

    try:
        ts = reader.fldmean(data[ts_name]).values - 273.15
        toa = reader.fldmean(data[toa_name[0]] + data[toa_name[1]]).values
    except Exception as e:
        logger.error(f"Error: {e}")
        raise NoDataError(f"Could not retrieve data for {model}-{exp}. No plot will be drawn.") from e

    # Preparing data for annual mean
    data = reader.timmean(data=data, freq='YS')

    if len(data.time) < 2:
        raise NotEnoughDataError("There are not enough data to proceed with annual Gregory plot.")

    ts_annual = reader.fldmean(data[ts_name]).values - 273.15
    toa_annual = reader.fldmean(data[toa_name[0]] + data[toa_name[1]]).values

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 6))

    # Plot monthly data
    ax1.axhline(0, color="k")
    ax1.plot(ts, toa, marker=".", **plot_kw, label="Monthly Mean", lw=0.5, alpha=0.8)
    ax1.plot(ts[0], toa[0], marker=">", color="tab:green", label="First Time-step")
    ax1.plot(ts[-1], toa[-1], marker="<", color="tab:red", label="Last Time-step")
    ax1.set_xlabel("2m temperature / C")
    ax1.set_ylabel(r"Net radiation TOA / $\rm Wm^{-2}$")
    ax1.grid(True)  # Add grid in the background
    ax1.legend()  # Add legend

    # Plot annual data
    ax2.axhline(0, color="k", lw=0.7)
    ax2.plot(ts_annual, toa_annual, marker=".", color="#000066", lw=1.1, label="Annual Mean")
    ax2.plot(ts_annual[0], toa_annual[0], marker=">", color="tab:green")
    ax2.plot(ts_annual[-1], toa_annual[-1], marker="<", color="tab:red")
    ax2.set_xlabel("2m temperature / C")
    ax2.grid(True)  # Add grid in the background
    ax2.legend()  # Add legend

    plt.tight_layout()  # Adjust spacing between subplots

    # Add model and experiment to title
    title = "Gregory plot"
    title += f"\n{model} {exp}"

    plt.subplots_adjust(top=0.85)
    fig.suptitle(title)

    return fig
