"""Gregory plot module."""
import matplotlib.pyplot as plt
from aqua import Reader
from aqua.logger import log_configure
from aqua.exceptions import NotEnoughDataError, NoDataError, NoObservationError
from .reference_data import get_reference_ts_gregory, get_reference_toa_gregory


def plot_gregory(model, exp, source,
                 reader_kw={}, plot_kw={},
                 regrid=None, freq=None,
                 ts_name='2t', toa_name=['mtnlwrf', 'mtnswrf'],
                 ref=True, ts_kw={}, toa_kw={},
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
        ref (bool): If True, reference data is plotted. Default is True.
        ts_kw (dict): Additional keyword arguments passed to the `get_reference_ts_gregory` function.
        toa_kw (dict): Additional keyword arguments passed to the `get_reference_toa_gregory` function.
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
    data = reader.timmean(data=data, freq='YS', exclude_incomplete=True)

    if len(data.time) < 2:
        raise NotEnoughDataError("There are not enough data to proceed with annual Gregory plot.")

    ts_annual = reader.fldmean(data[ts_name]).values - 273.15
    toa_annual = reader.fldmean(data[toa_name[0]] + data[toa_name[1]]).values

    if ref:
        try:
            ref_ts_mean, ref_ts_std = get_reference_ts_gregory(**ts_kw, loglevel=loglevel)
            ref_toa_mean, ref_toa_std = get_reference_toa_gregory(**toa_kw, loglevel=loglevel)
        except NoObservationError as e:
            logger.debug(f"Error: {e}")
            logger.error("No reference data available. No reference plot will be drawn.")
            ref = False

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 6))

    # Plot monthly data
    ax1.axhline(0, color="k")
    ax1.set_ylim(-12., 12.)  # W/m^2
    ax1.plot(ts, toa, marker=".", **plot_kw, label="Monthly Mean", lw=0.5, alpha=0.8)
    ax1.plot(ts[0], toa[0], marker=">", color="tab:green", label="First Time-step")
    ax1.plot(ts[-1], toa[-1], marker="<", color="tab:red", label="Last Time-step")
    ax1.set_xlabel("2m temperature [C]")
    ax1.set_ylabel(r"Net radiation TOA [$\rm Wm^{-2}$]")
    ax1.grid(True)  # Add grid in the background
    ax1.legend()  # Add legend

    # Plot annual data
    ax2.set_ylim(-1.5, 1.5)  # W/m^2
    ax2.axhline(0, color="k", lw=0.7)
    ax2.plot(ts_annual, toa_annual, marker=".", color="#000066", lw=1.1, label="Annual Mean")
    ax2.plot(ts_annual[0], toa_annual[0], marker=">", color="tab:green")
    ax2.plot(ts_annual[-1], toa_annual[-1], marker="<", color="tab:red")
    # add year to the first and last time-step
    ax2.text(ts_annual[0], toa_annual[0], str(data.time.dt.year[0].values), fontsize=8, ha='right')
    ax2.text(ts_annual[-1], toa_annual[-1], str(data.time.dt.year[-1].values), fontsize=8, ha='right')
    ax2.set_xlabel("2m temperature [C]")
    ax2.grid(True)  # Add grid in the background

    if ref:
        # Convert to Celsius
        logger.debug("Converting reference data to Celsius")
        ref_ts_mean -= 273.15
        logger.debug(f"Reference data: ts_mean={ref_ts_mean}, ts_std={ref_ts_std}")
        logger.debug(f"Reference data: toa_mean={ref_toa_mean}, toa_std={ref_toa_std}")

        # Fill with a horizontal band between the mean and the mean +/- sigma of TOA
        ax2.axhspan(ref_toa_mean - ref_toa_std, ref_toa_mean + ref_toa_std,
                    color="lightgreen", alpha=0.3, label=r"$\sigma$ band")
        # Fill with a vertical band between the mean and the mean +/- 2 sigma of 2m temperature
        ax2.axvspan(ref_ts_mean - ref_ts_std, ref_ts_mean + ref_ts_std,
                    color="lightgreen", alpha=0.3)

    ax2.legend()  # Add legend
    plt.tight_layout()  # Adjust spacing between subplots

    # Add model and experiment to title
    title = "Gregory plot"
    title += f" {model} {exp}"
    title += f" ({data.time.dt.year[0].values}-{data.time.dt.year[-1].values})"

    if ref:
        model_ts = ts_kw.get('model', 'ERA5')
        model_toa = toa_kw.get('model', 'CERES')
        if model_ts == model_toa:
            title += f"\nReference: {model_ts}"
            start_ts = ts_kw.get('startdate', '1980')
            end_ts = ts_kw.get('enddate', '2010')
            title += f" {start_ts}-{end_ts}"
        else:  # Different models for 2m temperature and Net radiation TOA
            title += f"\nReference: {model_ts} for 2m temperature"
            start_ts = ts_kw.get('startdate', '1980')
            end_ts = ts_kw.get('enddate', '2010')
            title += f" {start_ts}-{end_ts}"
            title += f" and {model_toa} for Net radiation TOA"
            start_toa = toa_kw.get('startdate', '2001')
            end_toa = toa_kw.get('enddate', '2020')
            title += f" {start_toa}-{end_toa}"

    plt.subplots_adjust(top=0.85)
    fig.suptitle(title)

    return fig
