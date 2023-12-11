"""Functions for global time series diagnostics.
"""
import matplotlib.pyplot as plt
from aqua import Reader
from aqua.logger import log_configure
from aqua.exceptions import NotEnoughDataError, NoObservationError, NoDataError
from aqua.util import eval_formula

__all__ = [
    "plot_timeseries",
    "plot_gregory",
]


def get_reference_data(varname, model='ERA5', exp='era5', source='monthly',
                       sel=None, resample=None, loglevel='WARNING'):
    """
    Get reference data for a given variable.
    Default is ERA5 monthly data.

    Parameters:
        varname (str): Variable name.
        sel (dict): Optional selection dictionary.
        resample (str): Optional resample rate (e.g. "M").
        loglevel (str): Logging level.

    Returns:
        data (xarray.DataArray): Reference ERA5 data.

    Raises:
        NoObservationError: if no reference data is found.
    """
    logger = log_configure(loglevel, 'plot_timeseries')

    logger.info(f"Reference data: {model}-{exp}-{source}")

    try:
        reader = Reader(model=model, exp=exp, source=source,
                        loglevel=loglevel)
    except Exception as e:
        raise NoObservationError("Could not retrieve ERA5 data. No plot will be drawn.") from e
    data = reader.retrieve()
    std = reader.fldmean(data.sel(time=slice("1991", "2020")).groupby("time.month").std())
    data = data.sel(sel)

    if resample is not None:
        logger.debug(f"Resampling reference data to {resample}")
        data = reader.timmean(data=data, freq=resample)

    try:
        return reader.fldmean(data[varname]), std[varname]
    except KeyError as e:
        try:
            return reader.fldmean(eval_formula(varname, data)), std[varname]
        except KeyError:
            raise NoObservationError(f"Could not retrieve {varname} from ERA5. No plot will be drawn.") from e


def plot_timeseries(
    model,
    exp,
    variable,
    resample=None,
    plot_era5=False,
    ylim={},
    reader_kw={},
    plot_kw={},
    ax=None,
    outfile=None,
    loglevel='WARNING',
    **kwargs,
):
    """
    Plot a time series of the global mean value of a given variable.

    Parameters:
        model (str): Model ID.
        exp (str): Experiment ID.
        variable (str): Variable name.
        resample (str): Optional resample rate (e.g. "M").
        plot_era5 (bool): Include ERA5 reference data.
        ylim (dict): Keyword arguments passed to `set_ylim()`.
        reader_kw (dict): Additional keyword arguments passed to the `aqua.Reader`.
        plot_kw (dict): Additional keyword arguments passed to the plotting function.
        ax (matplotlib.Axes): (Optional) axes to plot in.
        outfile (str): (Optional) output file to store data.

    Raises:
        NotEnoughDataError: if there are not enough data to plot.
        NoDataError: if the variable is not found.
    """

    logger = log_configure(loglevel, 'plot_timeseries')
    if ax is None:
        ax = plt.gca()

    reader = Reader(model, exp, **reader_kw, loglevel=loglevel)
    data = reader.retrieve()

    if variable not in data:
        try:
            data[variable] = eval_formula(variable, data)
        except KeyError:
            logger.error(f"Could not retrieve/derive {variable} for {model}-{exp}")
            raise KeyError(f'{variable} not found. Pick another variable.')
    data = data[variable]

    if len(data.time) < 2:
        raise NotEnoughDataError("There are not enough data to proceed. Global time series diagnostic requires at least two data points.")

    try:
        data = reader.fldmean(data)
    except KeyError as e:
        raise NoDataError(f"Could not retrieve {variable} from {model}-{exp}. No plot will be drawn.") from e

    if resample is not None:
        logger.debug(f"Resampling data to {resample}")
        data = reader.timmean(data=data, freq=resample)

    # If no label in plot_kw, use {model}-{exp}
    if "label" not in plot_kw:
        logger.debug(f"Using {model}-{exp} as label")
        plot_kw["label"] = f"{model}-{exp}"

    data.plot(**plot_kw, ax=ax)
    ax.set_title(f'Globally averaged {variable}')

    if outfile is not None:
        logger.debug(f"Saving data to {outfile}")
        data.to_netcdf(outfile)

    if plot_era5:
        eradata, erastd = get_reference_data(
            variable,
            sel={"time": slice(data.time.min(), data.time.max())},
            resample=resample, loglevel=loglevel
        )
        if eradata is not None:
            eradata.compute()
            erastd.compute()
            ax.fill_between(
                eradata.time,
                eradata - erastd.sel(month=eradata["time.month"]),
                eradata + erastd.sel(month=eradata["time.month"]),
                facecolor="grey",
                alpha=0.3678,
                color="grey",
                label="ERA5",
            )

    ax.legend()
    ax.set_ylim(**ylim)
    ax.grid(axis="x", color="k")
    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)


def plot_gregory(model, exp, reader_kw={}, plot_kw={}, ax=None, freq='M',
                 **kwargs):
    """Plot global mean SST against net radiation at TOA.

    Parameters:
        model (str): Model ID.
        exp (str): Experiment ID.
        reader_kw (dict): Additional keyword arguments passed to the `aqua.Reader`.
        plot_kw (dict): Additional keyword arguments passed to the plotting function.
        freq (str): frequency for timmean applied to data, default is 'M' (monthly)

    Raises:
        NotEnoughDataError: if there are not enough data to plot.
        NoDataError: if the variable is not found.
    """
    if ax is None:
        ax = plt.gca()

    try:
        reader = Reader(model, exp, **reader_kw)
        data = reader.retrieve()
    except Exception as e:
        raise NoDataError(f"Could not retrieve data for {model}-{exp}. No plot will be drawn.") from e

    if len(data.time) < 2:
        raise NotEnoughDataError("There are not enough data to proceed. Global time series diagnostic requires at least two data points.")

    try:
        ts = reader.timmean(data=reader.fldmean(data["2t"]), freq=freq).values - 273.15
        toa = reader.timmean(data=reader.fldmean(data["mtnsrf"] + data["mtntrf"]),
                             freq=freq).values
    except KeyError:
        raise NoDataError(f"Could not retrieve data for {model}-{exp}. No plot will be drawn.") from e

    ax.axhline(0, color="k", lw=0.8)
    lh, = ax.plot(ts, toa, marker=".", **plot_kw)
    ax.plot(ts[0], toa[0], marker=">", color="tab:green")
    ax.plot(ts[-1], toa[-1], marker="<", color="tab:red")
    ax.set_xlabel("2m temperature / C")
    ax.set_ylabel(r"Net radiation TOA / $\rm Wm^{-2}$")

    # Add model and experiment to title
    title = "Gregory plot"
    title += f"\n{model} {exp}"
    ax.set_title(title)
