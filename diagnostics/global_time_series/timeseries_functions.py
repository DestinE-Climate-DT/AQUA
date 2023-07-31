"""Functions for global time series diagnostics.
"""
import matplotlib.pyplot as plt
from aqua import Reader
from aqua.logger import log_configure

__all__ = [
    "plot_timeseries",
    "plot_gregory",
]

def get_reference_data(varname, sel=None, resample=None, loglevel='WARNING'):
    """Get ERA5 reference data for a given variable."""
    logger = log_configure(loglevel, 'plot_timeseries')
    reader = Reader(model="ERA5", exp="era5", source="monthly")
    data = reader.retrieve().sel(sel)

    if resample is not None:
        data = reader.timmean(data=data, freq=resample)

    try:
        return reader.fldmean(data[varname])
    except KeyError:
        logger.error(f"Could not retrieve {varname} from ERA5. No plot will be drawn.")
        return None


def plot_timeseries(
    model,
    exp,
    variable,
    resample=None,
    plot_era5=False,
    reader_kw={},
    plot_kw={},
    ax=None,
    outfile=None,
    loglevel='WARNING',
    **kwargs,
):
    """Plot a time series of the global mean value of a given variable.

    Parameters:
        model (str): Model ID.
        exp (str): Experiment ID.
        variable (str): Variable name.
        resample (str): Optional resample rate (e.g. "M").
        plot_era5 (bool): Include ERA5 reference data.
        reader_kw (dict): Additional keyword arguments passed to the `aqua.Reader`.
        plot_kw (dict): Additional keyword arguments passed to the plotting function.
        ax (matplotlib.Axes): (Optional) axes to plot in.
        outfile (str): (Optional) output file to store data.
    """

    logger = log_configure(loglevel, 'plot_timeseries')
    if ax is None:
        ax = plt.gca()

    reader = Reader(model, exp, **reader_kw)
    try:
        data = reader.retrieve(var=variable)
    except KeyError:
        logger.error(f"Could not retrieve {variable} for {model}-{exp}")
        raise KeyError(f'{variable} not found. Pick another variable.')

    data = reader.fldmean(data[variable])

    if resample is not None:
        data = reader.timmean(data=data, freq=resample)

    data.plot(**plot_kw, ax=ax)
    ax.set_title(f'Globally averaged {variable}')

    if outfile is not None:
        data.to_netcdf(outfile)

    if plot_era5:
        eradata = get_reference_data(
            variable,
            sel={"time": slice(data.time.min(), data.time.max())},
            resample=resample, loglevel=loglevel
        )
        if eradata is not None:
            eradata.plot(color="grey", label="ERA5", ax=ax)
    ax.legend()


def plot_gregory(model, exp, reader_kw={}, plot_kw={}, ax=None, **kwargs):
    """Plot global mean SST against net radiation at TOA.

    Parameters:
        model (str): Model ID.
        exp (str): Experiment ID.
        reader_kw (dict): Additional keyword arguments passed to the `aqua.Reader`.
        plot_kw (dict): Additional keyword arguments passed to the plotting function.
    """
    if ax is None:
        ax = plt.gca()

    reader = Reader(model, exp, **reader_kw)
    data = reader.retrieve()

    ts = reader.timmean(data=reader.fldmean(data["2t"]), freq="M").values - 273.15
    toa = reader.timmean(data=reader.fldmean(data["mtnsrf"] + data["mtntrf"]), freq="M").values

    ax.axhline(0, color="k", lw=0.8)
    lh, = ax.plot(ts, toa, marker=".", **plot_kw)
    ax.plot(ts[0], toa[0], marker=">", color="tab:green")
    ax.plot(ts[-1], toa[-1], marker="<", color="tab:red")
    ax.set_xlabel("2m temperature / C")
    ax.set_ylabel(r"Net radiation TOA / $\rm Wm^{-2}$")
    ax.set_title('Gregory plot')
