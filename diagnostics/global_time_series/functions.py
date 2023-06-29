"""Functions for global time series diagnostics.
"""
import matplotlib.pyplot as plt

from aqua import Reader


def get_reference_data(varname, sel=None, resample=None):
    """Get ERA5 reference data for a given variable."""
    reader = Reader(model="ERA5", exp="era5", source="monthly")
    data = reader.retrieve().sel(sel)

    if resample is not None:
        data = reader.timmean(data=data, freq=resample)

    try:
        return reader.fldmean(data[varname])
    except KeyError:
        raise KeyError(f"Could not retrieve {varname} from ERA5.")


def plot_timeseries(
    model,
    exp,
    variable,
    resample=None,
    plot_era5=False,
    reader_kw={},
    plot_kw={},
    ax=None,
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
    """
    if ax is None:
        ax = plt.gca()

    reader = Reader(model, exp, **reader_kw)
    data = reader.retrieve()

    data = reader.fldmean(data[variable])

    if resample is not None:
        data = reader.timmean(data=data, freq=resample)

    data.plot(**plot_kw, ax=ax)

    if plot_era5:
        get_reference_data(
            variable,
            sel={"time": slice(data.time.min(), data.time.max())},
            resample=resample,
        ).plot(color="grey", label="ERA5", ax=ax)
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

    ax.axhline(0, color="k", lw=0.8)
    ax.plot(
        reader.fldmean(data["2t"].resample(time="M").mean()) - 273.15,
        reader.fldmean((data["mtnsrf"] + data["mtntrf"]).resample(time="M").mean()),
        marker="o",
        **plot_kw,
    )
    ax.set_xlabel("2m temperature / C")
    ax.set_ylabel(r"Net radiation TOA / $\rm Wm^{-2}$")
