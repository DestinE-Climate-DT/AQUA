"""Functions for global time series diagnostics.
"""
import matplotlib.pyplot as plt
from aqua import Reader
from aqua.logger import log_configure
from aqua.exceptions import NotEnoughDataError, NoObservationError, NoDataError
from aqua.util import eval_formula

__all__ = [
    "plot_timeseries"
]


def get_reference_data(varname, formula=False, model='ERA5', exp='era5', source='monthly',
                       startdate=None, enddate=None,
                       std_startdate="1991-01-01", std_enddate="2020-12-31",
                       resample=None, regrid=None, loglevel='WARNING'):
    """
    Get reference data for a given variable.
    Default is ERA5 monthly data.

    Parameters:
        varname (str or list): Variable(s) name to retrieve.
        formula (bool, opt): If True, try to derive the variable from other variables.
        model (str, opt): Model ID. Default is ERA5.
        exp (str, opt): Experiment ID. Default is era5.
        source (str, opt): Source ID. Default is monthly.
        startdate (str, opt): Start date. Default is None.
        enddate (str, opt): End date. Default is None.
        std_startdate (str, opt): Start date for standard deviation. Default is "1991-01-01".
        std_enddate (str, opt): End date for standard deviation. Default is "2020-12-31".
        resample (str, opt): Resample rate (e.g. "M"). Default is None.
        regrid (str, opt): Regrid resolution. Default is None.
        loglevel (str, opt): Logging level. Default is WARNING.

    Returns:
        data (xarray.DataArray): Reference data.

    Raises:
        NoObservationError: if no reference data is found.
    """
    logger = log_configure(loglevel, 'get_reference_data')

    logger.debug(f"Reference data: {model} {exp} {source}")

    try:  # Retrieving the entire timespan since we need two periods for standard deviation and time series
        reader = Reader(model=model, exp=exp, source=source, regrid=regrid, loglevel=loglevel)
    except Exception as e:
        raise NoObservationError("Could not retrieve reference data. No plot will be drawn.") from e

    if formula:  # We retrieve all variables
        data = reader.retrieve()
    else:
        data = reader.retrieve(var=varname)

    # Standard deviation evaluation
    logger.info(f"Computing standard deviation from {std_startdate} to {std_enddate}")
    if formula:
        std = reader.fldmean(eval_formula(varname,
                                          data.sel(time=slice(std_startdate, std_enddate)))).groupby("time.month").std()
    else:
        std = reader.fldmean(data.sel(time=slice(std_startdate, std_enddate))).groupby("time.month").std()

    if startdate is not None and enddate is not None:
        data = data.sel(time=slice(startdate, enddate))
    else:
        logger.info("No start and end date given. Retrieving all data.")

    if resample:
        logger.debug(f"Resampling reference data to {resample}")
        data = reader.timmean(data=data, freq=resample)

    if regrid:
        logger.debug(f"Regridding reference data to {regrid}")
        data = reader.regrid(data)

    try:
        if formula:
            return reader.fldmean(eval_formula(varname, data)), std
        else:
            return reader.fldmean(data[varname]), std[varname]
    except KeyError as e:
        logger.error(f"Error: {e}")
        raise NoObservationError(f"Could not retrieve {varname} from reference data. No plot will be drawn.") from e


def plot_timeseries(
    model,
    exp,
    source,
    variable,
    formula=False,
    resample=None,
    regrid=None,
    plot_era5=True,
    annual=False,
    startdate=None,
    enddate=None,
    std_startdate="1991-01-01",
    std_enddate="2020-12-31",
    ylim={},
    reader_kw={},
    plot_kw={},
    ax=None,
    outfile=None,
    loglevel='WARNING',
    variable_ext=None,
    variable_ext_name=None,
    variable_ext_units=None,
    label_ext=None,
    **kwargs,
):
    """
    Plot a time series of the global mean value of a given variable.
    By default all the time series are plotted, the annual mean is plotted as dashed line
    and the standard deviation is computed for the period 1991-2020.

    Parameters:
        model (str): Model ID.
        exp (str): Experiment ID.
        source (str): Source ID.
        variable (str): Variable name.
        formula (bool): (Optional) If True, try to derive the variable from other variables.
        resample (str): Optional resample rate (e.g. "M").
        regrid (str): Optional regrid resolution. Default is None.
        plot_era5 (bool): Include ERA5 reference data. Default is True.
        annual (bool): Plot annual mean. Default is False.
        startdate (str): Start date. Default is None.
        enddate (str): End date. Default is None.
        std_startdate (str): Start date for standard deviation. Default is "1991-01-01".
        std_enddate (str): End date for standard deviation. Default is "2020-12-31".
        ylim (dict): Keyword arguments passed to `set_ylim()`.
        reader_kw (dict): Additional keyword arguments passed to the `aqua.Reader`.
        plot_kw (dict): Additional keyword arguments passed to the plotting function.
        ax (matplotlib.Axes): (Optional) axes to plot in.
        outfile (str): (Optional) output file to store data.
        loglevel (str): Logging level. Default is WARNING.
        variable_ext (str): Additional variable to plot directly without retrieval.

    Raises:
        NotEnoughDataError: if there are not enough data to plot.
        NoDataError: if the variable is not found.
    """
    logger = log_configure(loglevel, 'Plot timeseries')
    if ax is None:
        ax = plt.gca()

    try:
        reader = Reader(model, exp, source, regrid=regrid, startdate=startdate, enddate=enddate,
                        **reader_kw, loglevel=loglevel)
    except Exception as e:
        logger.error(f"Error: {e}")
        raise NoDataError(f"Could not retrieve data for {model} {exp} {source}. No plot will be drawn.") from e


    if formula:
        data = reader.retrieve()
        logger.debug(f"Deriving {variable} from other variables")
        try:
            data[variable] = eval_formula(variable, data)
        except KeyError:
            raise KeyError(f'{variable} not possible to evaluate.')
    else:
        try:
            data = reader.retrieve(var=variable)
        except KeyError as e:
            raise NoDataError(f"Could not retrieve {variable} from {model} {exp} {source}. No plot will be drawn.") from e

    data = data[variable]
    data = reader.fldmean(data)

    if variable_ext is not None:
        data_ext = variable_ext
        data_ext = reader.fldmean(data_ext)

    if len(data.time) < 2:
        raise NotEnoughDataError("There are not enough data to proceed. Global time series diagnostic requires at least two data points.")

    if resample:
        logger.debug(f"Resampling data to {resample}")
        data = reader.timmean(data=data, freq=resample)

        if resample == 'YS' and annual:
            logger.debug("Asked for annual mean, but resample is already annual. Skipping.")
            annual = False

    if regrid:
        logger.debug(f"Regridding data to {regrid}")
        data = reader.regrid(data)

    # If no label in plot_kw, use {model}-{exp}
    if "label" not in plot_kw:
        logger.debug(f"Using {model} {exp} monthly as label")
        plot_kw["label"] = f"{model} {exp} monthly mean"

    data.plot(**plot_kw, ax=ax)

    # in case an additional external variable is passed to the timeseries function
    # plot it with its own axis and units

    if variable_ext is not None:
        # Plot data on the original y-axis
        data.plot(ax=ax, label=label_ext)

        # Create a twin Axes object
        ax_ext = ax.twinx()

        # Plot variable_ext data on the twin Axes object
        data_ext.plot(ax=ax_ext)

        # Set y-axis label for the twin Axes object with its own units
        ax_ext.set_ylabel(variable_ext_units)

    if annual:
        data_annual = reader.timmean(data=data, freq='YS', center_time=True)
        logger.debug(f"Using {model} {exp} annual mean as label")
        plot_kw["label"] = f"{model} {exp} annual mean"
        data_annual.plot(**plot_kw, ax=ax, linestyle='--')

    if outfile is not None:
        # TODO: save also annual mean, std, etc.
        logger.debug(f"Saving data to {outfile}")
        data.to_netcdf(outfile)

    if plot_era5:
        # Getting start and end date from data if not given
        if startdate is None:
            startdate = data.time[0].values.astype(str)
        if enddate is None:
            enddate = data.time[-1].values.astype(str)
        logger.debug(f"Plotting reference data from {startdate} to {enddate}")

        try:
            eradata, erastd = get_reference_data(
                variable, formula=formula,
                resample=resample, regrid=regrid,
                startdate=startdate, enddate=enddate,
                std_startdate=std_startdate, std_enddate=std_enddate,
                loglevel=loglevel
            )
            logger.debug("Reference data retrieved.")
        except NoObservationError as e:
            logger.warning(f"Warning: {e}")
            logger.warning("Skipping reference data.")
            eradata = None

        if eradata is not None:
            eradata.compute()
            erastd.compute()
            ax.fill_between(
                eradata.time,
                eradata - erastd.sel(month=eradata["time.month"]),
                eradata + erastd.sel(month=eradata["time.month"]),
                facecolor="grey",
                alpha=0.35,
                color="grey"
            )
            eradata.plot(ax=ax, color="k", lw=0.5, label="ERA5 monthly mean")

            if annual:
                eradata_annual = reader.timmean(data=eradata, freq='YS', center_time=True)
                eradata_annual.plot(ax=ax, color="k", lw=0.5, linestyle='--', label="ERA5 annual mean")

    if variable_ext is not None:
        ax.set_title(f'Globally averaged {variable} and {variable_ext_name} timeseries')
    else:
        ax.set_title(f'Globally averaged {variable} timeseries')

    ax.legend(loc='upper right', fontsize='small')

    ax.set_ylim(**ylim)
    ax.grid(axis="x", color="k")
    ax.spines["right"].set_visible(False)
    ax.spines["top"].set_visible(False)
