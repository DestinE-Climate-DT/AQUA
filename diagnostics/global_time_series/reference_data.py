"""
Functions to retrieve reference data for global time series diagnostics.
"""
from aqua import Reader
from aqua.logger import log_configure
from aqua.exceptions import NoObservationError
from aqua.util import eval_formula


def get_reference_ts_gregory(ts_name='2t', ts_ref={'model': 'ERA5', 'exp': 'era5', 'source': 'monthly'},
                             startdate='1980-01-01', enddate='2010-12-31', loglevel='WARNING'):
    """Retrieve ts reference data for Gregory plot.

    Parameters:
        ts_name (str): variable name for 2m temperature, default is '2t'.
        ts_ref (dict): dictionary with model, exp and source for 2m temperature,
                       default is {'model': 'ERA5', 'exp': 'era5', 'source': 'monthly'}.
        startdate (str): start date for reference data, default is '1980-01-01'.
        enddate (str): end date for reference data, default is '2010-12-31'.
        loglevel (str): Logging level. Default is WARNING.

    Returns:
        ts_mean (float): mean of 2m temperature for reference data.
        ts_std (float): standard deviation of 2m temperature for reference data.

    Raises:
        NoObservationError: if no reference data is available.
    """
    logger = log_configure(loglevel, 'Ref ts Gregory')

    if startdate is not None or enddate is not None:
        logger.info(f"Reference data will be retrieved rom {startdate} to {enddate}.")
    else:
        logger.debug("Reference data will be retrieved for the full period.")
    logger.debug(f"Retrieving reference data for {ts_name} from {ts_ref['model']} {ts_ref['exp']} {ts_ref['source']}.")

    try:
        reader_ts = Reader(ts_ref['model'], ts_ref['exp'], ts_ref['source'],
                           startdate=startdate, enddate=enddate, loglevel=loglevel)

        data_ts = reader_ts.retrieve(var=ts_name)
        data_ts = data_ts[ts_name]
        data_ts = reader_ts.timmean(data_ts, freq='YS', exclude_incomplete=True)
        data_ts = reader_ts.fldmean(data_ts)

        logger.debug("Evaluating 2m temperature")

        ts_std = data_ts.std().values
        ts_mean = data_ts.mean().values

        logger.debug(f"Mean: {ts_mean}, Std: {ts_std}")
    except Exception as e:
        logger.debug(f"Error: {e}")
        logger.error(f"Failed to retrieve {ts_name} from {ts_ref['model']} {ts_ref['exp']}.")
        raise NoObservationError("No reference data available.") from e

    return ts_mean, ts_std


def get_reference_toa_gregory(toa_name=['mtnlwrf', 'mtnswrf'],
                              toa_ref={'model': 'CERES', 'exp': 'ebaf-toa42', 'source': 'monthly'},
                              startdate='2001-01-01', enddate='2020-12-31', loglevel='WARNING'):
    """Retrieve toa reference data for Gregory plot.

    Parameters:
        toa_name (list): list of variable names for net radiation at TOA, default is ['mtnlwrf', 'mtnswrf'].
        toa_ref (dict): dictionary with model, exp and source for net radiation at TOA,
                        default is {'model': 'CERES', 'exp': 'ebaf-toa42', 'source': 'monthly'}.
        startdate (str): start date for reference data, default is '2001-01-01'.
        enddate (str): end date for reference data, default is '2020-12-31'.
        loglevel (str): Logging level. Default is WARNING.

    Returns:
        toa_mean (float): mean of net radiation at TOA for reference data.
        toa_std (float): standard deviation of net radiation at TOA for reference data.

    Raises:
        NoObservationError: if no reference data is available.
    """
    logger = log_configure(loglevel, 'Ref toa Gregory')

    if startdate is not None or enddate is not None:
        logger.info(f"Reference data will be retrieved rom {startdate} to {enddate}.")
    else:
        logger.debug("Reference data will be retrieved for the full period.")
    logger.debug(f"Retrieving reference data for {toa_name} from {toa_ref['model']} {toa_ref['exp']} {toa_ref['source']}.")

    try:
        reader_toa = Reader(toa_ref['model'], toa_ref['exp'], toa_ref['source'],
                            startdate=startdate, enddate=enddate, loglevel=loglevel)
        data_toa = reader_toa.retrieve(var=toa_name)
        data_toa = data_toa[toa_name[0]] + data_toa[toa_name[1]]
        data_toa = reader_toa.timmean(data_toa, freq='YS', exclude_incomplete=True)
        data_toa = reader_toa.fldmean(data_toa)

        logger.debug("Evaluating net radiation at TOA")

        toa_std = data_toa.std().values
        toa_mean = data_toa.mean().values

        logger.debug(f"Mean: {toa_mean}, Std: {toa_std}")
    except Exception as e:
        logger.debug(f"Error: {e}")
        logger.error(f"Failed to retrieve {toa_name} from {toa_ref['model']} {toa_ref['exp']}.")
        raise NoObservationError("No reference data available.") from e

    return toa_mean, toa_std


def get_reference_timeseries(var, formula=False,
                             model='ERA5', exp='era5', source='monthly',
                             startdate=None, enddate=None,
                             std_startdate=None, std_enddate=None,
                             regrid=None,
                             monthly=True, annual=True,
                             monthly_std=True, annual_std=True,
                             loglevel='WARNING'):
    """
    Get reference data for a given variable.
    Default is ERA5 monthly data.
    By default it retrieve monthly and annual mean and standard deviation

    Parameters:
        var (str or list): Variable(s) name to retrieve. List if it is a formula
        formula (bool, opt): If True, try to derive the variable from other variables.
                             Default is False.
        model (str, opt): Model ID. Default is ERA5.
        exp (str, opt): Experiment ID. Default is era5.
        source (str, opt): Source ID. Default is monthly.
        startdate (str, opt): Start date. Default is None.
        enddate (str, opt): End date. Default is None.
        std_startdate (str, opt): Start date for standard deviation.
                                  Default is None.
        std_enddate (str, opt): End date for standard deviation.
                                Default is None.
        regrid (str, opt): Regrid resolution. Default is None.
        monthly (str, opt): Resample to monthly. Default is None.
        annual (bool, opt): Resample to annual. Default is True.
        monthly_std (bool, opt): Compute monthly standard deviation. Default is True.
        annual_std (bool, opt): Compute annual standard deviation. Default is True.
        loglevel (str, opt): Logging level. Default is WARNING.

    Returns:
        tuple: (monthly_data, monthly_std, annual_data, annual_std)

    Raises:
        NoObservationError: if no reference data is found.
    """
    logger = log_configure(loglevel, 'get_reference_data')

    logger.debug(f"Reference data: {model} {exp} {source}")

    start_retrieve, end_retrieve = _start_end_dates(startdate=startdate, enddate=enddate,
                                                    start_std=std_startdate, end_std=std_enddate)
    logger.debug(f"Retrieve data from {start_retrieve} to {end_retrieve}")

    try:  # Retrieving the entire timespan since we need two periods for standard deviation and time series
        reader = Reader(model=model, exp=exp, source=source, regrid=regrid,
                        startdate=start_retrieve, enddate=end_retrieve, loglevel=loglevel)
    except Exception as e:
        raise NoObservationError("Could not retrieve reference data. No plot will be drawn.") from e

    if formula:  # We retrieve all variables
        data = reader.retrieve()
    else:
        data = reader.retrieve(var=var)

    # Monthly data
    if monthly or monthly_std:
        # exclude resample if 'monthly' is in the source name
        if 'monthly' in source or 'mon' in source:
            logger.debug(f"No monthly resample needed for {model} {exp} {source}")
        else:
            data = reader.timmean(data=data, freq='MS', exclude_incomplete=True)

        if monthly:
            data_mon = data.sel(time=slice(startdate, enddate))
            if formula:
                data_mon = reader.fldmean(eval_formula(var, data_mon))
            else:
                data_mon = reader.fldmean(data_mon)
                data_mon = data_mon[var]
        else:
            data_mon = None

        if monthly_std:
            data_mon_std = data.sel(time=slice(std_startdate, std_enddate))
            if formula:
                logger.debug(f"Computing monthly std for a formula {var}")
                data_mon_std = reader.fldmean(eval_formula(var, data_mon_std)).groupby("time.month").std()
            else:
                data_mon_std = reader.fldmean(data_mon_std).groupby("time.month").std()
                data_mon_std = data_mon_std[var]
        else:
            data_mon_std = None
    else:
        data_mon = None
        data_mon_std = None

    # Annual data
    if annual or annual_std:
        data = reader.timmean(data=data, freq='YS',
                              exclude_incomplete=True,
                              center_time=True)
        if annual:
            data_ann = data.sel(time=slice(startdate, enddate))
            if formula:
                data_ann = reader.fldmean(eval_formula(var, data_ann))
            else:
                data_ann = reader.fldmean(data_ann)
                data_ann = data_ann[var]
        else:
            data_ann = None

        if annual_std:
            data_ann_std = data.sel(time=slice(std_startdate, std_enddate))
            if formula:
                data_ann_std = reader.fldmean(eval_formula(var, data_ann_std)).std()
            else:
                data_ann_std = reader.fldmean(data_ann_std).std()
                data_ann_std = data_ann_std[var]
        else:
            data_ann_std = None
    else:
        data_ann = None
        data_ann_std = None

    return data_mon, data_mon_std, data_ann, data_ann_std


def _start_end_dates(startdate=None, enddate=None,
                     start_std=None, end_std=None):
    """Evaluate start and end dates for the reference data retrieve"""
    try:
        start_retrieve = min(startdate, start_std)
    except TypeError:
        start_retrieve = None

    try:
        end_retrieve = max(enddate, end_std)
    except TypeError:
        end_retrieve = None

    return start_retrieve, end_retrieve
