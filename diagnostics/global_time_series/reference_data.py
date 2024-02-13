"""
Functions to retrieve reference data for global time series diagnostics.
"""
from aqua import Reader
from aqua.logger import log_configure
from aqua.exceptions import NoObservationError


def get_reference_ts_gregory(ts_name='2t', ts_ref={'model': 'ERA5', 'exp': 'era5', 'source': 'monthly'},
                             startdate='1980-01-01', enddate='2010-12-31', loglevel='WARNING'):
    """Retrieve ts reference data for Gregory plot.

    Parameters:
        ts_name (str): variable name for 2m temperature, default is '2t'.
        ts_ref (dict): dictionary with model, exp and source for 2m temperature, default is {'model': 'ERA5', 'exp': 'era5', 'source': 'monthly'}.
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


def get_reference_toa_gregory(toa_name=['mtnlwrf', 'mtnswrf'], toa_ref={'model': 'CERES', 'exp': 'ebaf-toa42', 'source': 'monthly'},
                              startdate='2001-01-01', enddate='2020-12-31', loglevel='WARNING'):
    """Retrieve toa reference data for Gregory plot.

    Parameters:
        toa_name (list): list of variable names for net radiation at TOA, default is ['mtnlwrf', 'mtnswrf'].
        toa_ref (dict): dictionary with model, exp and source for net radiation at TOA, default is {'model': 'CERES', 'exp': 'ebaf-toa42', 'source': 'monthly'}.
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
