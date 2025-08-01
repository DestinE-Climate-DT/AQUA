from aqua.logger import log_configure
import xarray as xr


def convert_so(so, loglevel="WARNING"):
    """
    Convert practical salinity to absolute salinity using a TEOS-10 approximation.

    Parameters
    ----------
    so : dask.array.core.Array
        Masked array containing the practical salinity values (psu or 0.001).

    Returns
    -------
    absso : dask.array.core.Array
        Masked array containing the absolute salinity values (g/kg).

    Notes
    -----
    This function uses an approximation from TEOS-10 equations and may yield different values,
    particularly in the Baltic Sea. See: http://www.teos-10.org/pubs/gsw/pdf/SA_from_SP.pdf
    """
    logger = log_configure(loglevel, "convert_so")
    logger.debug("Converting practical salinity to absolute salinity.")
    absso = so / 0.99530670233846
    logger.info("Practical salinity successfully converted to absolute salinity.")
    return absso


def convert_thetao(absso, thetao, loglevel="WARNING"):
    """
    Convert potential temperature to conservative temperature.

    Parameters
    ----------
    absso : dask.array.core.Array
        Masked array containing the absolute salinity values (g/kg).
    thetao : dask.array.core.Array
        Masked array containing the potential temperature values (degC).

    Returns
    -------
    bigthetao : dask.array.core.Array
        Masked array containing the conservative temperature values (degC).

    Notes
    -----
    Uses an approximation based on TEOS-10. See: http://www.teos-10.org/pubs/gsw/html/gsw_CT_from_pt.html
    """
    logger = log_configure(loglevel, "convert_thetao")
    logger.debug("Converting potential temperature to conservative temperature.")
    x = xr.ufuncs.sqrt(0.0248826675584615 * absso)
    y = thetao * 0.025e0
    enthalpy = (
        61.01362420681071e0
        + y
        * (
            168776.46138048015e0
            + y
            * (
                -2735.2785605119625e0
                + y
                * (
                    2574.2164453821433e0
                    + y
                    * (
                        -1536.6644434977543e0
                        + y
                        * (
                            545.7340497931629e0
                            + (-50.91091728474331e0 - 18.30489878927802e0 * y) * y
                        )
                    )
                )
            )
        )
        + x**2
        * (
            268.5520265845071e0
            + y
            * (
                -12019.028203559312e0
                + y
                * (
                    3734.858026725145e0
                    + y
                    * (
                        -2046.7671145057618e0
                        + y
                        * (
                            465.28655623826234e0
                            + (-0.6370820302376359e0 - 10.650848542359153e0 * y) * y
                        )
                    )
                )
            )
            + x
            * (
                937.2099110620707e0
                + y
                * (
                    588.1802812170108e0
                    + y
                    * (
                        248.39476522971285e0
                        + (-3.871557904936333e0 - 2.6268019854268356e0 * y) * y
                    )
                )
                + x
                * (
                    -1687.914374187449e0
                    + x
                    * (
                        246.9598888781377e0
                        + x * (123.59576582457964e0 - 48.5891069025409e0 * x)
                    )
                    + y
                    * (
                        936.3206544460336e0
                        + y
                        * (
                            -942.7827304544439e0
                            + y
                            * (
                                369.4389437509002e0
                                + (-33.83664947895248e0 - 9.987880382780322e0 * y) * y
                            )
                        )
                    )
                )
            )
        )
    )

    bigthetao = enthalpy / 3991.86795711963
    logger.info(
        "Potential temperature successfully converted to conservative temperature."
    )
    return bigthetao


def convert_variables(data, loglevel="WARNING"):
    """
    Convert variables in the given dataset to absolute salinity and conservative temperature.

    This function updates the dataset in-place with absolute salinity ('so') and conservative temperature ('thetao').
    Potential density ('rho') can be computed separately if needed.

    Parameters
    ----------
    data : xarray.Dataset
        Dataset containing the variables to be converted.
    loglevel : str, optional
        Logging level. Default is 'WARNING'.

    Returns
    -------
    xarray.Dataset
        Dataset with updated 'so' and 'thetao' variables.
    """
    logger = log_configure(loglevel, "convert_variables")
    logger.info(
        "Starting conversion of variables: practical salinity, potential temperature."
    )
    # Convert practical salinity to absolute salinity
    absso = convert_so(data.so)
    logger.debug("Practical salinity converted to absolute salinity.")

    # Convert potential temperature to conservative temperature
    thetao = convert_thetao(absso, data.thetao)
    logger.debug("Potential temperature converted to conservative temperature.")

    # Update the dataset with converted variables
    data["thetao"] = thetao
    data["so"] = absso
    logger.info("Variables successfully converted and updated in dataset.")
    return data
