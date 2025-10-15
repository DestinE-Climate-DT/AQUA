from aqua.logger import log_configure
import xarray as xr


def convert_so(so):
    """
    Convert practical salinity to absolute salinity using a TEOS-10 approximation.

    Args:
        so (dask.array.core.Array): masked array containing the practical salinity
            values (PSU or 0.001).

    Returns:
        dask.array.core.Array: masked array containing the absolute salinity values (g/kg).

    Notes:
        Uses an approximation from TEOS-10 equations and may yield different results,
        particularly in the Baltic Sea.
        Reference: http://www.teos-10.org/pubs/gsw/pdf/SA_from_SP.pdf
    """
    abs_so = so / 0.99530670233846
    return abs_so


def convert_thetao(abs_so, thetao):
    """
    Convert potential temperature to conservative temperature.

    Args:
        abs_so (dask.array.core.Array): masked array containing the absolute
            salinity values (g/kg).
        thetao (dask.array.core.Array): masked array containing the potential
            temperature values (°C).

    Returns:
        dask.array.core.Array: masked array containing the conservative
        temperature values (°C).

    Notes:
        Uses an approximation based on TEOS-10.
        Reference: http://www.teos-10.org/pubs/gsw/html/gsw_CT_from_pt.html
    """
    x = xr.ufuncs.sqrt(0.0248826675584615 * abs_so)
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
    return bigthetao
