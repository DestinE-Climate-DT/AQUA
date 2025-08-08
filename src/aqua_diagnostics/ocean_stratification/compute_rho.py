from aqua.logger import log_configure
import xarray as xr


def compute_rho(absso, bigthetao, ref_pressure, loglevel="WARNING"):
    """
    Compute the potential density in-situ.

    Parameters
    ----------
    absso : dask.array.core.Array
        Masked array containing the absolute salinity values (g/kg).
    bigthetao : dask.array.core.Array
        Masked array containing the conservative temperature values (degC).
    ref_pressure : float
        Reference pressure (dbar).

    Returns
    -------
    rho : dask.array.core.Array
        Masked array containing the potential density in-situ values (kg m-3).

    Notes
    -----
    Based on polyTEOS-10. See: https://github.com/fabien-roquet/polyTEOS/blob/36b9aef6cd2755823b5d3a7349cfe64a6823a73e/polyTEOS10.py#L57
    """
    logger = log_configure(loglevel, "compute_rho")
    logger.debug("Computing potential density in-situ.")
    # reduced variables
    SAu = 40.0 * 35.16504 / 35.0
    CTu = 40.0
    Zu = 1e4
    deltaS = 32.0
    ss = xr.ufuncs.sqrt((absso + deltaS) / SAu)
    tt = bigthetao / CTu
    pp = ref_pressure / Zu

    # vertical reference profile of density
    R00 = 4.6494977072e01
    R01 = -5.2099962525e00
    R02 = 2.2601900708e-01
    R03 = 6.4326772569e-02
    R04 = 1.5616995503e-02
    R05 = -1.7243708991e-03
    r0 = (((((R05 * pp + R04) * pp + R03) * pp + R02) * pp + R01) * pp + R00) * pp

    # density anomaly
    R000 = 8.0189615746e02
    R100 = 8.6672408165e02
    R200 = -1.7864682637e03
    R300 = 2.0375295546e03
    R400 = -1.2849161071e03
    R500 = 4.3227585684e02
    R600 = -6.0579916612e01
    R010 = 2.6010145068e01
    R110 = -6.5281885265e01
    R210 = 8.1770425108e01
    R310 = -5.6888046321e01
    R410 = 1.7681814114e01
    R510 = -1.9193502195e00
    R020 = -3.7074170417e01
    R120 = 6.1548258127e01
    R220 = -6.0362551501e01
    R320 = 2.9130021253e01
    R420 = -5.4723692739e00
    R030 = 2.1661789529e01
    R130 = -3.3449108469e01
    R230 = 1.9717078466e01
    R330 = -3.1742946532e00
    R040 = -8.3627885467e00
    R140 = 1.1311538584e01
    R240 = -5.3563304045e00
    R050 = 5.4048723791e-01
    R150 = 4.8169980163e-01
    R060 = -1.9083568888e-01
    R001 = 1.9681925209e01
    R101 = -4.2549998214e01
    R201 = 5.0774768218e01
    R301 = -3.0938076334e01
    R401 = 6.6051753097e00
    R011 = -1.3336301113e01
    R111 = -4.4870114575e00
    R211 = 5.0042598061e00
    R311 = -6.5399043664e-01
    R021 = 6.7080479603e0
    R121 = 3.5063081279e00
    R221 = -1.8795372996e00
    R031 = -2.4649669534e00
    R131 = -5.5077101279e-01
    R041 = 5.5927935970e-01
    R002 = 2.0660924175e00
    R102 = -4.9527603989e00
    R202 = 2.5019633244e00
    R012 = 2.0564311499e00
    R112 = -2.1311365518e-01
    R022 = -1.2419983026e00
    R003 = -2.3342758797e-02
    R103 = -1.8507636718e-02
    R013 = 3.7969820455e-01

    rz3 = R013 * tt + R103 * ss + R003
    rz2 = (R022 * tt + R112 * ss + R012) * tt + (R202 * ss + R102) * ss + R002
    rz1 = (
        (
            ((R041 * tt + R131 * ss + R031) * tt + (R221 * ss + R121) * ss + R021) * tt
            + ((R311 * ss + R211) * ss + R111) * ss
            + R011
        )
        * tt
        + (((R401 * ss + R301) * ss + R201) * ss + R101) * ss
        + R001
    )
    rz0 = (
        (
            (
                (
                    (
                        (R060 * tt + R150 * ss + R050) * tt
                        + (R240 * ss + R140) * ss
                        + R040
                    )
                    * tt
                    + ((R330 * ss + R230) * ss + R130) * ss
                    + R030
                )
                * tt
                + (((R420 * ss + R320) * ss + R220) * ss + R120) * ss
                + R020
            )
            * tt
            + ((((R510 * ss + R410) * ss + R310) * ss + R210) * ss + R110) * ss
            + R010
        )
        * tt
        + (((((R600 * ss + R500) * ss + R400) * ss + R300) * ss + R200) * ss + R100)
        * ss
        + R000
    )
    r = ((rz3 * pp + rz2) * pp + rz1) * pp + rz0

    # in-situ density
    return r + r0
