from aqua.logger import log_configure
import xarray as xr

def convert_so(so, loglevel= "WARNING"):
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
    logger = log_configure(loglevel, 'convert_so')
    logger.debug("Converting practical salinity to absolute salinity.")
    absso = so / 0.99530670233846
    logger.info("Practical salinity successfully converted to absolute salinity.")
    return absso


def convert_thetao(absso, thetao, loglevel= "WARNING"):
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
    logger = log_configure(loglevel, 'convert_thetao')
    logger.debug("Converting potential temperature to conservative temperature.")
    x = xr.ufuncs.sqrt(0.0248826675584615 * absso)
    y = thetao*0.025e0
    enthalpy = 61.01362420681071e0 + y*(168776.46138048015e0 +
                                        y*(-2735.2785605119625e0 + y*(2574.2164453821433e0 +
                                                                      y*(-1536.6644434977543e0 + y*(545.7340497931629e0 +
                                                                                                    (-50.91091728474331e0 - 18.30489878927802e0*y) *
                                                                                                    y))))) + x**2*(268.5520265845071e0 + y*(-12019.028203559312e0 +
                                                                                                                                            y*(3734.858026725145e0 + y*(-2046.7671145057618e0 +
                                                                                                                                                                        y*(465.28655623826234e0 + (-0.6370820302376359e0 -
                                                                                                                                                                                                   10.650848542359153e0*y)*y)))) +
                                                                                                                   x*(937.2099110620707e0 + y*(588.1802812170108e0 +
                                                                                                                                               y*(248.39476522971285e0 + (-3.871557904936333e0 -
                                                                                                                                                                          2.6268019854268356e0*y)*y)) +
                                                                                                                      x*(-1687.914374187449e0 + x*(246.9598888781377e0 +
                                                                                                                                                   x*(123.59576582457964e0 - 48.5891069025409e0*x)) +
                                                                                                                         y*(936.3206544460336e0 +
                                                                                                                            y*(-942.7827304544439e0 + y*(369.4389437509002e0 +
                                                                                                                                                         (-33.83664947895248e0 - 9.987880382780322e0*y)*y))))))

    bigthetao = enthalpy/3991.86795711963
    logger.info("Potential temperature successfully converted to conservative temperature.")
    return bigthetao


def compute_rho(absso, bigthetao, ref_pressure, loglevel= "WARNING"):
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
    logger = log_configure(loglevel, 'compute_rho')
    logger.debug("Computing potential density in-situ.")
    # reduced variables
    SAu = 40.*35.16504/35.
    CTu = 40.
    Zu = 1e4
    deltaS = 32.
    ss = xr.ufuncs.sqrt((absso+deltaS)/SAu)
    tt = bigthetao / CTu
    pp = ref_pressure / Zu

    # vertical reference profile of density
    R00 = 4.6494977072e+01
    R01 = -5.2099962525e+00
    R02 = 2.2601900708e-01
    R03 = 6.4326772569e-02
    R04 = 1.5616995503e-02
    R05 = -1.7243708991e-03
    r0 = (((((R05*pp + R04)*pp + R03)*pp + R02)*pp + R01)*pp + R00)*pp

    # density anomaly
    R000 = 8.0189615746e+02
    R100 = 8.6672408165e+02
    R200 = -1.7864682637e+03
    R300 = 2.0375295546e+03
    R400 = -1.2849161071e+03
    R500 = 4.3227585684e+02
    R600 = -6.0579916612e+01
    R010 = 2.6010145068e+01
    R110 = -6.5281885265e+01
    R210 = 8.1770425108e+01
    R310 = -5.6888046321e+01
    R410 = 1.7681814114e+01
    R510 = -1.9193502195e+00
    R020 = -3.7074170417e+01
    R120 = 6.1548258127e+01
    R220 = -6.0362551501e+01
    R320 = 2.9130021253e+01
    R420 = -5.4723692739e+00
    R030 = 2.1661789529e+01
    R130 = -3.3449108469e+01
    R230 = 1.9717078466e+01
    R330 = -3.1742946532e+00
    R040 = -8.3627885467e+00
    R140 = 1.1311538584e+01
    R240 = -5.3563304045e+00
    R050 = 5.4048723791e-01
    R150 = 4.8169980163e-01
    R060 = -1.9083568888e-01
    R001 = 1.9681925209e+01
    R101 = -4.2549998214e+01
    R201 = 5.0774768218e+01
    R301 = -3.0938076334e+01
    R401 = 6.6051753097e+00
    R011 = -1.3336301113e+01
    R111 = -4.4870114575e+00
    R211 = 5.0042598061e+00
    R311 = -6.5399043664e-01
    R021 = 6.7080479603e+0
    R121 = 3.5063081279e+00
    R221 = -1.8795372996e+00
    R031 = -2.4649669534e+00
    R131 = -5.5077101279e-01
    R041 = 5.5927935970e-01
    R002 = 2.0660924175e+00
    R102 = -4.9527603989e+00
    R202 = 2.5019633244e+00
    R012 = 2.0564311499e+00
    R112 = -2.1311365518e-01
    R022 = -1.2419983026e+00
    R003 = -2.3342758797e-02
    R103 = -1.8507636718e-02
    R013 = 3.7969820455e-01

    rz3 = R013*tt + R103*ss + R003
    rz2 = (R022*tt+R112*ss+R012)*tt+(R202*ss+R102)*ss+R002
    rz1 = (((R041*tt+R131*ss+R031)*tt +
            (R221*ss+R121)*ss+R021)*tt +
           ((R311*ss+R211)*ss+R111)*ss+R011)*tt + \
        (((R401*ss+R301)*ss+R201)*ss+R101)*ss+R001
    rz0 = (((((R060*tt+R150*ss+R050)*tt +
              (R240*ss+R140)*ss+R040)*tt +
             ((R330*ss+R230)*ss+R130)*ss+R030)*tt +
            (((R420*ss+R320)*ss+R220)*ss+R120)*ss+R020)*tt +
           ((((R510*ss+R410)*ss+R310)*ss+R210)*ss+R110)*ss+R010)*tt + \
        (((((R600*ss+R500)*ss+R400)*ss+R300)*ss+R200)*ss+R100)*ss+R000
    r = ((rz3*pp + rz2)*pp + rz1)*pp + rz0

    # in-situ density
    return r + r0


def convert_variables(data, loglevel= "WARNING"):
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
    logger = log_configure(loglevel, 'convert_variables')
    logger.info("Starting conversion of variables: practical salinity, potential temperature.")
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



def compute_mld_cont(rho, loglevel= "WARNING"):
    """
    Compute the mixed layer depth (MLD) from density fields with continuous levels.

    The MLD is determined using the criteria from de Boyer Montegut et al. (2004),
    by interpolating between the first level that exceeds the density threshold (0.03 kg/m³)
    and the next, to estimate where the threshold would be reached.

    Warning
    -------
    This function may not provide reasonable estimates in cases where the upper level
    has higher densities than the lower one. Use with caution until this issue is addressed.

    Parameters
    ----------
    rho : xarray.DataArray
        Density field (sigma0), must have dimensions time, space, and depth (in meters).
    loglevel : str, optional
        Logging level. Default is 'WARNING'.

    Returns
    -------
    xarray.DataArray
        Mixed layer depth (MLD) with dimensions of time and space.
    """
    logger = log_configure(loglevel, 'compute_mld_cont')
    logger.info("Starting computation of mixed layer depth (MLD) from density field.")
    # Identify the first level to represent the ocean surface
    logger.debug("Identifying surface density.")
    surf_dens = rho.isel(level=slice(0, 1)).mean("level")

    # Compute the density anomaly between surface and the full water column
    logger.debug("Computing density anomaly between surface and whole field.")
    dens_ano = rho - surf_dens

    # Apply the sigma difference threshold (0.03 kg/m³) as per de Boyer Montegut et al. (2004)
    logger.debug("Applying sigma difference threshold (0.03 kg/m3).")
    dens_diff = dens_ano - 0.03

    # Keep only the levels where the threshold has not been surpassed
    logger.debug("Filtering levels where threshold has not been surpassed.")
    dens_diff2 = dens_diff.where(dens_diff < 0)

    # Find the deepest level before the threshold is exceeded
    logger.debug("Finding deepest level before threshold is exceeded.")
    cutoff_lev1 = dens_diff2.level.where(dens_diff2 > -9999).max(["level"])

    # Find the first level after the threshold is exceeded
    logger.debug("Finding first level after threshold is exceeded.")
    cutoff_lev2 = dens_diff2.level.where(dens_diff2.level > cutoff_lev1).min(["level"])

    # Identify the last valid ocean level
    logger.debug("Identifying last valid ocean level.")
    depth = rho.level.where(rho > -9999).max(["level"])

    # Interpolate to estimate MLD between threshold levels
    ddif = cutoff_lev2 - cutoff_lev1
    logger.debug("Interpolating to estimate MLD between threshold levels.")
    rdif1 = dens_diff.where(dens_diff.level == cutoff_lev1).max(["level"])  # Density diff at first level
    rdif2 = dens_diff.where(dens_diff.level == cutoff_lev2).max(["level"])  # Density diff at second level
    mld = cutoff_lev1 + ((ddif) * (rdif1)) / (rdif1 - rdif2)

    # Set MLD as maximum depth if threshold is not exceeded
    logger.debug("Setting MLD as maximum depth if threshold not exceeded.")
    mld = xr.ufuncs.fmin(mld, depth)
    mld = mld.rename({"rho": "mld"})
    logger.info("MLD computation completed and variable renamed.")
    return mld