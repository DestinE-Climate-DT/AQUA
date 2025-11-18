from aqua.logger import log_configure
import xarray as xr


def compute_mld_cont(rho, loglevel="WARNING"):
    """
    Compute the Mixed Layer Depth (MLD) from a continuous density profile.

    Uses the threshold method from de Boyer Montegut et al. (2004), identifying
    the depth where density exceeds surface density by 0.03 kg/m³, with linear
    interpolation between levels.

    Warning
    -------
    May produce inaccurate results if surface levels are denser than deeper ones.

    Parameters
    ----------
    rho : xarray.DataArray
        Seawater density (sigma0) with dimensions including 'level' (depth).
    loglevel : str, optional
        Logging level (default: "WARNING").

    Returns
    -------
    xarray.DataArray
        Estimated MLD with same horizontal dimensions as `rho`.
    """
    # HACK: ensure level units are in meters, not model layers
    if rho["level"].attrs["units"] == "NEMO model layers":
        rho["level"].attrs["units"] = "m"

    logger = log_configure(loglevel, "compute_mld_cont")
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
    rdif1 = dens_diff.where(dens_diff.level == cutoff_lev1).max(
        ["level"]
    )  # Density diff at first level
    rdif2 = dens_diff.where(dens_diff.level == cutoff_lev2).max(
        ["level"]
    )  # Density diff at second level
    mld = cutoff_lev1 + ((ddif) * (rdif1)) / (rdif1 - rdif2)

    # Set MLD as maximum depth if threshold is not exceeded
    logger.debug("Setting MLD as maximum depth if threshold not exceeded.")
    mld = xr.ufuncs.fmin(mld, depth)
    mld = mld.rename({"rho": "mld"})
    logger.info("MLD computation completed and variable renamed.")
    # adding important attributes
    aqua_dict = {
        key: rho.rho.attrs[key]
        for key in rho.rho.attrs.keys()
        if key.startswith("AQUA")
    }
    mld.mld.attrs.update(aqua_dict)

    mld.mld.attrs["long_name"] = "Mixed Layer Depth"
    mld.mld.attrs["standard_name"] = "ocean_mixed_layer_depth"
    # mld.mld.attrs["units"] = "m"
    return mld
