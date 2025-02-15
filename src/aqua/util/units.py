import os
from metpy.units import units
from aqua.logger import log_configure
from .config import ConfigPath
from .yaml import load_yaml


def normalize_units(src, loglevel='WARNING'):
    """
    Get rid of stange grib units based on the default.yaml fix file

    Arguments:
        src (str): input unit to be fixed
    """
    logger = log_configure(loglevel, 'normalize_units')
    src = str(src)

    config_folder = ConfigPath().get_config_dir()
    config_folder = os.path.join(config_folder, "fixes")
    default_file = os.path.join(config_folder, "default.yaml")

    if not os.path.exists(default_file):
        raise FileNotFoundError(f"Cannot find default.yaml in {config_folder}")

    default_dict = load_yaml(default_file)
    fix_units = default_dict['defaults']['units']['fix']
    for key in fix_units:
        if key == src:
            # return fixed
            logger.info('Replacing non-metpy unit %s with %s', key, fix_units[key])
            return src.replace(key, fix_units[key])

    # return original
    return src


def convert_units(src, dst, deltat=None, var="input var", loglevel='WARNING'):
    """
    Converts source to destination units using metpy.

    Arguments:
        src (str): Source units.
        dst (str): Destination units.
        deltat (float, optional): Time delta in seconds (needed for some unit conversions).
        var (str): Variable name (optional, used only for diagnostic output).
        loglevel (str): Log level for the logger. Default is 'WARNING'.

    Returns:
        dict: A dictionary with keys `factor`, `offset`, and possible extra flags
              (e.g., `time_conversion_flag`).
    """
    logger = log_configure(loglevel, 'convert_units')
    src = normalize_units(src)
    dst = normalize_units(dst)
    factor = units(src).to_base_units() / units(dst).to_base_units()

    # Dictionary for storing conversion attributes
    conversion = {}

    # Flag for time-dependent conversions
    if "second" in str(factor.units) and deltat is not None:
        conversion['time_conversion_flag'] = 1
        conversion['deltat'] = str(deltat)
    elif "second" in str(factor.units) and deltat is None:
        logger.warning("Time-dependent conversion factor detected, but no accumulation time provided")

    if factor.units == units('dimensionless'):
        offset = (0 * units(src)).to(units(dst)) - (0 * units(dst))
    else:
        if factor.units == "meter ** 3 / kilogram":
            factor *= 1000 * units("kg m-3")
            if logger:
                logger.debug("%s: corrected multiplying by density of water 1000 kg m-3", var)
        elif factor.units == "meter ** 3 * second / kilogram":
            factor *= 1000 * units("kg m-3") / (deltat * units("s"))
            if logger:
                logger.debug("%s: corrected multiplying by density of water 1000 kg m-3", var)
                logger.info("%s: corrected dividing by accumulation time %s s", var, deltat)
        elif factor.units == "second":
            factor /= deltat * units("s")
            if logger:
                logger.debug("%s: corrected dividing by accumulation time %s s", var, deltat)
        elif factor.units == "kilogram / meter ** 3":
            factor /= 1000 * units("kg m-3")
            if logger:
                logger.debug("%s: corrected dividing by density of water 1000 kg m-3", var)
        else:
            if logger:
                logger.debug("%s: incommensurate units converting %s to %s --> %s",
                             var, src, dst, factor.units)
        offset = 0 * units(dst)

    # Store non-default conversion factors and offsets
    if offset.magnitude != 0:
        conversion['offset'] = offset.magnitude
    elif factor.magnitude != 1:
        conversion['factor'] = factor.magnitude

    return conversion
