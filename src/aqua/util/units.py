import os
import re
import xarray as xr
from metpy.units import units
from aqua.logger import log_configure, log_history
from aqua.configurer import ConfigPath
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
    Returns a dictionary with conversion factors and offsets.

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
    src = normalize_units(src, loglevel)
    dst = normalize_units(dst, loglevel)
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


def convert_data_units(data, var: str, units: str, loglevel: str = 'WARNING'):
    """
    Converts in-place the units of a variable in an xarray Dataset or DataArray.

    Args:
        data (xarray Dataset or DataArray): The data to be checked.
        var (str): The variable to be checked.
        units (str): The units to be checked.
    """
    logger = log_configure(log_name='check_data', log_level=loglevel)

    data_to_fix = data[var] if isinstance(data, xr.Dataset) else data
    final_units = units
    initial_units = data_to_fix.units

    conversion = convert_units(initial_units, final_units)

    factor = conversion.get('factor', 1)
    offset = conversion.get('offset', 0)

    if factor != 1 or offset != 0:
        logger.debug('Converting %s from %s to %s',
                     var, initial_units, final_units)
        data_to_fix = data_to_fix * factor + offset
        data_to_fix.attrs['units'] = final_units
        log_history(data_to_fix, f"Converting units of {var}: from {initial_units} to {final_units}")
    else:
        logger.debug('Units of %s are already in %s', var, final_units)
        return data

    if isinstance(data, xr.Dataset):
        data_fixed = data.copy()
        data_fixed[var] = data_to_fix
    else:
        data_fixed = data_to_fix

    return data_fixed


def multiply_units(unit1: str, unit2: str, normalise_units=True, 
                   to_base_units=True, loglevel: str = 'WARNING') -> str:
    """
    Multiply two unit strings together using metpy.
    
    This is useful when combining physical quantities. For example, integrating 
    a field in meters (e.g. sea ice thickness) over an area in m**2 (e.g. areacello), 
    resulting in m**3 (e.g. sea ice volume)).
    
    Args:
        unit1 (str): First unit string (e.g., 'm')
        unit2 (str): Second unit string (e.g., 'm2')
        normalise_units (bool): If True, normalize the units. Default is True.
        to_base_units (bool): If True, convert the units to base units. Default is True.
        loglevel (str): Log level for the logger. Default is 'WARNING'.
    
    Returns:
        str: The multiplied unit string (e.g., 'm**3')
        
    Example:
        >>> multiply_units('m', 'm2')
        'm**3'
    """
    unit1 = normalize_units(unit1, loglevel) if normalise_units else unit1
    unit2 = normalize_units(unit2, loglevel) if normalise_units else unit2
    
    result = units(unit1) * units(unit2)
    
    result = result.to_base_units() if to_base_units else result
    return str(result.units)


def units_to_latex(units_str: str) -> str:
    """
    Convert unit strings with powers (^ or **) to LaTeX notation for matplotlib labels.
    
    This function parses unit strings and converts power notation to LaTeX format,
    preserving prefixes and handling complex unit compositions.
    
    Args:
        units_str (str): Unit string to convert (e.g., "million km^2", "W/m^2", "m s^-1")
    
    Returns:
        str: LaTeX-formatted unit string (e.g., "million $\\mathrm{km}^2$", 
             "$\\mathrm{W}\\,\\mathrm{m}^{-2}$")
    """
    if not units_str:
        return units_str
    
    units_str = str(units_str).strip()
    
    # Check if already LaTeX formatted (contains \mathrm or $)
    if '\\mathrm' in units_str or (units_str.startswith('$') and units_str.endswith('$')):
        return units_str
    
    # Power notation pattern: ^, **, or unit followed by hyphen and digit
    power_pattern = r'[\^]|\*\*|(?<=[\w\u00B5\u03BC\u212B\u00C5\u00B0])-\d'
    
    # Handle bracket-only units like "[0-1]" - preserve as-is if no power notation
    if units_str.startswith('[') and units_str.endswith(']'):
        if not re.search(power_pattern, units_str):
            return units_str
    
    # Check if the string contains any power notation
    if not re.search(power_pattern, units_str):
        return units_str
    
    # Common prefixes that should be kept outside math mode
    prefixes = ['million', 'millions', 'thousands', 'thousand', 'billion', 'trillion', 
                'kilo', 'mega', 'giga', 'micro', 'nano', 'pico']
    
    return _process_units_string(units_str, prefixes)


def _process_units_string(units_str: str, prefixes: list) -> str:
    """
    Process a unit string, handling divisions, parentheses, and converting to LaTeX.
    
    Args:
        units_str (str): Unit string to process
        prefixes (list): List of prefix strings to recognize
    
    Returns:
        str: LaTeX-formatted string
    """
    # Handle divisions: convert "kg/m/s" to "kg m^-1 s^-1" style
    if '/' in units_str:
        # Remove parentheses for grouped units (e.g., "W/(m^2 s)" -> "W/m^2/s")
        if '(' in units_str and ')' in units_str:
            units_str = units_str.replace('(', '').replace(')', '')
        
        # Split and process: numerator first, then denominators with negative powers
        div_parts = [p.strip() for p in units_str.split('/')]
        if len(div_parts) > 1:
            parts = _parse_unit_expression(div_parts[0], prefixes, negative=False)
            for denom in div_parts[1:]:
                parts.extend(_parse_unit_expression(denom, prefixes, negative=True))
            return _parts_to_latex(parts)
    
    # No division, just parse the expression
    return _parts_to_latex(_parse_unit_expression(units_str, prefixes, negative=False))


def _parse_unit_expression(expr: str, prefixes: list, negative: bool = False) -> list:
    """
    Parse a unit expression into parts, handling prefixes, units, and powers.
    
    Args:
        expr (str): Expression to parse (e.g., "million km^2", "kg m-2 s-1")
        prefixes (list): List of prefix strings
        negative (bool): If True, make all powers negative
    
    Returns:
        list: List of part dictionaries
    """
    parts = []
    remaining = expr.strip()
    
    # Extract prefix using exact word matching (longest first to match "millions" before "million")
    for prefix in sorted(prefixes, key=len, reverse=True):
        pattern = r'^' + re.escape(prefix) + r'(?=\s|$)'
        match = re.match(pattern, remaining, re.IGNORECASE)
        if match:
            parts.append({'type': 'prefix', 'text': match.group(0)})
            remaining = remaining[len(match.group(0)):].strip()
            break
    
    # Tokenize remaining string, handling spaces and punctuation
    # Split by spaces, but preserve tokens that might have punctuation
    tokens = re.split(r'\s+', remaining)
    
    for token in tokens:
        if not token:
            continue
        
        # Handle tokens that might have leading/trailing parentheses or punctuation
        # e.g., "(m^2)", "m^2)", "kg/m^2)"
        token_clean = token
        
        # Remove leading opening paren if it's at the start
        leading_paren = ''
        if token_clean.startswith('('):
            leading_paren = '('
            token_clean = token_clean[1:]
        
        # Remove trailing closing paren or comma
        trailing_punct = ''
        if token_clean.endswith(')'):
            trailing_punct = ')' + trailing_punct
            token_clean = token_clean[:-1]
        if token_clean.endswith(','):
            trailing_punct = ',' + trailing_punct
            token_clean = token_clean[:-1]
        
        # Parse the cleaned token for unit and power
        unit_part = _parse_unit_token(token_clean, negative)
        
        if unit_part:
            if leading_paren:
                parts.append({'type': 'text', 'text': leading_paren})
            parts.append(unit_part)
            if trailing_punct:
                # Add trailing punctuation as text
                parts.append({'type': 'text', 'text': trailing_punct})
        else:
            # Couldn't parse as unit, treat as text (preserve original)
            parts.append({'type': 'text', 'text': token})
    
    return parts


def _parse_unit_token(token: str, negative: bool = False) -> dict:
    """
    Parse a single token to extract unit symbol and power.
    
    Handles:
    - Simple units: "m", "kg", "s"
    - Power notation: "m^2", "m**2", "m^(2)", "m**(2)"
    - Negative powers: "m^-2", "m**-2", "m-2"
    - Unicode characters: "µg", "°C", "Å", "μm"
    
    Args:
        token (str): Token to parse
        negative (bool): If True, make power negative
    
    Returns:
        dict: Unit part dictionary or None if not a unit
    """
    if not token:
        return None
    
    # Unicode-aware unit pattern (includes µ, μ, Å, °, etc.)
    unit_chars = r'[\w\u00B5\u03BC\u212B\u00C5\u00B0]+'
    
    # Try different power notations (order matters: more specific first)
    patterns = [
        (r'^(' + unit_chars + r')\*\*\(?(-?\d+)\)?$', '**'),  # m**(2), m**-2, m**2
        (r'^(' + unit_chars + r')\*\*(-?\d+)$', '**'),        # m**2, m**-2
        (r'^(' + unit_chars + r')\^\(?(-?\d+)\)?$', '^'),     # m^(2), m^-2, m^2
        (r'^(' + unit_chars + r')\^(-?\d+)$', '^'),           # m^2, m^-2
        (r'^(' + unit_chars + r')-(\d+)$', '-'),              # m-2 (always negative)
    ]
    
    for pattern, notation_type in patterns:
        match = re.match(pattern, token)
        if match:
            unit_symbol = match.group(1)
            power = '-' + match.group(2) if notation_type == '-' else match.group(2)
            if negative and not power.startswith('-'):
                power = '-' + power
            return {'type': 'unit', 'symbol': unit_symbol, 'power': power}
    
    # Simple unit without power
    if re.match(r'^' + unit_chars + r'$', token):
        return {'type': 'unit', 'symbol': token}
    
    return None


def _parts_to_latex(parts: list) -> str:
    """
    Convert parsed parts to LaTeX string.
    
    Args:
        parts (list): List of part dictionaries
    
    Returns:
        str: LaTeX-formatted string
    """
    if not parts:
        return ''
    
    latex_parts = []
    in_math_mode = False
    
    for part in parts:
        if part['type'] == 'prefix':
            if in_math_mode:
                latex_parts.append('$')
                in_math_mode = False
            latex_parts.append(part['text'] + ' ')
        elif part['type'] == 'unit':
            # Open math mode if not open
            if not in_math_mode:
                latex_parts.append('$')
                in_math_mode = True
            else:
                # Add thin space between units
                latex_parts.append(r'\,')
            
            unit_symbol = part['symbol']
            power = part.get('power', None)
            
            if power:
                latex_parts.append(f'\\mathrm{{{unit_symbol}}}^{{{power}}}')
            else:
                latex_parts.append(f'\\mathrm{{{unit_symbol}}}')
        elif part['type'] == 'text':
            # Close math mode if open, add text
            if in_math_mode:
                latex_parts.append('$')
                in_math_mode = False
            latex_parts.append(part['text'])
    
    # Close math mode if still open
    if in_math_mode:
        latex_parts.append('$')
    
    result = ''.join(latex_parts).strip()
    
    # Clean up any double spaces
    result = re.sub(r'\s+', ' ', result)
    
    return result