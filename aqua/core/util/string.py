"""
Module including string utilities for AQUA
"""

import re
import random
import string

def generate_random_string(length):
    """
    Generate a random string of lowercase and uppercase letters and digits
    """
    letters_and_digits = string.ascii_letters + string.digits
    random_string = ''.join(random.choice(letters_and_digits) for _ in range(length))
    return random_string


def strlist_to_phrase(items: list[str], oxford_comma: bool = False) -> str:
    """
    Convert a list of str to a english-consistent list.
       ['A'] will return "A"
       ['A','B'] will return "A and B"
       ['A','B','C'] will return "A, B and C" (oxford_comma=False)
       ['A','B','C'] will return "A, B, and C" (oxford_comma=True)
       
    Args:
        items (list[str]): The list of strings to format.
    """
    if not items: return ""
    if len(items) == 1: return items[0]
    if len(items) == 2: return f"{items[0]} and {items[1]}"
    return ", ".join(items[:-1]) + (", and " if oxford_comma else " and ") + items[-1]


def lat_to_phrase(lat: int) -> str:
    """
    Convert a latitude value into a string representation.

    Returns:
        str: formatted as "<deg>°N" for northern latitudes or "<deg>°S" for southern latitudes.
    """
    if lat >= 0:
        return f"{lat}°N"
    if lat < 0:
        return f"{abs(lat)}°S"


def get_quarter_anchor_month(freq_string: str) -> str:
    """
    Get the anchor month from a quarterly frequency string.
    Examples: 'QE-DEC' -> 'DEC'; 'Q-DEC' -> 'DEC'; 'QS' -> 'DEC' (default)
    Args:
        freq_string (str): The frequency string to extract the anchor month from.
    Returns:
        str: The anchor month.
    """
    if '-' in freq_string:
        return freq_string.split('-')[1]
    return 'DEC'


def clean_filename(filename: str) -> str:
    """
    Check a filename by replacing spaces with '_' and forcing lowercase.
    
    Args:
        filename (str): The filename (or part of filename) to check.
        
    Returns:
        str: Filename with spaces replaced by '_' and forced lowercase.
    """
    return filename.replace(' ', '_').lower()


def extract_literal_and_numeric(text):
    """
    Given a string, extract its literal and numeric part
    """
    # Using regular expression to find alphabetical characters and digits in the text
    match = re.search(r'(\d*)([A-Za-z]+)', text)

    if match:
        # If a match is found, return the literal and numeric parts
        literal_part = match.group(2)
        numeric_part = match.group(1)
        if not numeric_part:
            numeric_part = 1
        return literal_part, int(numeric_part)
    else:
        # If no match is found, return None or handle it accordingly
        return None, None


def unit_to_latex(unit_str):
    """
    Convert unit string to LaTeX notation. Preserves existing LaTeX notation.
    Handles:
    - Division:  W/m^2 -> W m$^{-2}$; W/(m^2 s) -> W m$^{-2}$ s$^{-1}$
    - Exponents: m-2   -> m$^{-2}$; m^2, m**2, m2 -> m$^{2}$; kg m-1 s-1 -> kg m$^{-1}$ s$^{-1}$
    - Multi-word: million km^2 -> million km$^{2}$
    
    Args:
        unit_str: Unit string in various formats
        
    Returns:
        Unit string with only exponents in LaTeX math mode
    """
    if not unit_str:
        return unit_str
        
    if not unit_str.strip():
        return ""

    s = unit_str.strip()

    # Preserve if already in LaTeX format
    if any(x in s for x in ['$', '\\', '{']):
        return s

    # Handle pure numeric units (e.g., "1" for dimensionless)
    if re.match(r'^\d+$', s):
        return s

    # Replace ** with ^ for consistency; e.g. m**2 -> m^2
    s = s.replace('**', '^')

    # Handle grouped division; e.g. W/(m^2 s) -> W / m^2 / s
    def _repl_div_group(match):
        content = match.group(1)
        return '/' + content.replace(' ', '/')

    s = re.sub(r'/\s*\(([^)]+)\)', _repl_div_group, s)
    
    # Remove any remaining parentheses
    s = s.replace('(', '').replace(')', '')

    # Split by '/' to handle division
    parts = s.split('/')
    
    latex_parts = []
    # Process numerator
    latex_parts.extend(_parse_unit_parts(parts[0], invert=False))

    # Process denominators by inverting exponents
    for p in parts[1:]:
        latex_parts.extend(_parse_unit_parts(p, invert=True))

    # Join and normalize spaces and replace multiple spaces with single space
    result = ' '.join(latex_parts)
    result = re.sub(r'\s+', ' ', result)
    return result


def _parse_unit_parts(text, invert):
    """
    Parses units and exponents from a string segment.
    Preserves spaces and only wraps exponents in math mode.
    """
    # Pattern matches: optional leading space, unit (letters/µ/°/%), optional exponent
    # This preserves spaces between units
    pattern = r'(\s*)([a-zA-Zµ°%]+)(?:\^?\s*(-?\d+)|(-?\d+))?'
    
    matches = re.findall(pattern, text)
    results = []
    
    for match in matches:
        leading_space = match[0]  # Preserve any leading whitespace
        unit = match[1]
        if not unit:
            continue
        
        # Get exponent from either capture group (^number or implicit number)
        exp_str = match[2] or match[3]
        
        # Parse exponent, default to 1 (no exponent)
        exp = int(exp_str) if exp_str else 1
        
        if invert:
            exp = -exp
        
        # Escape % for LaTeX (must be \% to display properly)
        unit_escaped = unit.replace('%', r"$\%$")
        
        # Format: only wrap exponents
        if exp == 1:
            # No exponent: just the unit with its leading space
            results.append(leading_space + unit_escaped)
        else:
            # With exponent: unit + $^{exp}$ with leading space
            results.append(leading_space + unit_escaped + f"$^{{{exp}}}$")
    
    return results
