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
