"""Tests for GSV timeutile in AQUA"""

import pytest
import pandas as pd
from aqua.core.gsv.timeutil import floor_datetime

# Assuming floor_datetime is already imported

def test_floor_datetime_all_cases():
    test_cases = [
        # Valid frequencies and expected results
        ('2025-01-28 15:30:45', 'h',  "20250128T1500"),  # Hour
        ('2025-01-28 15:30:45', 'H',  "20250128T1500"),  # Hour
        ('2025-01-28 15:30:45', 'D',  "20250128T0000"),  # Day
        ('2025-03-15 15:30:45', 'M',  "20250301T0000"),  # Month
        ('2025-03-15 15:30:45', 'Y',  "20250101T0000"),  # Year
        ('2025-03-15 12:45:00', 'MS', "20250301T0000"),  # Month Start
        ('2025-03-15 12:45:00', 'YS', "20250101T0000"),  # Year Start
        ('2025-01-28 15:30:45', 'T',  "20250128T1530"),  # Minute
        ('2025-01-28 15:30:45', 'S',  "20250128T1530"),  # Second
        ('2025-01-28 15:30:45', '2W', KeyError),  # Week (start of the week)
        ('2025-01-28 15:30:45', '3H', "20250128T1500"),  # Custom frequency: every 3 hours
        ('2025-01-28 15:30:45', '1D', "20250128T0000"),  # Custom frequency: daily
        ('2025-01-28 15:30:45', 'ME', KeyError),  # Invalid frequency should raise a KeyError
        ('2025-01-28 15:30:45', 'YE', KeyError),  # Invalid frequency should raise a KeyError
    ]

    for dt_str, freq, expected in test_cases:
        dt = pd.Timestamp(dt_str)
        if isinstance(expected, str):  # For valid cases
            result = floor_datetime(dt, freq)
            assert result == expected, f"Failed for {freq} on {dt_str}: expected {expected}, got {result}"
        elif expected == KeyError:  # For invalid frequencies
            with pytest.raises(KeyError):
                floor_datetime(dt, freq)