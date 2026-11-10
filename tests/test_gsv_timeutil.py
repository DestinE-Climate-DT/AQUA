"""Tests for the FDB time utilities in AQUA"""

import pandas as pd
import pytest

from aqua.core.intake_drivers.fdb.openers.timeutil import FDBTimeMixin

pytestmark = pytest.mark.aqua

floor_datetime = FDBTimeMixin._floor_datetime


@pytest.mark.parametrize(
    "dt, freq, expected",
    [
        ("2025-01-28 15:30:45", "h", "20250128T1500"),
        ("2025-01-28 15:30:45", "D", "20250128T0000"),
        ("2025-03-15 15:30:45", "M", "20250301T0000"),
        ("2025-03-15 15:30:45", "Y", "20250101T0000"),
        ("2025-03-15 12:45:00", "MS", "20250301T0000"),
        ("2025-03-15 12:45:00", "YS", "20250101T0000"),
        ("2025-01-28 15:30:45", "min", "20250128T1530"),
        ("2025-01-28 15:30:45", "s", "20250128T1530"),
        ("2025-01-28 15:30:45", "3h", "20250128T1500"),
        ("2025-01-28 15:30:45", "1D", "20250128T0000"),
        # FDB catalog dates are strings like 19900101T0000, not Timestamps
        ("19900101T1530", "h", "19900101T1500"),
        (None, "h", None),
        ("complete", "h", "complete"),
    ],
)
def test_floor_datetime(dt, freq, expected):
    """Dates are floored to the pandas frequency and formatted as FDB dates: fails on a wrong floor or format."""
    if dt is not None and dt != "complete" and "-" in dt:
        dt = pd.Timestamp(dt)
    assert floor_datetime(dt, freq) == expected


@pytest.mark.parametrize(
    "freq, error, match",
    [
        ("H", ValueError, "Invalid frequency: H"),
        ("T", ValueError, "Invalid frequency: T"),
        ("2W", KeyError, "Weekly frequency not supported"),
        ("ME", KeyError, "Freq ME is not supported, please use MS"),
        ("YE", KeyError, "Freq YE is not supported, please use YS"),
    ],
)
def test_floor_datetime_unsupported_freq(freq, error, match):
    """Deprecated pandas aliases, weekly and month/year-end frequencies are rejected with their own message."""
    with pytest.raises(error, match=match):
        floor_datetime(pd.Timestamp("2025-01-28 15:30:45"), freq)
