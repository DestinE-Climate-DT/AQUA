"""Utilities for calendar and timestep calculations"""

import os
from datetime import datetime

import numpy as np
import pandas as pd


class FDBTimeMixin:
    """Time and calendar calculations for FDB/GSV sources."""

    @staticmethod
    def _date2str(dateobj):
        """Converts a date object to a date, time string representation"""
        return dateobj.strftime("%Y%m%d"), dateobj.strftime("%H%M")

    @staticmethod
    def _date2yyyymm(dateobj):
        """Converts a date object to year, month string representation"""
        return dateobj.strftime("%Y"), dateobj.strftime("%m")

    def _add_offset(self, data_start_date, offset, timestep):
        """Sets initial date based on an offset in steps"""
        if int(offset) != 0:
            if timestep.upper() in ["H", "D"]:
                base_date = pd.Timestamp(str(data_start_date)) + pd.Timedelta(int(offset), unit=timestep)
            else:
                raise ValueError("Timestep not supported")

            startdate_obj = pd.Timestamp(str(self.startdate))
            if startdate_obj > base_date:
                base_date = startdate_obj

            self.startdate = base_date.strftime("%Y%m%dT%H%M")

    def _check_dates(self):
        """Check if starting and ending dates are within given range"""
        startdate_fmt = "%Y%m%dT%H%M" if "T" in str(self.startdate) else "%Y%m%d"
        start_date_fmt = "%Y%m%dT%H%M" if "T" in str(self.data_start_date) else "%Y%m%d"
        enddate_fmt = "%Y%m%dT%H%M" if "T" in str(self.enddate) else "%Y%m%d"
        end_date_fmt = "%Y%m%dT%H%M" if "T" in str(self.data_end_date) else "%Y%m%d"

        if datetime.strptime(str(self.startdate), startdate_fmt) < datetime.strptime(
            str(self.data_start_date), start_date_fmt
        ):
            raise ValueError(
                f"Starting date {str(self.startdate)} is earlier than the data start at {str(self.data_start_date)}."
            )

        if datetime.strptime(str(self.enddate), enddate_fmt) > datetime.strptime(str(self.data_end_date), end_date_fmt):
            raise ValueError(f"End date {str(self.enddate)} is later than the data end at {str(self.data_end_date)}.")

        if datetime.strptime(str(self.startdate), startdate_fmt) > datetime.strptime(str(self.enddate), enddate_fmt):
            raise ValueError(f"Start date {str(self.startdate)} is later than the end date at {str(self.enddate)}.")

        if datetime.strptime(str(self.data_start_date), start_date_fmt) > datetime.strptime(
            str(self.data_end_date), end_date_fmt
        ):
            raise ValueError(
                f"Data start date {str(self.data_start_date)} is later than the data end at {str(self.data_end_date)}."
            )

    @staticmethod
    def _shift_time_dataset(data):
        """Shift time of a dataset back one month"""
        newtime = pd.DatetimeIndex(data.time.values) + pd.DateOffset(months=-1)
        return data.assign_coords(time=newtime)

    @staticmethod
    def _split_date(datestr, timedefault="0000"):
        """Splits a date in YYYYMMDD:HHMM format into date and time strings"""
        dd = str(datestr).split("T")
        timestr = dd[1] if "T" in str(datestr) else timedefault
        return dd[0], timestr

    def _compute_timeaxis(self, timestep, savefreq, chunkfreq, skiplast=False):
        """Compute timeaxis and chunk start and end dates and indices."""
        if savefreq is None:
            savefreq = timestep
        if timestep is None:
            timestep = savefreq
        if chunkfreq is None:
            chunkfreq = timestep

        shiftmonth = self.timeshift

        if shiftmonth and savefreq != "M":
            raise ValueError("shiftmonth option requested but data are not saved at monthly frequency!")

        offset = len(pd.date_range(str(self.data_start_date), str(self.startdate), freq=timestep)) - 1

        sdate = pd.Timestamp(str(self.startdate))
        edate = pd.Timestamp(str(self.enddate))

        if shiftmonth:
            edate = edate + pd.offsets.MonthBegin()

        if skiplast:
            edate = edate - pd.Timedelta(1, unit=timestep)

        dates = pd.date_range(sdate, edate, freq=timestep)
        idx = range(len(dates))
        ts = pd.Series(idx, index=dates)

        if timestep != savefreq:
            if shiftmonth:
                idx = ts.resample(savefreq).min().values
                ts = pd.Series(idx[1:], index=dates[idx[0:-1]])
                idx = idx[0:-1]
            else:
                idx = ts.resample(savefreq).min().values
                ts = pd.Series(idx, index=dates[idx])

        tsr = ts.resample(chunkfreq)
        sidx = tsr.min().values
        eidx = tsr.max().values
        chunksize = tsr.count().values

        if shiftmonth:
            sdate = dates[sidx] - pd.offsets.MonthBegin()
            edate = dates[eidx] - pd.offsets.MonthBegin()
        else:
            sdate = dates[sidx]
            edate = dates[eidx]

        if self.bridge_end_date:
            bridge_start = pd.Timestamp(str(self.bridge_start_date))
            bridge_end = pd.Timestamp(str(self.bridge_end_date))
            chunktype = np.where((sdate >= bridge_start) & (sdate <= bridge_end), 1, 0)
            chunktype_end = np.where((edate >= bridge_start) & (edate <= bridge_end), 1, 0)
        else:
            chunktype = np.zeros(len(dates), dtype=int)
            chunktype_end = np.zeros(len(dates), dtype=int)

        self.timeaxis = dates[idx]
        self.chk_start_idx = sidx + offset
        self.chk_start_date = sdate
        self.chk_end_idx = eidx + offset
        self.chk_end_date = edate
        self.chk_size = chunksize
        self._npartitions = len(self.chk_start_date)
        self.chk_type = chunktype

        if not np.array_equal(self.chk_type, chunktype_end):
            raise ValueError("Chunk size is not aligned with bridge_start_data and bridge_end_data. Fix your catalog!")

    @staticmethod
    def _todatetime(datestr):
        """Converts a date string to a datetime object"""
        return pd.Timestamp(str(datestr))

    @staticmethod
    def _read_bridge_date(obj):
        """Reads the bridge end date from a file or string"""
        if obj and obj != "complete" and os.path.isfile(obj):
            with open(obj, "r") as file:
                date = file.read()
            date = pd.Timestamp(date.strip())
            return date.strftime("%Y%m%dT%H%M")
        else:
            return obj

    @staticmethod
    def _floor_datetime(dt, freq, output_format="%Y%m%dT%H%M"):
        """Floors a datetime object to the specified pandas frequency"""
        if dt is None:
            return dt
        if dt == "complete":
            return dt

        if not isinstance(dt, pd.Timestamp):
            dt = pd.Timestamp(str(dt))

        if freq in ["M", "MS"]:
            dt = pd.Timestamp(dt.year, dt.month, 1)
        elif freq in ["Y", "YS"]:
            dt = pd.Timestamp(dt.year, 1, 1)
        elif freq in ["ME", "YE"]:
            raise KeyError(f"Freq {freq} is not supported, please use {freq[0]}S")
        elif "W" in freq:
            raise KeyError("Weekly frequency not supported")
        else:
            dt = dt.floor(freq)

        return dt.strftime(output_format)
