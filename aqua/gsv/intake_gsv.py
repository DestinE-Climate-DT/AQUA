import sys
import xarray as xr
from intake.source import base

from datetime import datetime
from dateutil.relativedelta import relativedelta

# Test if FDB5 binary library is available
try:
    from gsv.retriever import GSVRetriever
    gsv_available = True
except RuntimeError:
    gsv_available = False


class GSVSource(base.DataSource):
    container = 'xarray'
    name = 'gsv'
    version = '0.0.1'
    partition_access = True

    def __init__(self, request, step, startdate=None, enddate=None, var='167', metadata=None, **kwargs):
        self._request = request
        self._kwargs = kwargs
        if startdate and enddate:
            self._dates = make_date_list(startdate, enddate, step=step)
            ndates = len(self._dates)
        else:
            ndates = 1  # read only one
            self._dates = None
        self._var = var
        self._npartitions = ndates
        if gsv_available:
            self.gsv = GSVRetriever()
        else:
            raise ImportError("FDB5 binary library not present on system or outdated.")
        self._dataset = None
        super(GSVSource, self).__init__(metadata=metadata)

    def _get_schema(self):
        # if self._dataset is None:
        #     self._get_partition(0)
        return base.Schema(
            datashape=None,
            dtype=xr.Dataset,
            # shape=self._dataset.shape,
            shape=None,
            npartitions=self._npartitions,
            extra_metadata={},
        )

    def _get_partition(self, i):
        if self._dates:
            self._request["date"] = self._dates[i]
        if self._var:  # if no var provided keep the default in the catalogue
            self._request["param"] = self._var
        dataset = self.gsv.request_data(self._request)
        return dataset

    def read(self):
        ds = [self._get_partition(i) for i in range(self._npartitions)]
        ds = xr.concat(ds, dim='time')
        return ds

    def to_dask(self):
        return self.read_chunked()

    # def _load(self):
    #     self._dataset = self._get_partition(0)


def make_date_list(start, end, step="M", numsteps=1):

    if step == "M":
        delta = relativedelta(months=numsteps)
    elif step == "D":
        delta = relativedelta(days=numsteps)
    elif step == "H":
        delta = relativedelta(hours=numsteps)
    else:
        sys.exit("step not recognized")

    start_date = datetime.strptime(start, '%Y%m%d')
    end_date = datetime.strptime(end, '%Y%m%d')

    date_list = []
    current_date = start_date

    while current_date <= end_date:
        date_string = current_date.strftime('%Y%m%d')
        date_list.append(date_string)
        current_date += delta

    return date_list
