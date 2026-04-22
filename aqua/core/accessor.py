"Module defining a new aqua accessor to extend xarray"

import xarray as xr

from .graphics import plot_single_map
from .reader import Reader


# For now not distinguishing between dataarray and dataset methods
@xr.register_dataset_accessor("aqua")
@xr.register_dataarray_accessor("aqua")
class AquaAccessor:
    def __init__(self, xarray_obj):
        self._obj = xarray_obj
        self.instance = Reader.instance  # by default use the latest available instance of the Reader class

    def set_default(self, reader):
        """
        Sets a specific reader instance as default for further accessor uses.
        Arguments:
            reader (object of class Reader): the reader to set as default

        Returns:
            None
        """
        reader.set_default()  # set this also as the next Reader default
        self.instance = reader  # set as reader to be used for the accessor
        return self._obj

    def plot_single_map(self, **kwargs):
        """Plot contour or pcolormesh map of a single variable."""
        plot_single_map(self._obj, **kwargs)

    def select_area(self, **kwargs):
        """Extract a custom area"""
        return self.instance.select_area(self._obj, **kwargs)

    def regrid(self, **kwargs):
        """Perform regridding of the input dataset."""
        return self.instance.regrid(self._obj, **kwargs)

    # Time stat operations
    def timmean(self, **kwargs):
        """Perform time averaging."""
        return self.instance.timmean(self._obj, **kwargs)

    def timmax(self, **kwargs):
        """Perform time maximum."""
        return self.instance.timmax(self._obj, **kwargs)

    def timmin(self, **kwargs):
        """Perform time minimum."""
        return self.instance.timmin(self._obj, **kwargs)

    def timstd(self, **kwargs):
        """Perform time standard deviation."""
        return self.instance.timstd(self._obj, **kwargs)

    def timfirst(self, **kwargs):
        """Perform time first element."""
        return self.instance.timfirst(self._obj, **kwargs)

    def timlast(self, **kwargs):
        """Perform time last element."""
        return self.instance.timlast(self._obj, **kwargs)

    def timstat(self, **kwargs):
        """Perform time statistics."""
        return self.instance.timstat(self._obj, **kwargs)

    # Field stat operations
    def fldstat(self, **kwargs):
        """Perform a weighted field statistic."""
        return self.instance.fldstat(self._obj, **kwargs)

    def fldmean(self, **kwargs):
        """Perform a weighted global average."""
        return self.instance.fldmean(self._obj, **kwargs)

    def fldmax(self, **kwargs):
        """Return the field maximum."""
        return self.instance.fldmax(self._obj, **kwargs)

    def fldmin(self, **kwargs):
        """Return the field minimum."""
        return self.instance.fldmin(self._obj, **kwargs)

    def fldstd(self, **kwargs):
        """Perform a weighted field standard deviation."""
        return self.instance.fldstd(self._obj, **kwargs)

    def fldsum(self, **kwargs):
        """Perform a weighted field sum."""
        return self.instance.fldsum(self._obj, **kwargs)

    def fldintg(self, **kwargs):
        """Perform a weighted field integral."""
        return self.instance.fldintg(self._obj, **kwargs)

    def fldarea(self, **kwargs):
        """Return the sum of area cells where data is not null."""
        return self.instance.fldarea(self._obj, **kwargs)

    # Other operations
    def vertinterp(self, **kwargs):
        """A basic vertical interpolation."""
        return self.instance.vertinterp(self._obj, **kwargs)

    def detrend(self, **kwargs):
        """A basic detrending."""
        return self.instance.detrend(self._obj, **kwargs)

    def stream(self, **kwargs):
        """Stream the dataset."""
        return self.instance.stream(self._obj, **kwargs)

    def histogram(self, **kwargs):
        """Compute a histogram (or pdf) of the data."""
        return self.instance.histogram(self._obj, **kwargs)
