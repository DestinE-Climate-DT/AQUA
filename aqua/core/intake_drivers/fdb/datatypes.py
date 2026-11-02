from copy import copy

from intake.readers import BaseData

from aqua.core.logger import log_configure

from .openers.dates import FDBDatesMixin
from .openers.timeutil import FDBTimeMixin


class FDB(BaseData):
    """Base Datatype that uses a FDB request"""

    structure = {"fdb_request"}

    def __init__(
        self,
        request,
        metadata=None,
        data_start_date=None,
        data_end_date=None,
        bridge_start_date=None,
        bridge_end_date=None,
        hpc_expver=None,
        timestyle="date",
        chunks="S",
        savefreq="h",
        timestep="h",
        timeshift=None,
        startdate=None,
        enddate=None,
        var=None,
        level=None,
        loglevel="WARNING",
        engine=None,
        databridge=None,
        config_fdb=None,
        **kwargs,
    ):

        self.request = request
        self.data_start_date = data_start_date
        self.data_end_date = data_end_date
        self.bridge_start_date = bridge_start_date
        self.bridge_end_date = bridge_end_date
        self.hpc_expver = hpc_expver
        self.timestyle = timestyle
        self.chunks = chunks
        self.savefreq = savefreq
        self.timestep = timestep
        self.timeshift = timeshift
        self.startdate = startdate
        self.enddate = enddate
        self.var = var
        self.level = level
        self.loglevel = loglevel
        self.engine = engine
        self.databridge = databridge
        self.config_fdb = config_fdb

        super().__init__(metadata)

    def to_dict(self):
        """Serialize datatype instance attributes to a dict for the openers."""
        exclude = {"request", "structure"}
        return {k: v for k, v in self.__dict__.items() if k not in exclude and not k.startswith("_")}


class GSV(FDB):
    """Derived Datatype from FDB that uses a GSV request"""

    structure = {"gsv_request"}

    def __init__(
        self,
        request,
        **kwargs,
    ):

        super().__init__(request, **kwargs)


class Polytope(FDB):
    """Derived Datatype from FDB that uses a Polytope request"""

    structure = {"polytope_request"}

    def __init__(
        self,
        request,
        **kwargs,
    ):
        super().__init__(request, **kwargs)


class Z3FDB(FDB, FDBDatesMixin, FDBTimeMixin):
    """Derived Datatype from FDB that uses a z3fdb request"""

    structure = {"z3fdb_request"}

    def __init__(
        self,
        request,
        **kwargs,
    ):

        super().__init__(request, **kwargs)

        if "levels" in self.metadata:
            self.level_values = self.metadata["levels"]
        else:
            self.level_values = None

    def _resolve_dates(self):
        """Resolve and validate availability on a fresh copy for each read."""
        data = copy(self)
        data.request = self.request.copy()
        data._request = data.request
        data.engine = "z3fdb"
        data.logger = log_configure(log_level=self.loglevel, log_name="Z3FDB")
        config_is_path = self.config_fdb and self.config_fdb.endswith((".yaml", ".yml"))
        data.fdbhome = self.metadata.get("fdb_home")
        data.fdbpath = self.metadata.get("fdb_path")
        if self.config_fdb:
            data.fdbhome = None if config_is_path else self.config_fdb
            data.fdbpath = self.config_fdb if config_is_path else None
        data.fdb_info_file = self.metadata.get("fdb_info_file")
        data._define_start_end_dates(self.data_start_date, self.data_end_date, self.bridge_start_date, self.bridge_end_date)
        data._define_retrieve_dates(self.startdate, self.enddate)
        data._raw_data_start_date = data.data_start_date
        for attr in ("data_start_date", "data_end_date", "bridge_start_date", "bridge_end_date", "startdate", "enddate"):
            setattr(data, attr, data._floor_datetime(getattr(data, attr), self.savefreq or self.timestep))
        data._check_dates()
        return data
