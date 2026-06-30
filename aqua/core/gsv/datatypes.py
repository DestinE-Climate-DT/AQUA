from intake.readers import BaseData


class FDB(BaseData):
    """Datatypes that use a FDB request"""

    structure = {"fdb_request"}

    def __init__(
        self,
        request,
        data_start_date,
        data_end_date,
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
        switch_eccodes=False,
        loglevel="WARNING",
        engine=None,
        databridge=None,
        metadata: dict | None = None,
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
        self.switch_eccodes = switch_eccodes
        self.loglevel = loglevel
        self.engine = engine
        self.databridge = databridge

        super().__init__(metadata)
