from intake.readers import BaseData


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
        switch_eccodes=False,
        **kwargs,
    ):
        self.switch_eccodes = switch_eccodes

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


class Z3FDB(FDB):
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
