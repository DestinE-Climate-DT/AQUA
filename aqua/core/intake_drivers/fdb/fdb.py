from .base import IntakeFDBSourceAdapter
from .datatypes import GSV, Polytope, Z3FDB
from .readers import GSVDatasetReader, PolytopeDatasetReader, Z3FDBDatasetReader


class IntakeFDBSource(IntakeFDBSourceAdapter):
    """Open FDB source file."""

    container = "xarray:Dataset"
    name = "gsv"  # This is still called gsv for compatibility with the catalog
    version = ""
    # instancecount = 0

    def __init__(
        self,
        request,
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
        metadata=None,
        level=None,
        switch_eccodes=False,
        loglevel="WARNING",
        engine=None,
        databridge=None,
        **kwargs,
    ):

        # TODO: remove this
        # IntakeFDBSource.instancecount += 1
        # print("Number of FDB source calls = " + str(IntakeFDBSource.instancecount))

        if engine == "polytope-new":
            data = Polytope(
                request,
                data_start_date=data_start_date,
                data_end_date=data_end_date,
                bridge_start_date=bridge_start_date,
                bridge_end_date=bridge_end_date,
                hpc_expver=hpc_expver,
                timestyle=timestyle,
                chunks=chunks,
                savefreq=savefreq,
                timestep=timestep,
                timeshift=timeshift,
                startdate=startdate,
                enddate=enddate,
                var=var,
                metadata=metadata,
                level=level,
                loglevel=loglevel,
                engine=engine,
                databridge=databridge,
                **kwargs,
            )
            reader = PolytopeDatasetReader(data, **kwargs)
        elif engine == "z3fdb":
            data = Z3FDB(
                request,
                data_start_date=data_start_date,
                data_end_date=data_end_date,
                bridge_start_date=bridge_start_date,
                bridge_end_date=bridge_end_date,
                hpc_expver=hpc_expver,
                timestyle=timestyle,
                chunks=chunks,
                savefreq=savefreq,
                timestep=timestep,
                timeshift=timeshift,
                startdate=startdate,
                enddate=enddate,
                var=var,
                metadata=metadata,
                level=level,
                loglevel=loglevel,
                engine=engine,
                databridge=databridge,
                **kwargs,
            )
            reader = Z3FDBDatasetReader(data, **kwargs)
        else:
            data = GSV(
                request,
                data_start_date=data_start_date,
                data_end_date=data_end_date,
                bridge_start_date=bridge_start_date,
                bridge_end_date=bridge_end_date,
                hpc_expver=hpc_expver,
                timestyle=timestyle,
                chunks=chunks,
                savefreq=savefreq,
                timestep=timestep,
                timeshift=timeshift,
                startdate=startdate,
                enddate=enddate,
                var=var,
                metadata=metadata,
                level=level,
                switch_eccodes=switch_eccodes,
                loglevel=loglevel,
                engine=engine,
                databridge=databridge,
                **kwargs,
            )
            reader = GSVDatasetReader(data, **kwargs)
        self.reader = reader
        self.reader.metadata = metadata
        super(IntakeFDBSource, self).__init__(metadata=metadata)
