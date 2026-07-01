from aqua.core.gsv.base import IntakeFDBSourceAdapter
from aqua.core.gsv.datatypes import FDB
from aqua.core.gsv.readers import GSVDatasetReader


class IntakeGSVSource(IntakeFDBSourceAdapter):
    """Open GSV source file."""

    container = "xarray:Dataset"
    name = "gsv"
    version = ""

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

        data = FDB(
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
        super(IntakeGSVSource, self).__init__(metadata=metadata)
