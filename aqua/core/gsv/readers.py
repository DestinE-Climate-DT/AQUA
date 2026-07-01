from intake import BaseReader

from aqua.core.gsv.datatypes import FDB
from aqua.core.gsv.openers import open_gsv


class GSVDatasetReader(BaseReader):
    output_instance = "xarray:Dataset"
    imports = {"aqua.core.gsv.open_gsv:open_gsv"}
    optional_imports = {}
    func = "open_gsv"
    implements = {
        FDB,
    }

    def _read(self, data, **kwargs):

        params = dict(
            data_start_date=data.data_start_date,
            data_end_date=data.data_end_date,
            bridge_start_date=data.bridge_start_date,
            bridge_end_date=data.bridge_end_date,
            hpc_expver=data.hpc_expver,
            timestyle=data.timestyle,
            chunks=data.chunks,
            savefreq=data.savefreq,
            timestep=data.timestep,
            timeshift=data.timeshift,
            startdate=data.startdate,
            enddate=data.enddate,
            var=data.var,
            metadata=data.metadata,
            level=data.level,
            switch_eccodes=data.switch_eccodes,
            loglevel=data.loglevel,
            engine=data.engine,
            databridge=data.databridge,
        )
        params.update(kwargs)
        return open_gsv(data.request, **params)
