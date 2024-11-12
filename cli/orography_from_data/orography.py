#!/usr/bin/env python3
"""Script to calculate orography from data."""

import xarray as xr
from aqua import Reader
from aqua.logger import log_configure
from aqua.util import create_folder

xr.set_options(keep_attrs=True)

catalog = "nextgems4"
model = "IFS-FESOM"
exp = "historical-1990"
source = "2D_monthly_healpix512"
loglevel = "INFO"
res = "r100"
var = "z"
outputdir = "./output"


def main():
    logger = log_configure(loglevel, 'Orography')

    reader = Reader(catalog=catalog, model=model, exp=exp, source=source,
                    regrid=res, loglevel=loglevel)

    data = reader.retrieve(var=var)

    if res is not None:
        data = reader.regrid(data)

    orography = data[var].isel(time=0)

    create_folder(outputdir, loglevel=loglevel)

    orography.to_netcdf(f"{outputdir}/orography_{model}_{exp}_{source}_{res}.nc")
    logger.info(f"Orography saved to {outputdir}/orography_{model}_{exp}_{source}_{res}.nc")


if __name__ == "__main__":

    main()
