#!/usr/bin/env python3
import argparse
import os
import sys
from dask.distributed import Client, LocalCluster
import matplotlib.pyplot as plt
import xarray as xr
import healpy as hp

from aqua import Reader
from aqua import __version__ as aqua_version
from aqua.logger import log_configure
from aqua.util import get_arg, load_yaml, create_folder

xr.set_options(keep_attrs=True)


def parse_argument(args):
    """Parse command line argument"""

    parser = argparse.ArgumentParser(description='Cumulative precipitation')

    parser.add_argument("-c", "--config",
                        type=str, required=False,
                        help="yaml configuration file")
    parser.add_argument('-n', '--nworkers', type=int,
                        help='number of dask distributed workers')
    parser.add_argument("--loglevel", "-l", type=str,
                        required=False, help="loglevel")

    # These will override the first one in the config file if provided
    parser.add_argument("--catalog", type=str,
                        required=False, help="catalog name")
    parser.add_argument("--model", type=str,
                        required=False, help="model name")
    parser.add_argument("--exp", type=str,
                        required=False, help="experiment name")
    parser.add_argument("--source", type=str,
                        required=False, help="source name")
    parser.add_argument("--outputdir", type=str,
                        required=False, help="output directory")
                        
    parser.add_argument("--save", type=bool,
                        required=False, help="save the statistic",
                        default=False)

    return parser.parse_args(args)


if __name__ == '__main__':

    args = parse_argument(sys.argv[1:])

    loglevel = get_arg(args, 'loglevel', 'WARNING')
    logger = log_configure(log_level=loglevel, log_name='ETCCDI')
    logger.info('Running with AQUA version {}'.format(aqua_version))

    nworkers = get_arg(args, 'nworkers', None)
    save_statistic = get_arg(args, 'save', False)

    # Dask distributed cluster
    if nworkers:
        cluster = LocalCluster(n_workers=nworkers, threads_per_worker=1)
        client = Client(cluster)
        logger.info(f"Running with {nworkers} dask distributed workers.")

    # Load configuration file
    file = get_arg(args, "config", "config.yaml")
    logger.info(f"Reading configuration file {file}")
    config = load_yaml(file)

    # Read from configuration file
    catalog = get_arg(args, "catalog", config.get("catalog"))
    model = get_arg(args, "model", config.get("model"))
    exp = get_arg(args, "exp", config.get("exp"))
    source = get_arg(args, "source", config.get("source"))

    outputdir = get_arg(args, "outputdir", config.get("outputdir"))
    create_folder(outputdir, loglevel=loglevel)

    var = config.get("var", "2t")

    res = config.get("res", None)
    freq = config.get("freq", None)
    aggregation = config.get("aggregation", "D")

    index = config.get("index", "unknown")
    logger.info(f"ETCCDI index: {index}")

    startdate = config.get("startdate", None)
    enddate = config.get("enddate", None)
    logger.debug(f"Start date: {startdate}, End date: {enddate}")

    if not startdate and not enddate:
        logger.warning("No startdate and enddate provided. Using the first and last date in the catalog.")

    reader = Reader(catalog=catalog, model=model, exp=exp, source=source,
                    regrid=res, loglevel=loglevel,
                    startdate=startdate, enddate=enddate,
                    streaming=True, aggregation=aggregation)
    
    # Initial date (shape is YYYYMMDD)
    year = startdate[0:4]
    month = int(startdate[4:6])
    logger.info(f"Analysis of year: {year}")
    logger.debug(f"Initial month: {month}")
    
    etccdi = None
    while (etccdi is None or data is not None):
        data = reader.retrieve(var=var)
        if data is not None:
            logger.info(f"Retrieving {data.time.values[0]} to {data.time.values[-1]}")

            new_month = data.time.values[0].astype('datetime64[M]').astype(int) % 12 + 1
            if new_month != month:
                logger.info(f"New month: {new_month}")

                # Save the result
                etccdi.to_netcdf(os.path.join(outputdir, f"{model}_{exp}_{source}_{var}_ETCCDI_{index}_{year}_{month}.nc"))
                logger.info(f"ETCCDI index saved to {outputdir}")
                etccdi = None
                month = new_month

            # Find the maximum daily value
            max_daily = data[var].max(dim='time')

            if save_statistic:
                max_daily.to_netcdf(os.path.join(outputdir,
                                                 f"{model}_{exp}_{source}_{var}_max_daily_{data.time.values[0]}.nc"))

            # Set True for days with maximum daily value > 25 + 273.15 K
            hot_days = xr.where(max_daily > 25 + 273.15, 1, 0)

            # Sum the number of hot days in the year
            if etccdi is None:
                etccdi = hot_days
            else:
                etccdi += hot_days
        else:
            logger.info("No more data to retrieve.")
            # Save final month
            if etccdi is not None:
                etccdi.to_netcdf(os.path.join(outputdir, f"{model}_{exp}_{source}_{var}_ETCCDI_{index}_{year}_{month}.nc"))
                logger.info(f"ETCCDI index saved to {outputdir}")
            break

    # Producing the final result
    index_res = None
    for i in range(1, 13):
        res = xr.open_mfdataset(os.path.join(outputdir, f"{model}_{exp}_{source}_{var}_ETCCDI_{index}_{year}_{i}.nc"))
        logger.debug(f"Opening {model}_{exp}_{source}_{var}_ETCCDI_{index}_{year}_{i}.nc")
        res = res[var]
    if index_res is None:
        index_res = res
    else:
        index_res += res

    index_res.to_netcdf(os.path.join(outputdir, f"{model}_{exp}_{source}_{var}_ETCCDI_{index}_{year}.nc"))
    logger.info(f"ETCCDI final index saved to {outputdir}")

    # Produce the plot
    title = f"{model} {exp} ETCCDI {index} {year}"
    hp.mollview(index_res, title=title, flip='geo', nest=True, unit='days', cmap='Blues')
    plt.savefig(os.path.join(outputdir, f"{model}_{exp}_{source}_{var}_ETCCDI_{index}_{year}.png"))

    