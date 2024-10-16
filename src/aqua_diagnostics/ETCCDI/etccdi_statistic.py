#!/usr/bin/env python3
import argparse
import os
import sys
import dask
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
    parser.add_argument('-o', '--overwrite', action="store_true",
                        help='overwrite existing output')

    return parser.parse_args(args)


if __name__ == '__main__':

    args = parse_argument(sys.argv[1:])

    loglevel = get_arg(args, 'loglevel', 'WARNING')
    logger = log_configure(log_level=loglevel, log_name='ETCCDI')
    logger.info('Running with AQUA version {}'.format(aqua_version))

    nworkers = get_arg(args, 'nworkers', None)
    save_statistic = get_arg(args, 'save', False)
    overwrite = get_arg(args, 'overwrite', False)

    # Dask distributed cluster
    if nworkers:
        cluster = LocalCluster(n_workers=nworkers, threads_per_worker=1)
        client = Client(cluster)
        logger.info(f"Running with {nworkers} dask distributed workers.")
    else:
        dask.config.set(scheduler='synchronous')

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

    aggregation = config.get("aggregation", None)

    index = config.get("index", "unknown")
    logger.info(f"ETCCDI index: {index}")

    if index == "su":
        logger.info("ETCCDI index: Summer days")
        var = '2t'
        if aggregation is None:
            aggregation = 'D'
        condition = 273.15 + 25 # 25 degree Celsius
        condition_sign = 'bigger_than'
        statistic = 'max'
        cmap = 'Blues'
    elif index == "fd":
        logger.info("ETCCDI index: Frost days")
        var = '2t'
        if aggregation is None:
            aggregation = 'D'
        condition = 273.15 # 0 degree Celsius
        condition_sign = 'smaller_than'
        statistic = 'min'
        cmap = 'Blues'
    elif index == "id":
        logger.info("ETCCDI index: Ice days")
        var = '2t'
        if aggregation is None:
            aggregation = 'D'
        condition = 273.15
        condition_sign = 'smaller_than'
        statistic = 'max'
        cmap = 'Blues'
    elif index == "tr":
        logger.info("ETCCDI index: Tropical nights")
        var = '2t'
        if aggregation is None:
            aggregation = 'D'
        condition = 273.15 + 20
        condition_sign = 'bigger_than'
        statistic = 'min'
        cmap = 'Blues'
    else:
        raise ValueError("Index is not known. Please provide the index in the configuration file.")

    year = config.get("year", None)
    if year is None:
        raise ValueError("Year is not provided in the configuration file.")
    startdate = f"{year}0101"
    enddate = f"{year}1231"

    reader = Reader(catalog=catalog, model=model, exp=exp, source=source,
                    loglevel=loglevel,
                    startdate=startdate, enddate=enddate,
                    streaming=True, aggregation=aggregation)
    
    month = int(startdate[4:6])
    logger.info(f"Analysis of year: {year}")
    
    etccdi = None
    while (etccdi is None or data is not None):
        data = reader.retrieve(var=var)
        if data is not None:
            logger.info(f"Retrieving {data.time.values[0]} to {data.time.values[-1]}")

            new_month = data.time.values[0].astype('datetime64[M]').astype(int) % 12 + 1
            if new_month != month:
                logger.info(f"New month: {new_month}")

                # Save the result. If overwriting is enabled, the file will be overwritten
                etccdi_filename = os.path.join(outputdir, f"{model}_{exp}_{source}_{var}_{index}ETCCDI_{year}_{month}.nc")

                if os.path.exists(etccdi_filename) and not overwrite:
                    logger.info(f"File {etccdi_filename} exists. Skip saving.")
                elif os.path.exists(etccdi_filename) and overwrite:
                    logger.warning(f"File {etccdi_filename} exists. Overwriting.")
                    os.remove(etccdi_filename)
                    etccdi.to_netcdf(etccdi_filename)
                    logger.info(f"ETCCDI index saved to {outputdir}")
                else:
                    etccdi.to_netcdf(etccdi_filename)
                    logger.info(f"ETCCDI index saved to {outputdir}")

                etccdi = None
                month = new_month

            # Evaluate the statistic
            if statistic == 'max':
                statistic_daily = data[var].max(dim='time')
            elif statistic == 'min':
                statistic_daily = data[var].min(dim='time')
            else:
                raise ValueError("Statistic is not supported.")

            if save_statistic:
                statistic_filename = os.path.join(outputdir, f"{model}_{exp}_{source}_{var}_{statistic}_daily_{data.time.values[0]}.nc")

                if os.path.exists(statistic_filename) and not overwrite:
                    logger.info(f"File {statistic_filename} exists. Skip saving.")
                elif os.path.exists(statistic_filename) and overwrite:
                    logger.warning(f"File {statistic_filename} exists. Overwriting.")
                    os.remove(statistic_filename)
                    statistic_daily.to_netcdf(statistic_filename)
                else:
                    statistic_daily.to_netcdf(statistic_filename)

            # Set 1 if the condition is met
            if condition_sign == 'bigger_than':
                condition_days = xr.where(statistic_daily > condition, 1, 0)
            elif condition_sign == 'smaller_than':
                condition_days = xr.where(statistic_daily < condition, 1, 0)
            else:
                raise ValueError("Condition sign is not supported.")

            # Sum the number of hot days in the year
            if etccdi is None:
                etccdi = condition_days
            else:
                etccdi += condition_days
        else:
            logger.info("No more data to retrieve.")
            # Save final month
            if etccdi is not None:
                etccdi_filename = os.path.join(outputdir, f"{model}_{exp}_{source}_{var}_{index}ETCCDI_{year}_{month}.nc")

                if os.path.exists(etccdi_filename) and not overwrite:
                    logger.info(f"File {etccdi_filename} exists. Skip saving.")
                elif os.path.exists(etccdi_filename) and overwrite:
                    logger.warning(f"File {etccdi_filename} exists. Overwriting.")
                    os.remove(etccdi_filename)
                    etccdi.to_netcdf(etccdi_filename)
                    logger.info(f"ETCCDI index saved to {outputdir}")
                else:
                    etccdi.to_netcdf(etccdi_filename)
                    logger.info(f"ETCCDI index saved to {outputdir}")
            break

    # Producing the final result
    logger.info("Producing the final results")
    index_res = None
    for i in range(1, 13):
        try:
            res = xr.open_mfdataset(os.path.join(outputdir, f"{model}_{exp}_{source}_{var}_{index}ETCCDI_{year}_{i}.nc"))
            logger.debug(f"Opening file {model}_{exp}_{source}_{var}_{index}ETCCDI_{year}_{i}.ncc")
        except FileNotFoundError:
            raise FileNotFoundError(f"File {model}_{exp}_{source}_{var}_ETCCDI_{index}_{year}_{i}.nc not found.")
        res = res[var]
        if index_res is None:
            index_res = res
        else:
            index_res += res

    etccdi_final_filename = os.path.join(outputdir, f"{model}_{exp}_{source}_{var}_{index}ETCCDI_{year}.nc")
    if os.path.exists(etccdi_final_filename) and not overwrite:
        logger.info(f"File {etccdi_final_filename} exists. Skip saving.")
    elif os.path.exists(etccdi_final_filename) and overwrite:
        logger.warning(f"File {etccdi_final_filename} exists. Overwriting.")
        os.remove(etccdi_final_filename)
        index_res.to_netcdf(etccdi_final_filename)
        logger.info(f"ETCCDI final index saved to {outputdir}")
    else:
        index_res.to_netcdf(etccdi_final_filename)
        logger.info(f"ETCCDI final index saved to {outputdir}")

    # Produce the plot
    title = f"{model} {exp} {index}ETCCDI {year}"
    hp.mollview(index_res, title=title, flip='geo', nest=True, unit='days', cmap=cmap)
    filename_fig = os.path.join(outputdir, f"{model}_{exp}_{source}_{var}_{index}ETCCDI_{year}.png")
    plt.savefig(filename_fig)
    logger.info(f"Plot saved to {filename_fig}")
