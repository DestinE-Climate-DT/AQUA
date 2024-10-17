#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
AQUA teleconnections command line interface for a single dataset.
It evaluates the ENSO teleconnections and performs a boostrap to
evaluate the concordance of the teleconnections with ERA5.
'''
import argparse
import os
import sys
import gc

import matplotlib.pyplot as plt
import xarray as xr
from dask.distributed import Client, LocalCluster

from aqua import __version__ as aquaversion
from aqua.graphics import plot_single_map
from aqua.util import load_yaml, get_arg, create_folder
from aqua.exceptions import NoDataError, NotEnoughDataError
from aqua.logger import log_configure
from teleconnections import __version__ as telecversion
from teleconnections.bootstrap import bootstrap_teleconnections, build_confidence_mask
from teleconnections.tc_class import Teleconnection
from teleconnections.tools import set_filename

xr.set_options(keep_attrs=True)


def parse_arguments(cli_args):
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(description='Teleconnections CLI')

    parser.add_argument('-c', '--config', type=str,
                        help='yaml configuration file')
    parser.add_argument('-n', '--nworkers', type=int,
                        help='number of dask distributed workers')
    parser.add_argument('-d', '--dry', action='store_true',
                        required=False,
                        help='if True, run is dry, no files are written')
    parser.add_argument('-l', '--loglevel', type=str,
                        help='log level [default: WARNING]')

    # This arguments will override the configuration file if provided
    parser.add_argument('--catalog', type=str, help='catalog name',
                        required=False)
    parser.add_argument('--model', type=str, help='model name',
                        required=False)
    parser.add_argument('--exp', type=str, help='experiment name',
                        required=False)
    parser.add_argument('--source', type=str, help='source name',
                        required=False)
    parser.add_argument('--outputdir', type=str, help='output directory',
                        required=False)
    parser.add_argument('--interface', type=str, help='interface to use',
                        required=False)

    return parser.parse_args(cli_args)


if __name__ == '__main__':

    args = parse_arguments(sys.argv[1:])
    loglevel = get_arg(args, 'loglevel', 'WARNING')
    logger = log_configure(log_name='Teleconnections Bootstrap CLI', log_level=loglevel)

    logger.info(f'Running AQUA v{aquaversion} Teleconnections diagnostic v{telecversion}')

    # change the current directory to the one of the CLI so that relative path works
    # before doing this we need to get the directory from wich the script is running
    execdir = os.getcwd()
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    if os.getcwd() != dname:
        os.chdir(dname)
        logger.info(f'Moving from current directory to {dname} to run!')

    # Dask distributed cluster
    nworkers = get_arg(args, 'nworkers', None)
    if nworkers:
        cluster = LocalCluster(n_workers=nworkers, threads_per_worker=1)
        client = Client(cluster)
        logger.info(f"Running with {nworkers} dask distributed workers.")

    # Read configuration file
    file = get_arg(args, 'config', 'cli_config_atm.yaml')
    logger.info('Reading configuration yaml file: {}'.format(file))
    config = load_yaml(file)

    # if dry we're not saving any file, debug mode
    dry = get_arg(args, 'dry', False)
    if dry:
        logger.warning('Dry run, no files will be written')
        savefig = False
        savefile = False
    else:
        logger.debug('Saving files')
        savefig = True
        savefile = True

    try:
        outputdir = get_arg(args, 'outputdir', config['outputdir'])
        # if the outputdir is relative we need to make it absolute
        if not os.path.isabs(outputdir):
            outputdir = os.path.join(execdir, outputdir)
        outputnetcdf = os.path.join(outputdir, 'netcdf')
        outputpdf = os.path.join(outputdir, 'pdf')
        create_folder(outputnetcdf, loglevel=loglevel)
        create_folder(outputpdf, loglevel=loglevel)
    except KeyError:
        outputdir = None
        outputnetcdf = None
        outputpdf = None
        logger.error('Output directory not defined')

    configdir = config['configdir']
    logger.debug('configdir: %s', configdir)

    interface = get_arg(args, 'interface', config['interface'])
    logger.debug('Interface name: %s', interface)

    # Turning on/off the teleconnections
    # the try/except is used to avoid KeyError if the teleconnection is not
    # defined in the yaml file, since we have oceanic and atmospheric
    # configuration files
    NAO = config['teleconnections'].get('NAO', False)
    ENSO = config['teleconnections'].get('ENSO', False)

    teleclist = []
    if NAO:
        teleclist.append('NAO')
        logger.error('NAO bootstrap is not yet implemented, exiting')
        sys.exit(1)
    if ENSO:
        teleclist.append('ENSO')

    logger.debug('Teleconnections to be evaluated: %s', teleclist)

    # if exclusive we're running only the first model/exp/source combination
    # if model/exp/source are provided as arguments, we're overriding the
    # first model/exp/source combination
    models = config['models']

    models[0]['catalog'] = get_arg(args, 'catalog', models[0]['catalog'])
    models[0]['model'] = get_arg(args, 'model', models[0]['model'])
    models[0]['exp'] = get_arg(args, 'exp', models[0]['exp'])
    models[0]['source'] = get_arg(args, 'source', models[0]['source'])

    for telec in teleclist:
        logger.info('Running %s teleconnection', telec)
        # Getting generic configs
        months_window = config[telec].get('months_window', 3)
        full_year = config[telec].get('full_year', True)
        seasons = config[telec].get('seasons', None)

        ref_config = config['reference'][0]
        catalog_ref = ref_config.get('catalog', 'obs')
        model_ref = ref_config.get('model', 'ERA5')
        exp_ref = ref_config.get('exp', 'era5')
        source_ref = ref_config.get('source', 'monthly')
        regrid = ref_config.get('regrid', None)
        freq = ref_config.get('freq', None)
        logger.debug("setup: %s %s %s %s %s",
                     model_ref, exp_ref, source_ref, regrid, freq)

        try:
            tc_ref = Teleconnection(telecname=telec,
                                    configdir=configdir,
                                    catalog=catalog_ref,
                                    model=model_ref, exp=exp_ref, source=source_ref,
                                    regrid=regrid, freq=freq,
                                    months_window=months_window,
                                    outputdir=outputnetcdf,
                                    outputfig=outputpdf,
                                    savefig=savefig, savefile=savefile,
                                    interface=interface,
                                    loglevel=loglevel)
            tc_ref.retrieve()
        except NoDataError:
            logger.error('No data available for %s teleconnection', telec)
            continue
        except ValueError as e:
            logger.error('Error retrieving data for %s teleconnection: %s',
                         telec, e)
            continue
        except Exception as e:
            logger.error('Unexpected error retrieving data for %s teleconnection: %s',
                         telec, e)

        try:
            tc_ref.evaluate_index()
            ref_index = tc_ref.index
        except NotEnoughDataError:
            logger.error('Not enough data available for %s teleconnection', telec)
            continue
        except Exception as e:
            logger.error('Error evaluating index for %s teleconnection: %s', telec, e)
            continue

        # We now evaluate the regression and correlation
        # They are not saved, we just need them for comparison plots
        # so we save them as variables
        if full_year:
            try:
                ref_reg_full = tc_ref.evaluate_regression()
                ref_cor_full = tc_ref.evaluate_correlation()
            except NotEnoughDataError:
                logger.error('Not enough data available for %s teleconnection', telec)
                continue
        else:
            ref_reg_full = None
            ref_cor_full = None

        if seasons:
            logger.error("Seasons are not yet implemented for the bootstrap technique")
            ref_reg_season = None
            ref_cor_season = None
            continue
            # ref_reg_season = []
            # ref_cor_season = []
            # for i, season in enumerate(seasons):
            #     try:
            #         logger.info('Evaluating %s regression and correlation for %s season',
            #                     telec, season)
            #         reg = tc_ref.evaluate_regression(season=season)
            #         ref_reg_season.append(reg)
            #         cor = tc_ref.evaluate_correlation(season=season)
            #         ref_cor_season.append(cor)
            #     except NotEnoughDataError:
            #         logger.error('Not enough data available for %s teleconnection',
            #                     telec)
            #         continue
        else:
            ref_reg_season = None
            ref_cor_season = None

        ref_data = tc_ref.data

        del tc_ref
        gc.collect()

        # Model evaluation
        logger.debug('Models to be evaluated: %s', models)
        for mod in models:
            catalog = mod['catalog']
            model = mod['model']
            exp = mod['exp']
            source = mod['source']
            regrid = mod.get('regrid', None)
            freq = mod.get('freq', None)
            reference = mod.get('reference', False)
            startdate = mod.get('startdate', None)
            enddate = mod.get('enddate', None)

            logger.debug("setup: %s %s %s %s %s",
                         model, exp, source, regrid, freq)

            try:
                tc = Teleconnection(telecname=telec,
                                    configdir=configdir,
                                    catalog=catalog,
                                    model=model, exp=exp, source=source,
                                    regrid=regrid, freq=freq,
                                    months_window=months_window,
                                    outputdir=outputnetcdf,
                                    outputfig=outputpdf,
                                    savefig=savefig, savefile=savefile,
                                    startdate=startdate, enddate=enddate,
                                    interface=interface,
                                    loglevel=loglevel)
                tc.retrieve()
            except NoDataError:
                logger.error('No data available for %s teleconnection', telec)
                continue
            except ValueError as e:
                logger.error('Error retrieving data for %s teleconnection: %s',
                             telec, e)
                continue
            except Exception as e:
                logger.error('Unexpected error retrieving data for %s teleconnection: %s',
                             telec, e)

            try:
                tc.evaluate_index()
            except NotEnoughDataError:
                logger.error('Not enough data available for %s teleconnection', telec)
                continue
            except Exception as e:
                logger.error('Error evaluating index for %s teleconnection: %s', telec, e)
                continue

            if full_year:
                try:
                    reg_full = tc.evaluate_regression()
                    cor_full = tc.evaluate_correlation()
                except NotEnoughDataError:
                    logger.error('Not enough data available for %s teleconnection',
                                 telec)
                    continue
            else:
                reg_full = None
                cor_full = None

            if seasons:
                logger.error("Seasons are not yet implemented for the bootstrap technique")
                reg_season = None  # = []
                cor_season = None  # = []
                continue
                # for i, season in enumerate(seasons):
                #     try:
                #         logger.info('Evaluating %s regression and correlation for %s season',
                #                     telec, season)
                #         reg = tc.evaluate_regression(season=season)
                #         reg_season.append(reg)
                #         cor = tc.evaluate_correlation(season=season)
                #         cor_season.append(cor)
                #     except NotEnoughDataError:
                #         logger.error('Not enough data available for %s teleconnection',
                #                      telec)
                #         continue
            else:
                reg_season = None
                cor_season = None

            # Evaluate bootstrap
            # You can use **eval_kwargs for the season argument in a future development
            logger.info('Evaluating bootstrap for %s teleconnection', telec)
            l, u = bootstrap_teleconnections(map=reg_full, index=tc.index,
                                             index_ref=ref_index,
                                             data_ref=ref_data, statistic='reg',
                                             n_bootstraps=1000,
                                             loglevel=loglevel)

            if not dry:
                logger.info('Saving bootstrap results')
                filename = set_filename(tc.filename, fig_type='bootstrap')
                l_filename = filename + '_lower.nc'
                u_filename = filename + '_upper.nc'
                l_path = os.path.join(outputnetcdf, l_filename)
                u_path = os.path.join(outputnetcdf, u_filename)
                l.name = 'avg_tos'
                u.name = 'avg_tos'
                l.to_netcdf(l_path)
                u.to_netcdf(u_path)

            confidence_mask = build_confidence_mask(reg_full, l, u)

            if not dry:
                logger.info('Saving confidence mask')
                filename = set_filename(filename=tc.filename, fig_type='confidence_mask')
                mask_filename = filename + '.nc'
                mask_path = os.path.join(outputnetcdf, mask_filename)
                confidence_mask.name = 'avg_tos'
                confidence_mask.to_netcdf(mask_path)

            # Plotting
            if not dry:
                logger.info('Plotting concordance map')
                fig, ax = plot_single_map(reg_full, transform_first=True,
                                          return_fig=True, sym=True,
                                          title=f'{telec} {model} {exp} {source} Concordance')
                confidence_mask.where(confidence_mask == 1).plot.contour(levels=[0, 1], colors='none', hatches=['.', ''],
                                                                         add_colorbar=False, ax=ax)
                fig.tight_layout()
                filename = set_filename(tc.filename, fig_type='concordance')
                fig.savefig(os.path.join(outputpdf, filename + '.pdf'))

    logger.info('Teleconnections diagnostic finished.')
