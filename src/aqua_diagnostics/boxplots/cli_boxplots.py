import argparse
import sys
from aqua.logger import log_configure
from aqua.util import get_arg
from aqua.version import __version__ as aqua_version
from aqua.diagnostics.core import template_parse_arguments, open_cluster, close_cluster
from aqua.diagnostics.core import load_diagnostic_config, merge_config_args
from aqua.diagnostics import Boxplots, PlotBoxplots

def parse_arguments(args):
    """Parse command-line arguments for GlobalBiases diagnostic.

    Args:
        args (list): list of command-line arguments to parse.
    """
    parser = argparse.ArgumentParser(description='Boxplots CLI')
    parser = template_parse_arguments(parser)
    return parser.parse_args(args)

if __name__ == '__main__':

    args = parse_arguments(sys.argv[1:])

    loglevel = get_arg(args, 'loglevel', 'WARNING')
    logger = log_configure(log_level=loglevel, log_name='Boxplots CLI')
    logger.info(f"Running Boxplots diagnostic with AQUA version {aqua_version}")

    cluster = get_arg(args, 'cluster', None)
    nworkers = get_arg(args, 'nworkers', None)

    client, cluster, private_cluster, = open_cluster(nworkers=nworkers, cluster=cluster, loglevel=loglevel)

    # Load the configuration file and then merge it with the command-line arguments
    config_dict = load_diagnostic_config(diagnostic='boxplots', config=args.config,
                                         default_config='config_radiation-boxplots.yaml',
                                         loglevel=loglevel)
    config_dict = merge_config_args(config=config_dict, args=args, loglevel=loglevel)

    regrid = get_arg(args, 'regrid', None) 

    if regrid:
        logger.info(f"Regrid option is set to {regrid}")
    realization = get_arg(args, 'realization', None)
    if realization:
        logger.info(f"Realization option is set to {realization}")
        reader_kwargs = {'realization': realization}
    else:
        reader_kwargs = {}

    # Output options
    outputdir = config_dict['output'].get('outputdir', './')
    rebuild = config_dict['output'].get('rebuild', True)
    save_netcdf = config_dict['output'].get('save_netcdf', True)
    save_pdf = config_dict['output'].get('save_pdf', True)
    save_png = config_dict['output'].get('save_png', True)
    dpi = config_dict['output'].get('dpi', 300) 

    # Boxplots diagnostic
    if 'boxplots' in config_dict['diagnostics']:
        if config_dict['diagnostics']['boxplots']['run']:
            logger.info("Boxplots diagnostic is enabled.")

            diagnostic_name = config_dict['diagnostics']['boxplots'].get('diagnostic_name', 'boxplots')
            datasets = config_dict['datasets']
            references = config_dict['references']
            variable_groups = config_dict['diagnostics']['boxplots'].get('variables', [])

            for group in variable_groups:
                variables = group.get('vars', [])
                plot_kwargs = {k: v for k, v in group.items() if k != 'vars'}

                logger.info(f"Running boxplots for {variables} with options {plot_kwargs}")

                fldmeans = []
                for dataset in datasets:
                    dataset_args = {'catalog': dataset['catalog'], 'model': dataset['model'],
                                    'exp': dataset['exp'], 'source': dataset['source'],
                                    'regrid': dataset.get('regrid', regrid),
                                    'startdate': dataset.get('startdate'),
                                    'enddate': dataset.get('enddate')}

                    boxplots = Boxplots(**dataset_args, diagnostic=diagnostic_name, save_netcdf=save_netcdf, outputdir=outputdir, loglevel=loglevel)
                    boxplots.run(var=variables, reader_kwargs=reader_kwargs)
                    fldmeans.append(boxplots.fldmeans)
                
                fldmeans_ref = []
                for reference in references:
                    reference_args = {'catalog': reference['catalog'], 'model': reference['model'],
                                    'exp': reference['exp'], 'source': reference['source'],
                                    'regrid': reference.get('regrid', regrid),
                                    'startdate': reference.get('startdate'),
                                    'enddate': reference.get('enddate')}

                    boxplots_ref = Boxplots(**reference_args, save_netcdf=save_netcdf, outputdir=outputdir, loglevel=loglevel)
                    boxplots_ref.run(var=variables, reader_kwargs=reader_kwargs)

                    if getattr(boxplots_ref, "fldmeans", None) is None:
                        logger.warning(
                            f"No data retrieved for reference {reference['model']} ({reference['exp']}, {reference['source']}). Skipping."
                        )
                        continue 

                    fldmeans_ref.append(boxplots_ref.fldmeans)


                plot = PlotBoxplots(diagnostic=diagnostic_name, save_pdf=save_pdf, save_png=save_png, dpi=dpi, outputdir=outputdir, loglevel=loglevel)
                plot.plot_boxplots(data=fldmeans, data_ref=fldmeans_ref, var=variables, **plot_kwargs)

    close_cluster(client=client, cluster=cluster, private_cluster=private_cluster, loglevel=loglevel)

    logger.info("Boxplots diagnostic completed.")
