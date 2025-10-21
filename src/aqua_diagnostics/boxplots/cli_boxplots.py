"""Command-line interface for Boxplots diagnostic."""

import argparse
import sys
from aqua.diagnostics.core import template_parse_arguments
from aqua.diagnostics import Boxplots, PlotBoxplots
from aqua_diagnostics.core import DiagnosticCLI

DIAGNAME='boxplots'

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
    
    # Initialize CLI handler
    cli = DiagnosticCLI(
        args,
        diagnostic_name=DIAGNAME,
        config='config_radiation-boxplots.yaml',
        log_name=f'{DIAGNAME} CLI'
    )
    
    # Prepare CLI (load config, setup logging, etc.)
    cli.prepare()

    # Open Dask cluster if needed
    cli.open_dask_cluster()


    # Boxplots diagnostic
    if DIAGNAME in cli.config_dict['diagnostics']:
        if cli.config_dict['diagnostics'][DIAGNAME]['run']:
            cli.logger.info("Boxplots diagnostic is enabled.")

            diagnostic_name = cli.config_dict['diagnostics'][DIAGNAME].get('diagnostic_name', DIAGNAME)
            datasets = cli.config_dict['datasets']
            references = cli.config_dict['references']
            variable_groups = cli.config_dict['diagnostics'][DIAGNAME].get('variables', [])

            for group in variable_groups:
                variables = group.get('vars', [])
                plot_kwargs = {k: v for k, v in group.items() if k != 'vars'}

                cli.logger.info("Running %s for %s with options %s", DIAGNAME, variables, plot_kwargs)

                fldmeans = []
                for dataset in datasets:
                    dataset_args = cli.dataset_args(dataset)

                    boxplots = Boxplots(**dataset_args, save_netcdf=cli.save_netcdf, outputdir=cli.outputdir, loglevel=cli.loglevel)
                    boxplots.run(var=variables, reader_kwargs=cli.reader_kwargs)
                    fldmeans.append(boxplots.fldmeans)
                
                fldmeans_ref = []
                for reference in references:
                    reference_args = cli.dataset_args(reference)

                    boxplots_ref = Boxplots(**reference_args, save_netcdf=cli.save_netcdf, outputdir=cli.outputdir, loglevel=cli.loglevel)
                    boxplots_ref.run(var=variables, reader_kwargs=cli.reader_kwargs)

                    if getattr(boxplots_ref, "fldmeans", None) is None:
                        cli.logger.warning(
                            f"No data retrieved for reference {reference['model']} ({reference['exp']}, {reference['source']}). Skipping."
                     )
                        continue

                    fldmeans_ref.append(boxplots_ref.fldmeans)

                plot = PlotBoxplots(diagnostic=diagnostic_name, save_pdf=cli.save_pdf, save_png=cli.save_png, dpi=cli.dpi, outputdir=cli.outputdir, loglevel=cli.loglevel)
                plot.plot_boxplots(data=fldmeans, data_ref=fldmeans_ref, var=variables, **plot_kwargs)

    cli.close_dask_cluster()

    cli.logger.info("Boxplots diagnostic completed.")
