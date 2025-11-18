"""Command-line interface for Boxplots diagnostic."""

import argparse
import sys

from aqua.diagnostics.core import template_parse_arguments
from aqua.diagnostics import Boxplots, PlotBoxplots
from aqua.diagnostics.core import DiagnosticCLI

# default tool name
TOOLNAME='Boxplots'
TOOLNAME_KEY = TOOLNAME.lower()

def parse_arguments(arguments):
    """Parse command-line arguments for Boxplots diagnostic.

    Args:
        args (list): list of command-line arguments to parse.
    """
    parser = argparse.ArgumentParser(description=f'{TOOLNAME} CLI')
    parser = template_parse_arguments(parser)
    return parser.parse_args(arguments)

if __name__ == '__main__':

    args = parse_arguments(sys.argv[1:])

    # set tool name for config lookup
    

    # Initialize CLI handler
    cli = DiagnosticCLI(
        args,
        diagnostic_name=TOOLNAME_KEY,
        default_config='config_radiation-boxplots.yaml',
    )

    # Prepare CLI (load config, setup logging, etc.)
    cli.prepare()

    # Open Dask cluster if needed
    cli.open_dask_cluster()

    # Retrieve tool-specific configuration
    tool_dict = cli.config_dict['diagnostics'].get(TOOLNAME_KEY, {})

    # Boxplots diagnostic
    if tool_dict and tool_dict.get('run', False):
        cli.logger.info(f"{TOOLNAME_KEY} diagnostic is enabled.")

        diagnostic_name = tool_dict.get('diagnostic_name', TOOLNAME_KEY)
        datasets = cli.config_dict['datasets']
        references = cli.config_dict['references']
        variable_groups = tool_dict.get('variables', [])

        for group in variable_groups:
            variables = group.get('vars', [])
            plot_kwargs = {k: v for k, v in group.items() if k != 'vars'}

            cli.logger.info("Running %s for %s with options %s", TOOLNAME_KEY, variables, plot_kwargs)

            fldmeans = []
            for dataset in datasets:
                dataset_args = cli.dataset_args(dataset)

                boxplots = Boxplots(**dataset_args, diagnostic=diagnostic_name,
                                    save_netcdf=cli.save_netcdf, outputdir=cli.outputdir,
                                    loglevel=cli.loglevel)
                boxplots.run(var=variables, reader_kwargs=cli.reader_kwargs)
                fldmeans.append(boxplots.fldmeans)

            fldmeans_ref = []
            for reference in references:
                reference_args = cli.dataset_args(reference)

                boxplots_ref = Boxplots(**reference_args, diagnostic=cli.diagnostic_name,
                                        save_netcdf=cli.save_netcdf,
                                        outputdir=cli.outputdir, loglevel=cli.loglevel)
                boxplots_ref.run(var=variables, reader_kwargs=cli.reader_kwargs)

                if getattr(boxplots_ref, "fldmeans", None) is None:
                    cli.logger.warning(
                        "No data retrieved for reference %s (%s, %s). Skipping.",
                        reference['model'],
                        reference['exp'],
                        reference['source']
                    )
                    continue

                fldmeans_ref.append(boxplots_ref.fldmeans)

            all_entries = datasets + references
            model_exp_list = [f"{entry['model']} ({entry['exp']})" for entry in all_entries]
            model_exp_list_unique = list(dict.fromkeys(model_exp_list))

            if variables == ['-snlwrf', 'snswrf', 'slhtf', 'ishf']:
                TITLE = "Boxplot of Surface Radiation Fluxes for: " + ", ".join(model_exp_list_unique)
            elif variables == ['-tnlwrf', 'tnswrf']:
                TITLE = "Boxplot of TOA Radiation Fluxes for: " + ", ".join(model_exp_list_unique)
            else:
                TITLE = None
            plot = PlotBoxplots(diagnostic=cli.diagnostic_name, save_pdf=cli.save_pdf,
                                save_png=cli.save_png, dpi=cli.dpi, outputdir=cli.outputdir, loglevel=cli.loglevel)
            plot.plot_boxplots(data=fldmeans, data_ref=fldmeans_ref, var=variables, title=TITLE, **plot_kwargs)

    cli.close_dask_cluster()

    cli.logger.info("Boxplots diagnostic completed.")
