import argparse
import sys
from aqua.diagnostics.core import template_parse_arguments
from aqua.diagnostics import Boxplots, PlotBoxplots
from aqua_diagnostics.core import DiagnosticCLI

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
    
    cli = DiagnosticCLI(args, 'boxplots', 'config_radiation-boxplots.yaml', log_name='Boxplots CLI').prepare()
    cli.open_dask_cluster()
    
    logger = cli.logger
    config_dict = cli.config_dict
    regrid = cli.regrid
    reader_kwargs = cli.reader_kwargs

    # Output options (from cli_base)
    outputdir = cli.outputdir
    rebuild = cli.rebuild
    save_pdf = cli.save_pdf
    save_png = cli.save_png
    save_netcdf = cli.save_netcdf
    dpi = cli.dpi

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

                    boxplots = Boxplots(**dataset_args, save_netcdf=save_netcdf, outputdir=outputdir, loglevel=cli.loglevel)
                    boxplots.run(var=variables, reader_kwargs=reader_kwargs)
                    fldmeans.append(boxplots.fldmeans)
                
                fldmeans_ref = []
                for reference in references:
                    reference_args = {'catalog': reference['catalog'], 'model': reference['model'],
                                    'exp': reference['exp'], 'source': reference['source'],
                                    'regrid': reference.get('regrid', regrid),
                                    'startdate': reference.get('startdate'),
                                    'enddate': reference.get('enddate')}

                    boxplots_ref = Boxplots(**reference_args, save_netcdf=save_netcdf, outputdir=outputdir, loglevel=cli.loglevel)
                    boxplots_ref.run(var=variables, reader_kwargs=reader_kwargs)

                    if getattr(boxplots_ref, "fldmeans", None) is None:
                        logger.warning(
                            f"No data retrieved for reference {reference['model']} ({reference['exp']}, {reference['source']}). Skipping."
                        )
                        continue 

                    fldmeans_ref.append(boxplots_ref.fldmeans)


                plot = PlotBoxplots(diagnostic=diagnostic_name, save_pdf=save_pdf, save_png=save_png, dpi=dpi, outputdir=outputdir, loglevel=cli.loglevel)
                plot.plot_boxplots(data=fldmeans, data_ref=fldmeans_ref, var=variables, **plot_kwargs)

    cli.close_dask_cluster()

    logger.info("Boxplots diagnostic completed.")
