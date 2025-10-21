import argparse
import sys

from aqua.util import get_arg, to_list
from aqua.exceptions import NotEnoughDataError, NoDataError, NoObservationError
from aqua.diagnostics import GlobalBiases, PlotGlobalBiases
from aqua.diagnostics.core import template_parse_arguments
from aqua_diagnostics.core import DiagnosticCLI


def parse_arguments(args):
    """Parse command-line arguments for GlobalBiases diagnostic.

    Args:
        args (list): list of command-line arguments to parse.
    """
    parser = argparse.ArgumentParser(description='GlobalBiases CLI')
    parser = template_parse_arguments(parser)
    return parser.parse_args(args)

if __name__ == '__main__':

    args = parse_arguments(sys.argv[1:])
    
    cli = DiagnosticCLI(args, 'globalbiases', 'config_global_biases.yaml', log_name='GlobalBiases CLI').prepare()
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
    dpi = cli.dpi
    save_netcdf = cli.save_netcdf

    # Global Biases diagnostic
    if 'globalbiases' in config_dict['diagnostics']:
        if config_dict['diagnostics']['globalbiases']['run']:
            logger.info("GlobalBiases diagnostic is enabled.")

            if len(config_dict['datasets']) > 1:
                logger.warning(
                    "Only the first entry in 'datasets' will be used.\n"
                    "Multiple datasets are not supported by this diagnostic."
                )
            if len(config_dict['references']) > 1:
                logger.warning(
                    "Only the first entry in 'references' will be used.\n"
                    "Multiple references are not supported by this diagnostic."
                )
            diagnostic_name = config_dict['diagnostics']['globalbiases'].get('diagnostic_name', 'globalbiases')
            dataset = config_dict['datasets'][0]
            reference = config_dict['references'][0]
            dataset_args = {'catalog': dataset['catalog'], 'model': dataset['model'],
                            'exp': dataset['exp'], 'source': dataset['source'],
                            'regrid': regrid if regrid is not None else dataset.get('regrid', None)}
            reference_args = {'catalog': reference['catalog'], 'model': reference['model'],
                            'exp': reference['exp'], 'source': reference['source'],
                            'regrid': regrid if regrid is not None else reference.get('regrid', None)}
            
            variables = config_dict['diagnostics']['globalbiases'].get('variables', [])
            formulae = config_dict['diagnostics']['globalbiases'].get('formulae', [])
            plev = config_dict['diagnostics']['globalbiases']['params']['default'].get('plev')
            seasons = config_dict['diagnostics']['globalbiases']['params']['default'].get('seasons', False)
            seasons_stat = config_dict['diagnostics']['globalbiases']['params']['default'].get('seasons_stat', 'mean')
            vertical = config_dict['diagnostics']['globalbiases']['params']['default'].get('vertical', False)

            startdate_data = config_dict['diagnostics']['globalbiases']['params']['default'].get('startdate_data', None)
            enddate_data = config_dict['diagnostics']['globalbiases']['params']['default'].get('enddate_data', None)
            startdate_ref = config_dict['diagnostics']['globalbiases']['params']['default'].get('startdate_ref', None)
            enddate_ref = config_dict['diagnostics']['globalbiases']['params']['default'].get('enddate_ref', None)

            logger.debug("Selected levels for vertical plots: %s", plev)

            biases_dataset = GlobalBiases(**dataset_args, startdate=startdate_data, enddate=enddate_data,
                                          outputdir=outputdir, loglevel=cli.loglevel)
            biases_reference = GlobalBiases(**reference_args, startdate=startdate_ref, enddate=enddate_ref,
                                            outputdir=outputdir, loglevel=cli.loglevel)

            all_vars = [(v, False) for v in variables] + [(f, True) for f in formulae]

            for var, is_formula in all_vars:
                logger.info(f"Running Global Biases diagnostic for {'formula' if is_formula else 'variable'}: {var}")

                all_plot_params = config_dict['diagnostics']['globalbiases'].get('plot_params', {})
                default_params = all_plot_params.get('default', {})
                var_params = all_plot_params.get(var, {})
                plot_params = {**default_params, **var_params}

                vmin, vmax = plot_params.get('vmin'), plot_params.get('vmax')
                param_dict = config_dict['diagnostics']['globalbiases'].get('params', {}).get(var, {})
                units = param_dict.get('units', None)
                long_name = param_dict.get('long_name', None)
                short_name = param_dict.get('short_name', None)

                try:
                    biases_dataset.retrieve(var=var, units=units, formula=is_formula,
                                            long_name=long_name, short_name=short_name,
                                            reader_kwargs=reader_kwargs)
                    biases_reference.retrieve(var=var, units=units, formula=is_formula,
                                            long_name=long_name, short_name=short_name)
                except (NoDataError, KeyError, ValueError) as e:
                    logger.warning(f"Variable '{var}' not found in dataset. Skipping. ({e})")
                    continue  

                biases_dataset.compute_climatology(seasonal=seasons, seasons_stat=seasons_stat)
                biases_reference.compute_climatology(seasonal=seasons, seasons_stat=seasons_stat)

                if short_name is not None: 
                    var = short_name

                if 'plev' in biases_dataset.data.get(var, {}).dims and plev:
                    plev_list = to_list(plev)
                else: 
                    plev_list = [None] 

                for p in plev_list:
                    logger.info(f"Processing variable: {var} at pressure level: {p}" if p else f"Processing variable: {var} at surface level")

                    proj = plot_params.get('projection', 'robinson')
                    proj_params = plot_params.get('projection_params', {})
                    cmap= plot_params.get('cmap', 'RdBu_r')

                    logger.debug(f"Using projection: {proj} for variable: {var}")
                    plot_biases = PlotGlobalBiases(diagnostic=diagnostic_name, save_pdf=save_pdf, save_png=save_png,
                                                dpi=dpi, outputdir=outputdir, cmap=cmap, loglevel=cli.loglevel)
                    plot_biases.plot_bias(data=biases_dataset.climatology, data_ref=biases_reference.climatology,
                                          var=var, plev=p,
                                          proj=proj, proj_params=proj_params,
                                          vmin=vmin, vmax=vmax) 
                    if seasons:
                        plot_biases.plot_seasonal_bias(data=biases_dataset.seasonal_climatology, 
                                                       data_ref=biases_reference.seasonal_climatology,
                                                       var=var, plev=p, 
                                                       proj=proj, proj_params=proj_params,
                                                       vmin=vmin, vmax=vmax)

                if vertical and 'plev' in biases_dataset.data.get(var, {}).dims:
                    logger.debug(f"Plotting vertical bias for variable: {var}")
                    vmin_v , vmax_v = plot_params.get('vmin_v'), plot_params.get('vmax_v')
                    plot_biases.plot_vertical_bias(data=biases_dataset.climatology, data_ref=biases_reference.climatology, 
                                                   var=var, vmin=vmin_v, vmax=vmax_v)

    cli.close_dask_cluster()

    logger.info("Global Biases diagnostic completed.")
