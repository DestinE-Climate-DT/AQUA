#!/usr/bin/env python3
"""Command-line interface for SeaIce diagnostic."""

import argparse
import sys

from aqua.util import get_arg
from aqua.diagnostics.core import template_parse_arguments, DiagnosticCLI
from aqua.diagnostics import SeaIce, PlotSeaIce, Plot2DSeaIce
from aqua.diagnostics.seaice.util import filter_region_list

TOOLNAME = 'SeaIce'
TOOLNAME_KEY = 'seaice'

def parse_arguments(args):
    """Parse command-line arguments for SeaIce diagnostic.

    Args:
        args (list): list of command-line arguments to parse.
    """
    parser = argparse.ArgumentParser(description=f'{TOOLNAME} CLI')
    parser = template_parse_arguments(parser)

    # Add extra arguments
    parser.add_argument("--proj", type=str, choices=['orthographic', 'azimuthal_equidistant'],
                        default='orthographic', help="Projection type for 2D plots (default: orthographic)")
    return parser.parse_args(args)

def process_timeseries_or_seasonal_cycle(cli, diagnostic_type, datasets, regions_dict):
    """
    Process timeseries or seasonal cycle diagnostic.
    
    Args:
        cli: DiagnosticCLI instance
        diagnostic_type: 'seaice_timeseries' or 'seaice_seasonal_cycle'
        datasets: List of dataset configurations
        regions_dict: Dictionary of region definitions
    """
    conf_dict = cli.config_dict['diagnostics'][diagnostic_type]
    is_seasonal = (diagnostic_type == 'seaice_seasonal_cycle')
    
    cli.logger.info(f"Executing Sea ice {'seasonal cycle' if is_seasonal else 'timeseries'} diagnostic.")

    for method in conf_dict['methods']:
        cli.logger.info(f"Method: {method}")

        plot_ts_seaice = {}
        regions = conf_dict['regions']
        startdate = conf_dict['startdate']
        enddate = conf_dict['enddate']
        
        # Process model datasets
        monthly_mod = [None] * len(datasets)
        for i, dataset in enumerate(datasets):
            mod_var = conf_dict['varname'][method]
            dataset_args = cli.dataset_args(dataset)
            dataset_args['regions'] = regions

            seaice = SeaIce(**dataset_args,
                            outputdir=cli.outputdir,
                            loglevel=cli.loglevel)

            monthly_mod[i] = seaice.compute_seaice(
                method=method, 
                var=mod_var, 
                get_seasonal_cycle=is_seasonal,
                reader_kwargs=cli.reader_kwargs
            )

            product = 'seasonalcycle' if is_seasonal else 'timeseries'
            seaice.save_netcdf(monthly_mod[i], 'seaice', diagnostic_product=product, 
                               extra_keys={'method': method, 'source': dataset['source'], 
                                           'regions_domain': "_".join(regions)})
        
        plot_ts_seaice['monthly_models'] = monthly_mod
        
        # Process reference datasets
        method_to_ref_key = {
            'extent': 'references_extent',
            'volume': 'references_volume'
        }
        ref_key = method_to_ref_key.get(method)
        
        if ref_key and ref_key in conf_dict:
            references = conf_dict[ref_key]
            calc_ref_std = conf_dict.get('calc_ref_std', False)
            calc_std_freq = conf_dict.get('ref_std_freq', None) if calc_ref_std else None
            
            monthly_ref = [None] * len(references)
            monthly_std_ref = [None] * len(references) if calc_ref_std else None

            for i, reference in enumerate(references):
                domain_ref = reference.get('domain', None)
                regs_indomain = filter_region_list(regions_dict, regions, domain_ref, cli.logger)
                
                reference_args = cli.dataset_args(reference, default_startdate=startdate, default_enddate=enddate)
                reference_args['regions'] = regs_indomain
                reference_args['regrid'] = cli.regrid or reference.get('regrid', None)
                
                seaice_ref = SeaIce(**reference_args,
                                    outputdir=cli.outputdir,
                                    loglevel=cli.loglevel)

                var_name = reference.get('varname', conf_dict['varname'][method])

                if calc_ref_std:
                    monthly_ref[i], monthly_std_ref[i] = seaice_ref.compute_seaice(
                        method=method, 
                        var=var_name, 
                        calc_std_freq=calc_std_freq,
                        get_seasonal_cycle=is_seasonal
                    )
                    product = 'seasonalcycle_std' if is_seasonal else 'timeseries_std'
                    seaice_ref.save_netcdf(monthly_std_ref[i], 'seaice', diagnostic_product=product,
                                           extra_keys={'method': method, 'source': reference['source'], 
                                                       'regions_domain': "_".join(regs_indomain)})
                else:
                    monthly_ref[i] = seaice_ref.compute_seaice(
                        method=method, 
                        var=var_name,
                        get_seasonal_cycle=is_seasonal
                    )
                
                product = 'seasonalcycle' if is_seasonal else 'timeseries'
                seaice_ref.save_netcdf(monthly_ref[i], 'seaice', diagnostic_product=product,
                                       extra_keys={'method': method, 'source': reference['source'], 
                                                   'regions_domain': "_".join(regs_indomain)})
            
            plot_ts_seaice['monthly_ref'] = monthly_ref
            plot_ts_seaice['monthly_std_ref'] = monthly_std_ref

        cli.logger.info(f"Plotting {'Seasonal Cycle' if is_seasonal else 'Timeseries'}")

        # Plot
        psi = PlotSeaIce(catalog=datasets[0]['model'],
                         model=datasets[0]['model'], 
                         exp=datasets[0]['exp'], 
                         source=datasets[0]['source'],
                         loglevel=cli.loglevel,
                         outputdir=cli.outputdir,
                         rebuild=cli.rebuild,
                         **plot_ts_seaice)

        plot_type = 'seasonalcycle' if is_seasonal else 'timeseries'
        psi.plot_seaice(plot_type=plot_type, save_pdf=cli.save_pdf, save_png=cli.save_png)


def process_2d_bias(cli, datasets, regions_dict, projection):
    """
    Process 2D bias maps diagnostic.
    
    Args:
        cli: DiagnosticCLI instance
        datasets: List of dataset configurations
        regions_dict: Dictionary of region definitions
        projection: Projection type for plotting
    """
    conf_dict_2d = cli.config_dict['diagnostics']['seaice_2d_bias']
    cli.logger.info("Executing Sea ice 2D bias diagnostic.")

    regions = conf_dict_2d['regions']
    startdate = conf_dict_2d['startdate']
    enddate = conf_dict_2d['enddate']
    months = conf_dict_2d.get('months', [3, 9])

    for method in conf_dict_2d['methods']:
        cli.logger.info(f"Method: {method}")

        plot_bias_seaice = {}
        clims_mod = [None] * len(datasets)

        for i, dataset in enumerate(datasets):
            mod_var = conf_dict_2d['varname'][method]
            dataset_args = cli.dataset_args(dataset)
            dataset_args['regions'] = regions

            seaice = SeaIce(**dataset_args,
                            outputdir=cli.outputdir,
                            loglevel=cli.loglevel)

            clims_mod[i] = seaice.compute_seaice(
                method=method, 
                var=mod_var, 
                stat='mean', 
                freq='monthly', 
                reader_kwargs=cli.reader_kwargs
            )
            
            seaice.save_netcdf(clims_mod[i], 'seaice', diagnostic_product='bias',
                               extra_keys={'method': method, 'source': dataset['source'], 
                                           'exp': dataset['exp'], 'regions_domain': "_".join(regions)})

        plot_bias_seaice['models'] = clims_mod
        
        # Process reference datasets
        # Map method to the appropriate references key
        method_to_ref_key = {
            'fraction': 'references_fraction',
            'thickness': 'references_thickness'
        }
        ref_key = method_to_ref_key.get(method)
        
        if ref_key and ref_key in conf_dict_2d:
            references = conf_dict_2d[ref_key]
            clims_ref = [None] * len(references)

            for i, reference in enumerate(references):
                domain_ref = reference.get('domain', None)
                regs_indomain = filter_region_list(regions_dict, regions, domain_ref, cli.logger)
                
                reference_args = cli.dataset_args(reference, default_startdate=startdate, default_enddate=enddate)
                reference_args['regions'] = regs_indomain
                reference_args['regrid'] = cli.regrid or reference.get('regrid', None)
                
                seaice_ref = SeaIce(**reference_args,
                                    outputdir=cli.outputdir,
                                    loglevel=cli.loglevel)

                var_name = reference.get('varname', conf_dict_2d['varname'][method])

                clims_ref[i] = seaice_ref.compute_seaice(
                    method=method, 
                    var=var_name, 
                    stat='mean', 
                    freq='monthly'
                )
                
                seaice_ref.save_netcdf(clims_ref[i], 'seaice', diagnostic_product='bias',
                                       extra_keys={'method': method, 'source': reference['source'], 
                                                   'exp': reference['exp'], 'regions_domain': "_".join(regs_indomain)})

            plot_bias_seaice['ref'] = clims_ref

        cli.logger.info(f"Plotting 2D Bias Maps for method: {method}")
        
        projkw = conf_dict_2d['projections'][projection]
        longregs_indomain = [regions_dict['regions'][reg]['longname'] for reg in regions]

        psi = Plot2DSeaIce(ref=plot_bias_seaice.get('ref'),
                           models=plot_bias_seaice.get('models'),
                           regions_to_plot=longregs_indomain,
                           outputdir=cli.outputdir,
                           rebuild=cli.rebuild,
                           loglevel=cli.loglevel)

        psi.plot_2d_seaice(plot_type='bias', 
                           months=months,
                           method=method,
                           projkw=projkw,
                           plot_ref_contour=True if method == 'fraction' else False,
                           save_pdf=cli.save_pdf, 
                           save_png=cli.save_png)


if __name__ == '__main__':

    args = parse_arguments(sys.argv[1:])

    # Initialize and prepare CLI
    cli = DiagnosticCLI(
        args=args,
        diagnostic_name=TOOLNAME_KEY,
        default_config='config_seaice.yaml',
        log_name=f'{TOOLNAME} CLI'
    )
    cli.prepare()
    cli.open_dask_cluster()
    
    # Diagnostic-specific options
    projection = get_arg(args, 'proj', 'orthographic')
    
    # Load region dict
    regions_dict = SeaIce(model='', exp='', source='')._load_regions_from_file(diagnostic=TOOLNAME_KEY)

    # Use the top-level datasets
    datasets = cli.config_dict['datasets']

    # Process diagnostics based on what's enabled in config
    diagnostics = cli.config_dict.get('diagnostics', {})
    
    # Timeseries diagnostic
    if diagnostics.get('seaice_timeseries', {}).get('run', False):
        process_timeseries_or_seasonal_cycle(cli, 'seaice_timeseries', datasets, regions_dict)
    
    # Seasonal cycle diagnostic
    if diagnostics.get('seaice_seasonal_cycle', {}).get('run', False):
        process_timeseries_or_seasonal_cycle(cli, 'seaice_seasonal_cycle', datasets, regions_dict)
    
    # 2D bias diagnostic
    if diagnostics.get('seaice_2d_bias', {}).get('run', False):
        process_2d_bias(cli, datasets, regions_dict, projection)

    cli.close_dask_cluster()

    cli.logger.info(f"{TOOLNAME} diagnostic completed.")