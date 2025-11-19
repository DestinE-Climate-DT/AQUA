#!/usr/bin/env python3
"""
Command-line interface for seaice diagnostic.

This CLI allows to perform multiple plots such as the timeseries 
of integrated sea ice volume and extent from a yaml configuration 
file for a single or multiple experiments with the possibility to 
add reference data.
"""
import argparse
import sys

from aqua.util import get_arg
from aqua.diagnostics.core import template_parse_arguments, DiagnosticCLI
from aqua.diagnostics import SeaIce, PlotSeaIce, Plot2DSeaIce
from aqua.diagnostics.seaice.util import filter_region_list

def parse_arguments(args):
    """Parse command-line arguments for SeaIce diagnostic.

    Args:
        args (list): list of command-line arguments to parse.
    """
    parser = argparse.ArgumentParser(description='SeaIce CLI')
    parser = template_parse_arguments(parser)

    # Add extra arguments
    parser.add_argument("--proj", type=str, choices=['orthographic', 'azimuthal_equidistant'],
                        default='orthographic', help="Projection type for 2D plots (default: orthographic)")
    return parser.parse_args(args)

if __name__ == '__main__':

    args = parse_arguments(sys.argv[1:])

    # Initialize and prepare CLI
    cli = DiagnosticCLI(
        args=args,
        diagnostic_name='seaice',
        default_config='config_seaice.yaml',
        log_name='SeaIce CLI'
    ).prepare()
    cli.open_dask_cluster()
    
    # Diagnostic-specific options
    projection = get_arg(args, 'proj', 'orthographic')
    
    # Load region dict through dummy method access
    regions_dict = SeaIce(model='', exp='', source='')._load_regions_from_file(diagnostic='seaice')

    regrid = get_arg(args, 'regrid', None)

    realization = get_arg(args, 'realization', None)
    if realization:
        cli.logger.info(f"Realization option is set to: {realization}")
        reader_kwargs = {'realization': realization}
    else:
        reader_kwargs = {}

    # Use the top-level datasets
    datasets = cli.config_dict['datasets']

    # ============= Sea Ice diagnostic - Timeseries diagnostic ============
    # =====================================================================
    if ('seaice_timeseries' in cli.config_dict['diagnostics'] and cli.config_dict['diagnostics']['seaice_timeseries']['run']):
        
        # Initialise dict to store data to plot
        plot_ts_seaice = {}

        conf_dict_ts = cli.config_dict['diagnostics']['seaice_timeseries']
        cli.logger.info("Executing Sea ice timeseries diagnostic for loaded config_dict.")

        # Initialize a list of len from the number of datasets
        for method in conf_dict_ts['methods']:
            cli.logger.info(f"Method: {method}")

            # Get info
            regions   = conf_dict_ts['regions']
            
            # Initialise monthly_models with the number of datasets
            monthly_mod = [None] * len(datasets)

            for i, dataset in enumerate(datasets):
                # Get the variable name for this method from the diagnostic configuration
                varname = conf_dict_ts['varname'][method]
                dataset_args = cli.dataset_args(dataset)

                # Integrate by method the model data and store them in a list.
                seaice = SeaIce(**dataset_args,
                                regions=regions,
                                outputdir=cli.outputdir,
                                loglevel=cli.loglevel)

                monthly_mod[i] = seaice.compute_seaice(method=method, var=varname, reader_kwargs=reader_kwargs)

                seaice.save_netcdf(monthly_mod[i], 'seaice', diagnostic_product='timeseries', 
                                   extra_keys={'method': method, 'source': dataset['source'], 'regions_domain': "_".join(regions)})
            
                # Update the dict
                plot_ts_seaice['monthly_models'] = monthly_mod
            
            # Initialize a list of len from the number of references
            if 'references' in conf_dict_ts:
                references = conf_dict_ts['references']

                # Initialise monthly_refs with the number of refs (also for std)
                if conf_dict_ts['calc_ref_std']:
                    calc_std_freq = conf_dict_ts.get('ref_std_freq',None)
                    monthly_std_ref = [None] * len(references)

                monthly_ref = [None] * len(references)

                for i, reference in enumerate(references):

                    use_for_method = reference.get("use_for_method", None)
                    if use_for_method is not None and use_for_method != method:
                        cli.logger.info(f"Skipping ref data {reference['model']}, {reference['exp']}, "
                                    f"{reference['source']} as it is not meant to operate for method: '{method}'")
                        continue

                    domain_ref = reference.get('domain', None)

                    # Filter the region from the domain information
                    regs_indomain = filter_region_list(regions_dict, regions, domain_ref, cli.logger)
                    
                    # Get reference args and override specific fields
                    reference_args = cli.dataset_args(reference)
                    reference_args['regions'] = regs_indomain
                    reference_args['regrid'] = regrid or reference.get('regrid', None)
                    
                    # Integrate by method the reference data and store them in a list
                    seaice_ref = SeaIce(**reference_args,
                                        outputdir=cli.outputdir,
                                        loglevel=cli.loglevel)

                    if conf_dict_ts['calc_ref_std']:
                        monthly_ref[i], monthly_std_ref[i] = seaice_ref.compute_seaice(method=method, var=varname, 
                                                                                       calc_std_freq=calc_std_freq) #, reader_kwargs=reader_kwargs)

                        seaice_ref.save_netcdf(monthly_std_ref[i], 'seaice', diagnostic_product='timeseries_std',
                                               extra_keys={'method': method, 'source': reference['source'], 'regions_domain': "_".join(regs_indomain)})
                    else:
                        monthly_ref[i] = seaice_ref.compute_seaice(method=method, var=varname) #, reader_kwargs=reader_kwargs)
                    
                    seaice_ref.save_netcdf(monthly_ref[i], 'seaice', diagnostic_product='timeseries',
                                           extra_keys={'method': method, 'source': reference['source'], 'regions_domain': "_".join(regs_indomain)})
                
                # Update the dict
                plot_ts_seaice['monthly_ref'] = monthly_ref
                plot_ts_seaice['monthly_std_ref'] = monthly_std_ref if monthly_std_ref else None

            cli.logger.info("Plotting Timeseries")

            # Start plotting
            psi = PlotSeaIce(catalog=datasets[0]['model'],
                             model=datasets[0]['model'], 
                             exp=datasets[0]['exp'], 
                             source=datasets[0]['source'],
                             loglevel=cli.loglevel,
                             outputdir=cli.outputdir,
                             rebuild=cli.rebuild,
                             **plot_ts_seaice)

            psi.plot_seaice(plot_type='timeseries', save_pdf=cli.save_pdf, save_png=cli.save_png)

    # ================ Sea Ice diagnostic - Seasonal Cycle ================
    # =====================================================================
    if ('seaice_seasonal_cycle' in cli.config_dict['diagnostics'] and cli.config_dict['diagnostics']['seaice_seasonal_cycle']['run']):

        # Initialise dict to store data to plot
        plot_ts_seaice = {}

        conf_dict_ts = cli.config_dict['diagnostics']['seaice_seasonal_cycle']
        cli.logger.info("Executing Sea ice seasonal cycle diagnostic for loaded config_dict.")

        # Initialize a list of len from the number of datasets
        for method in conf_dict_ts['methods']:
            cli.logger.info(f"Method: {method}")

            # Get info
            regions   = conf_dict_ts['regions']

            # Initialise monthly_models with the number of datasets
            monthly_mod = [None] * len(datasets)

            for i, dataset in enumerate(datasets):
                # Get the variable name for this method from the diagnostic configuration
                varname = conf_dict_ts['varname'][method]
                dataset_args = cli.dataset_args(dataset)
                dataset_args['regions'] = regions

                # Integrate by method the model data and store them in a list.
                seaice = SeaIce(**dataset_args,
                                outputdir=cli.outputdir,
                                loglevel=cli.loglevel)

                monthly_mod[i] = seaice.compute_seaice(method=method, var=varname, 
                                                       get_seasonal_cycle=True, reader_kwargs=reader_kwargs)

                seaice.save_netcdf(monthly_mod[i], 'seaice', diagnostic_product='seasonalcycle', 
                                   extra_keys={'method': method, 'source': dataset['source'], 'regions_domain': "_".join(regions)})
            
            # Update the dict
            plot_ts_seaice['monthly_models'] = monthly_mod
            
            # Initialize a list of len from the number of references
            if 'references' in conf_dict_ts:
                references = conf_dict_ts['references']

                # Initialise monthly_refs with the number of refs (also for std)
                if conf_dict_ts['calc_ref_std']:
                    calc_std_freq = conf_dict_ts.get('ref_std_freq',None)
                    monthly_std_ref = [None] * len(references)

                monthly_ref = [None] * len(references)

                for i, reference in enumerate(references):

                    use_for_method = reference.get("use_for_method", None)
                    if use_for_method is not None and use_for_method != method:
                        cli.logger.info(f"Skipping ref data {reference['model']}, {reference['exp']}, "
                                    f"{reference['source']} as it is not meant to operate for method: '{method}'")
                        continue

                    domain_ref = reference.get('domain', None)

                    # Filter the region from the domain information
                    regs_indomain = filter_region_list(regions_dict, regions, domain_ref, cli.logger)
                    
                    # Get reference args and override specific fields
                    reference_args = cli.dataset_args(reference)
                    reference_args['regions'] = regs_indomain
                    reference_args['regrid'] = regrid or reference.get('regrid', None)
                    
                    # Integrate by method the reference data and store them in a list.
                    seaice_ref = SeaIce(**reference_args,
                                        outputdir=cli.outputdir,
                                        loglevel=cli.loglevel)

                    if conf_dict_ts['calc_ref_std']:
                        monthly_ref[i], monthly_std_ref[i] = seaice_ref.compute_seaice(method=method, var=varname, 
                                                                                       calc_std_freq=calc_std_freq, 
                                                                                       get_seasonal_cycle=True) #, reader_kwargs=reader_kwargs)
                        seaice_ref.save_netcdf(monthly_std_ref[i], 'seaice', diagnostic_product='seasonalcycle_std',
                                               extra_keys={'method': method, 'source': reference['source'], 'regions_domain': "_".join(regs_indomain)})
                    else:
                        monthly_ref[i] = seaice_ref.compute_seaice(method=method, var=varname, 
                                                                   get_seasonal_cycle=True) # ,
                                                                   # reader_kwargs=reader_kwargs)
                                                                   
                    seaice_ref.save_netcdf(monthly_ref[i], 'seaice', diagnostic_product='seasonalcycle',
                                           extra_keys={'method': method, 'source': reference['source'], 'regions_domain': "_".join(regs_indomain)})

                # Update the dict
                plot_ts_seaice['monthly_ref'] = monthly_ref
                plot_ts_seaice['monthly_std_ref'] = monthly_std_ref if monthly_std_ref else None

            cli.logger.info("Plotting Seasonal Cycle")

            # Start plotting
            psi = PlotSeaIce(catalog=datasets[0]['model'],
                             model=datasets[0]['model'], 
                             exp=datasets[0]['exp'], 
                             source=datasets[0]['source'],
                             loglevel=cli.loglevel,
                             outputdir=cli.outputdir,
                             rebuild=cli.rebuild,
                             **plot_ts_seaice)

            psi.plot_seaice(plot_type='seasonalcycle', save_pdf=cli.save_pdf, save_png=cli.save_png)

    # ================ Sea Ice diagnostic - 2D Bias Maps ================
    # ===================================================================
    if ('seaice_2d_bias' in cli.config_dict['diagnostics'] and cli.config_dict['diagnostics']['seaice_2d_bias']['run']):

        conf_dict_2d = cli.config_dict['diagnostics']['seaice_2d_bias']
        cli.logger.info("Executing Sea ice 2D bias diagnostic for loaded config_dict.")

        # Get info
        regions = conf_dict_2d['regions']
        months = conf_dict_2d.get('months', [3, 9])

        # Loop over the methods (fraction and thickness)
        for method in conf_dict_2d['methods']:
            cli.logger.info(f"Method: {method}")

            # Initialise dict to store data to plot
            plot_bias_seaice = {}

            # Initialise monthly_models with the number of datasets
            clims_mod = [None] * len(datasets)

            for i, dataset in enumerate(datasets):
                # Get the variable name for this method from the diagnostic configuration
                varname = conf_dict_2d['varname'][method]
                dataset_args = cli.dataset_args(dataset)
                dataset_args['regions'] = regions

                # Compute 2D sea ice data for the model
                seaice = SeaIce(**dataset_args,
                                outputdir=cli.outputdir,
                                loglevel=cli.loglevel)

                # Compute 2D data for each region
                clims_mod[i] = seaice.compute_seaice(method=method, var=varname, stat='mean', freq='monthly', reader_kwargs=reader_kwargs)
                
                seaice.save_netcdf(clims_mod[i], 'seaice', diagnostic_product='bias',
                                   extra_keys={'method': method, 'source':dataset['source'], 
                                   'exp':dataset['exp'], 'regions_domain': "_".join(regions)})

            plot_bias_seaice['models'] = clims_mod
            
            # Initialize a list of len from the number of references
            if 'references' in conf_dict_2d:
                references = conf_dict_2d['references']

                clims_ref = [None] * len(references)

                for i, reference in enumerate(references):

                    use_for_method = reference.get("use_for_method", None)
                    
                    if use_for_method is not None and use_for_method != method:
                        cli.logger.info(f"Skipping ref data {reference['model']}, {reference['exp']}, "
                                    f"{reference['source']} as it is not meant to operate for method: '{method}'")
                        continue

                    domain_ref = reference.get('domain', None)

                    # Filter the regions from the domain information
                    regs_indomain = filter_region_list(regions_dict, regions, domain_ref, cli.logger)
                    
                    # Get reference args and override specific fields
                    reference_args = cli.dataset_args(reference)
                    reference_args['regions'] = regs_indomain
                    reference_args['regrid'] = regrid or reference.get('regrid', None)
                    
                    # Get by method the reference data and store them in a list.
                    seaice_ref = SeaIce(**reference_args,
                                        outputdir=cli.outputdir,
                                        loglevel=cli.loglevel)

                    clims_ref[i] = seaice_ref.compute_seaice(method=method, var=varname, 
                                                             stat='mean', freq='monthly') # , reader_kwargs=reader_kwargs)
                    
                    seaice_ref.save_netcdf(clims_ref[i], 'seaice', diagnostic_product='bias',
                                           extra_keys={'method': method, 'source':reference['source'], 
                                           'exp':reference['exp'], 'regions_domain': "_".join(regs_indomain)})

                plot_bias_seaice['ref'] = clims_ref

            cli.logger.info(f"Plotting 2D Bias Maps for method: {method}")
            
            projkw = conf_dict_2d['projections'][projection]

            longregs_indomain = [regions_dict['regions'][reg]['longname'] for reg in regions]

            # Start plotting                                   
            psi = Plot2DSeaIce(ref=plot_bias_seaice.get('ref'),
                               models=plot_bias_seaice.get('models'),
                               regions_to_plot=longregs_indomain,
                               outputdir=cli.outputdir,
                               rebuild=cli.rebuild,
                               loglevel=cli.config_dict['setup']['loglevel'])

            psi.plot_2d_seaice(plot_type='bias', 
                               months=months,
                               method=method,
                               projkw=projkw,
                               plot_ref_contour= True if method == 'fraction' else False,
                               save_pdf=cli.save_pdf, 
                               save_png=cli.save_png)

    cli.close_dask_cluster()

    cli.logger.info("Sea Ice diagnostic completed.")