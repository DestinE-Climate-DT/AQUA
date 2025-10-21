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
    
    # Extract prepared attributes
    logger = cli.logger
    loglevel = cli.loglevel
    config_dict = cli.config_dict
    regrid = cli.regrid
    reader_kwargs = cli.reader_kwargs
    outputdir = cli.outputdir
    rebuild = cli.rebuild
    save_pdf = cli.save_pdf
    save_png = cli.save_png
    dpi = cli.dpi
    
    # Diagnostic-specific options
    projection = get_arg(args, 'proj', 'orthographic')
    config_dict['setup']['loglevel'] = loglevel
    
    # Load region dict through dummy method access
    regions_dict = SeaIce(model='', exp='', source='')._load_regions_from_file(diagnostic='seaice')    

    # Use the top-level datasets
    datasets = config_dict['datasets']

    # ============= Sea Ice diagnostic - Timeseries diagnostic ============
    # =====================================================================
    if ('seaice_timeseries' in config_dict['diagnostics'] and config_dict['diagnostics']['seaice_timeseries']['run']):
        
        # Initialise dict to store data to plot
        plot_ts_seaice = {}

        conf_dict_ts = config_dict['diagnostics']['seaice_timeseries']
        logger.info("Executing Sea ice timeseries diagnostic for loaded config_dict.")

        # Initialize a list of len from the number of datasets
        for method in conf_dict_ts['methods']:
            logger.info(f"Method: {method}")

            # Get info
            regions   = conf_dict_ts['regions']
            startdate = conf_dict_ts['startdate']
            enddate   = conf_dict_ts['enddate']
            
            # Initialise monthly_models with the number of datasets
            monthly_mod = [None] * len(datasets)

            for i, dataset in enumerate(datasets):
                # Get the variable name for this method from the diagnostic configuration
                mod_var = conf_dict_ts['varname'][method]
                
                # Integrate by method the model data and store them in a list.
                seaice = SeaIce(model=dataset['model'], 
                                exp=dataset['exp'], 
                                source=dataset['source'], 
                                regions=regions,
                                startdate=dataset.get('startdate', None), 
                                enddate=dataset.get('enddate', None), 
                                regrid=regrid or dataset.get('regrid', None),
                                outputdir=outputdir,
                                loglevel=config_dict['setup']['loglevel'])

                monthly_mod[i] = seaice.compute_seaice(method=method, var=mod_var, reader_kwargs=reader_kwargs)

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
                        logger.info(f"Skipping ref data {reference['model']}, {reference['exp']}, "
                                    f"{reference['source']} as it is not meant to operate for method: '{method}'")
                        continue

                    domain_ref = reference.get('domain', None)

                    # Filter the region from the domain information
                    regs_indomain = filter_region_list(regions_dict, regions, domain_ref, logger)
                    
                    # Integrate by method the reference data and store them in a list
                    seaice_ref = SeaIce(model=reference['model'], 
                                        exp=reference['exp'], 
                                        source=reference['source'],
                                        catalog=reference['catalog'],
                                        regions=regs_indomain,
                                        startdate=reference.get('startdate', startdate), # Get specific start-end date for dataset if provided in config
                                        enddate=reference.get('enddate', enddate), 
                                        regrid=regrid or reference.get('regrid', None),
                                        outputdir=outputdir,
                                        loglevel=config_dict['setup']['loglevel'])

                    if conf_dict_ts['calc_ref_std']:
                        monthly_ref[i], monthly_std_ref[i] = seaice_ref.compute_seaice(method=method, var=reference.get('varname'), 
                                                                                       calc_std_freq=calc_std_freq, reader_kwargs=reader_kwargs)

                        seaice_ref.save_netcdf(monthly_std_ref[i], 'seaice', diagnostic_product='timeseries_std',
                                               extra_keys={'method': method, 'source': reference['source'], 'regions_domain': "_".join(regs_indomain)})
                    else:
                        monthly_ref[i] = seaice_ref.compute_seaice(method=method, var=reference.get('varname'), reader_kwargs=reader_kwargs)
                    
                    seaice_ref.save_netcdf(monthly_ref[i], 'seaice', diagnostic_product='timeseries',
                                           extra_keys={'method': method, 'source': reference['source'], 'regions_domain': "_".join(regs_indomain)})
                
                # Update the dict
                plot_ts_seaice['monthly_ref'] = monthly_ref
                plot_ts_seaice['monthly_std_ref'] = monthly_std_ref if monthly_std_ref else None

            logger.info(f"Plotting Timeseries")

            # Start plotting
            psi = PlotSeaIce(catalog=datasets[0]['model'],
                             model=datasets[0]['model'], 
                             exp=datasets[0]['exp'], 
                             source=datasets[0]['source'],
                             loglevel=config_dict['setup']['loglevel'],
                             outputdir=outputdir,
                             rebuild=rebuild,
                             **plot_ts_seaice)

            psi.plot_seaice(plot_type='timeseries', save_pdf=save_pdf, save_png=save_png)

    # ================ Sea Ice diagnostic - Seasonal Cycle ================
    # =====================================================================
    if ('seaice_seasonal_cycle' in config_dict['diagnostics'] and config_dict['diagnostics']['seaice_seasonal_cycle']['run']):

        # Initialise dict to store data to plot
        plot_ts_seaice = {}

        conf_dict_ts = config_dict['diagnostics']['seaice_seasonal_cycle']
        logger.info("Executing Sea ice seasonal cycle diagnostic for loaded config_dict.")

        # Initialize a list of len from the number of datasets
        for method in conf_dict_ts['methods']:
            logger.info(f"Method: {method}")

            # Get info
            regions   = conf_dict_ts['regions']
            startdate = conf_dict_ts['startdate']
            enddate   = conf_dict_ts['enddate']

            # Initialise monthly_models with the number of datasets
            monthly_mod = [None] * len(datasets)

            for i, dataset in enumerate(datasets):
                # Get the variable name for this method from the diagnostic configuration
                mod_var = conf_dict_ts['varname'][method]

                # Integrate by method the model data and store them in a list.
                seaice = SeaIce(model=dataset['model'], 
                                exp=dataset['exp'], 
                                source=dataset['source'], 
                                regions=regions,
                                startdate=dataset.get('startdate', None), 
                                enddate=dataset.get('enddate', None), 
                                regrid=regrid or dataset.get('regrid', None),
                                outputdir=outputdir,
                                loglevel=config_dict['setup']['loglevel'])

                monthly_mod[i] = seaice.compute_seaice(method=method, var=mod_var, 
                                                       get_seasonal_cycle=True, reader_kwargs=reader_kwargs)

                seaice.save_netcdf(monthly_mod[i], 'seaice', diagnostic_product='seasonal_cycle', 
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
                        logger.info(f"Skipping ref data {reference['model']}, {reference['exp']}, "
                                    f"{reference['source']} as it is not meant to operate for method: '{method}'")
                        continue

                    domain_ref = reference.get('domain', None)

                    # Filter the region from the domain information
                    regs_indomain = filter_region_list(regions_dict, regions, domain_ref, logger)
                    
                    # Integrate by method the reference data and store them in a list.
                    seaice_ref = SeaIce(model=reference['model'], 
                                        exp=reference['exp'], 
                                        source=reference['source'],
                                        catalog=reference['catalog'],
                                        regions=regs_indomain,
                                        startdate=reference.get('startdate', startdate), # Get specific start-end date for reference if provided in config
                                        enddate=reference.get('enddate', enddate), 
                                        regrid=regrid or reference.get('regrid', None),
                                        outputdir=outputdir,
                                        loglevel=config_dict['setup']['loglevel'])

                    if conf_dict_ts['calc_ref_std']:
                        monthly_ref[i], monthly_std_ref[i] = seaice_ref.compute_seaice(method=method, var=reference.get('varname'), 
                                                                                       calc_std_freq=calc_std_freq, 
                                                                                       get_seasonal_cycle=True, reader_kwargs=reader_kwargs)
                        seaice_ref.save_netcdf(monthly_std_ref[i], 'seaice', diagnostic_product='seasonal_cycle_std',
                                               extra_keys={'method': method, 'source': reference['source'], 'regions_domain': "_".join(regs_indomain)})
                    else:
                        monthly_ref[i] = seaice_ref.compute_seaice(method=method, var=reference.get('varname'), 
                                                                   get_seasonal_cycle=True, 
                                                                   reader_kwargs=reader_kwargs)
                                                                   
                    seaice_ref.save_netcdf(monthly_ref[i], 'seaice', diagnostic_product='seasonal_cycle',
                                           extra_keys={'method': method, 'source': reference['source'], 'regions_domain': "_".join(regs_indomain)})

                # Update the dict
                plot_ts_seaice['monthly_ref'] = monthly_ref
                plot_ts_seaice['monthly_std_ref'] = monthly_std_ref if monthly_std_ref else None

            logger.info(f"Plotting Seasonal Cycle")

            # Start plotting
            psi = PlotSeaIce(catalog=datasets[0]['model'],
                             model=datasets[0]['model'], 
                             exp=datasets[0]['exp'], 
                             source=datasets[0]['source'],
                             loglevel=config_dict['setup']['loglevel'],
                             outputdir=outputdir,
                             rebuild=rebuild,
                             **plot_ts_seaice)

            psi.plot_seaice(plot_type='seasonal_cycle', save_pdf=save_pdf, save_png=save_png)

    # ================ Sea Ice diagnostic - 2D Bias Maps ================
    # ===================================================================
    if ('seaice_2d_bias' in config_dict['diagnostics'] and config_dict['diagnostics']['seaice_2d_bias']['run']):

        conf_dict_2d = config_dict['diagnostics']['seaice_2d_bias']
        logger.info("Executing Sea ice 2D bias diagnostic for loaded config_dict.")

        # Get info
        regions = conf_dict_2d['regions']
        startdate = conf_dict_2d['startdate']
        enddate = conf_dict_2d['enddate']
        months = conf_dict_2d.get('months', [3, 9])

        # Loop over the methods (fraction and thickness)
        for method in conf_dict_2d['methods']:
            logger.info(f"Method: {method}")

            # Initialise dict to store data to plot
            plot_bias_seaice = {}

            # Initialise monthly_models with the number of datasets
            clims_mod = [None] * len(datasets)

            for i, dataset in enumerate(datasets):
                # Get the variable name for this method from the diagnostic configuration
                mod_var = conf_dict_2d['varname'][method]

                # Compute 2D sea ice data for the model
                seaice = SeaIce(model=dataset['model'], exp=dataset['exp'], 
                                source=dataset['source'], 
                                regions=regions,
                                startdate=dataset.get('startdate', None), 
                                enddate=dataset.get('enddate', None), 
                                regrid=regrid or dataset.get('regrid', None),
                                outputdir=outputdir,
                                loglevel=config_dict['setup']['loglevel'])
                
                # Compute 2D data for each region
                clims_mod[i] = seaice.compute_seaice(method=method, var=mod_var, stat='mean', freq='monthly', reader_kwargs=reader_kwargs)
                
                seaice.save_netcdf(clims_mod[i], 'seaice', diagnostic_product='bias_2d',
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
                        logger.info(f"Skipping ref data {reference['model']}, {reference['exp']}, "
                                    f"{reference['source']} as it is not meant to operate for method: '{method}'")
                        continue

                    domain_ref = reference.get('domain', None)

                    # Filter the regions from the domain information
                    regs_indomain = filter_region_list(regions_dict, regions, domain_ref, logger)
                    
                    # Get by method the reference data and store them in a list.
                    seaice_ref = SeaIce(model=reference['model'],
                                        exp=reference['exp'],
                                        source=reference['source'],
                                        catalog=reference['catalog'],
                                        regions=regs_indomain,
                                        startdate=reference.get('startdate', startdate),
                                        enddate=reference.get('enddate', enddate),
                                        regrid=regrid or reference.get('regrid', None),
                                        outputdir=outputdir,
                                        loglevel=config_dict['setup']['loglevel'])

                    clims_ref[i] = seaice_ref.compute_seaice(method=method, var=reference.get('varname'), 
                                                             stat='mean', freq='monthly', reader_kwargs=reader_kwargs)
                    
                    seaice_ref.save_netcdf(clims_ref[i], 'seaice', diagnostic_product='bias_2d',
                                           extra_keys={'method': method, 'source':reference['source'], 
                                           'exp':reference['exp'], 'regions_domain': "_".join(regs_indomain)})

                plot_bias_seaice['ref'] = clims_ref

            logger.info(f"Plotting 2D Bias Maps for method: {method}")
            
            projkw = conf_dict_2d['projections'][projection]

            longregs_indomain = [regions_dict['regions'][reg]['longname'] for reg in regions]

            # Start plotting                                   
            psi = Plot2DSeaIce(ref=plot_bias_seaice.get('ref'),
                               models=plot_bias_seaice.get('models'),
                               regions_to_plot=longregs_indomain,
                               outputdir=outputdir,
                               rebuild=rebuild,
                               loglevel=config_dict['setup']['loglevel'])

            psi.plot_2d_seaice(plot_type='bias', 
                               months=months,
                               method=method,
                               projkw=projkw,
                               plot_ref_contour= True if method == 'fraction' else False,
                               save_pdf=save_pdf, 
                               save_png=save_png)

    cli.close_dask_cluster()
