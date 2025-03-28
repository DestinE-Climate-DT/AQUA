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

from aqua.logger import log_configure
from aqua.util import get_arg, ConfigPath
from aqua.version import __version__ as aqua_version
from aqua.diagnostics.core import template_parse_arguments, open_cluster, close_cluster
from aqua.diagnostics.core import load_diagnostic_config, merge_config_args
from aqua.diagnostics import SeaIce, PlotSeaIce
from aqua.diagnostics.seaice.util import filter_region_list

def parse_arguments(args):
    """Parse command-line arguments for SeaIce diagnostic.

    Args:
        args (list): list of command-line arguments to parse.
    """
    parser = argparse.ArgumentParser(description='SeaIce CLI')
    parser = template_parse_arguments(parser)
    return parser.parse_args(args)

if __name__ == '__main__':

    args = parse_arguments(sys.argv[1:])

    loglevel = get_arg(args, 'loglevel', 'WARNING')
    logger = log_configure(log_level=loglevel, log_name='SeaIce CLI')
    logger.info(f"Running Sea Ice diagnostic with AQUA version {aqua_version}")

    cluster = get_arg(args, 'cluster', None)
    nworkers = get_arg(args, 'nworkers', None)

    client, cluster, private_cluster, = open_cluster(nworkers=nworkers, cluster=cluster, loglevel=loglevel)

    # Load the configuration file and then merge it with the command-line arguments,
    # overwriting the configuration file values with the command-line arguments.
    config_dict = load_diagnostic_config(diagnostic='seaice', args=args,
                                         default_config='config/config_seaice.yaml',
                                         loglevel=loglevel)
    config_dict = merge_config_args(config=config_dict, args=args, loglevel=loglevel)
    logger.info("Loaded config_dict")
    
    # Set the loglevel to the config_file
    config_dict['setup']['loglevel'] = loglevel

    regrid = get_arg(args, 'regrid', None)

    # Output options
    outputdir = config_dict['output'].get('outputdir', './')
    rebuild   = config_dict['output'].get('rebuild',  True)
    save_pdf  = config_dict['output'].get('save_pdf', True)
    save_png  = config_dict['output'].get('save_png', True)
    dpi = config_dict['output'].get('dpi', 300)

    # ============= Sea Ice diagnostic - Timeseries diagnostic ============
    # =====================================================================
    if ('seaice_timeseries' in config_dict) and (config_dict['seaice_timeseries']['run']):
        
        # Initialise dict to store data to plot
        plot_ts_seaice = {}

        conf_dict_ts = config_dict['seaice_timeseries']
        logger.info("Executing Sea ice timeseries diagnostic for loaded config_dict.")

        # Initialize a list of len from the number of datasets
        for method in conf_dict_ts['methods']:
            logger.info(f"Method: {method}")

            # Get info
            regions   = conf_dict_ts['regions']
            startdate = conf_dict_ts['startdate']
            enddate   = conf_dict_ts['enddate']
            
            # Loop over the model datasets
            if 'datasets' in conf_dict_ts:
                datasets = conf_dict_ts['datasets']

                # Initialise monthly_models with the number of datasets
                monthly_mod = [None] * len(datasets)

                for i, dataset in enumerate(datasets):

                    # Get the pre-defined varnames from each method 
                    mod_var = (dataset['varname']).get(method)

                    # Get specific stra-end date for dataset if provided in config
                    if "monthly" in dataset['time']:
                        # Integrate by method the model data and store them in a list.
                        # Get specific start-enddate for dataset if provided in 
                        # config or (if None) get entire available time (only for models)
                        seaice = SeaIce(model=dataset['model'], 
                                        exp=dataset['exp'], 
                                        source=dataset['source'], 
                                        regions=regions,
                                        startdate=dataset.get('startdate', None), 
                                        enddate=dataset.get('enddate', None), 
                                        regrid=dataset.get('regrid', None),
                                        loglevel=config_dict['setup']['loglevel'])
                        monthly_mod[i] = seaice.compute_seaice(method=method, var=mod_var)

                    seaice.save_netcdf(monthly_mod[i], 'SeaIce', diagnostic_product=f"{dataset['source']}_{method}_timeseries")
                
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
                    
                    # Call class method
                    regions_dict = SeaIce.load_region_file()

                    domain_ref = reference.get('domain', None)

                    # Filter the region from the domain information
                    regions_indomain = filter_region_list(regions_dict, regions, domain_ref, logger)
                    
                    # Integrate by method the reference data and store them in a list
                    seaice_ref = SeaIce(model=reference['model'], 
                                        exp=reference['exp'], 
                                        source=reference['source'],
                                        catalog=reference['catalog'],
                                        regions=regions_indomain,
                                        startdate=reference.get('startdate', startdate), # Get specific start-end date for dataset if provided in config
                                        enddate=reference.get('enddate', enddate), 
                                        regrid=reference.get('regrid', None),
                                        loglevel=config_dict['setup']['loglevel'])

                    if conf_dict_ts['calc_ref_std']:
                        monthly_ref[i], monthly_std_ref[i] = seaice_ref.compute_seaice(method=method, var=reference.get('varname'), 
                                                                                       calc_std_freq=calc_std_freq)

                        seaice_ref.save_netcdf(monthly_std_ref[i], 'SeaIce', diagnostic_product=f"{reference['source']}_{method}_timeseries_std")
                    else:
                        monthly_ref[i] = seaice_ref.compute_seaice(method=method, var=reference.get('varname'))
                    
                    seaice_ref.save_netcdf(monthly_ref[i], 'SeaIce', diagnostic_product=f"{reference['source']}_{method}_timeseries")
                
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
                             **plot_ts_seaice)

            psi.plot_seaice(plot_type='timeseries', save_pdf=False, save_png=True)

    # ================ Sea Ice diagnostic - Seasonal Cycle ================
    # =====================================================================
    if ('seaice_seasonal_cycle' in config_dict) and (config_dict['seaice_seasonal_cycle']['run']):

        # Initialise dict to store data to plot
        plot_ts_seaice = {}

        conf_dict_ts = config_dict['seaice_seasonal_cycle']
        logger.info("Executing Sea ice seasonal cycle diagnostic for loaded config_dict.")

        # Initialize a list of len from the number of datasets
        for method in conf_dict_ts['methods']:
            logger.info(f"Method: {method}")

            # Get info
            regions   = conf_dict_ts['regions']
            startdate = conf_dict_ts['startdate']
            enddate   = conf_dict_ts['enddate']
            
            # Loop over the model datasets
            if 'datasets' in conf_dict_ts:
                datasets = conf_dict_ts['datasets']

                # Initialise monthly_models with the number of datasets
                monthly_mod = [None] * len(datasets)

                for i, dataset in enumerate(datasets):

                    # Get the pre-defined varnames from each method 
                    mod_var = (dataset['varname']).get(method)

                    # Get specific stra-end date for dataset if provided in config
                    if "monthly" in dataset['time']:
                        # Integrate by method the model data and store them in a list.
                        # Get specific start-enddate for dataset if provided in 
                        # config or (if None) get entire available time (only for models)
                        seaice = SeaIce(model=dataset['model'], 
                                        exp=dataset['exp'], 
                                        source=dataset['source'], 
                                        regions=regions,
                                        startdate=dataset.get('startdate', None), 
                                        enddate=dataset.get('enddate', None), 
                                        regrid=dataset.get('regrid', None),
                                        loglevel=config_dict['setup']['loglevel'])
                        monthly_mod[i] = seaice.compute_seaice(method=method, var=mod_var, 
                                                               get_seasonal_cycle=True)

                    seaice.save_netcdf(monthly_mod[i], 'SeaIce', diagnostic_product=f"{dataset['source']}_{method}_SeasCycle")
                
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
                    
                    # Call class method
                    regions_dict = SeaIce.load_region_file()

                    domain_ref = reference.get('domain', None)

                    # Filter the region from the domain information
                    regions_indomain = filter_region_list(regions_dict, regions, domain_ref, logger)
                    
                    # Integrate by method the reference data and store them in a list.
                    seaice_ref = SeaIce(model=reference['model'], 
                                        exp=reference['exp'], 
                                        source=reference['source'],
                                        catalog=reference['catalog'],
                                        regions=regions_indomain,
                                        startdate=reference.get('startdate', startdate), # Get specific start-end date for reference if provided in config
                                        enddate=reference.get('enddate', enddate), 
                                        regrid=reference.get('regrid', None),
                                        loglevel=config_dict['setup']['loglevel'])

                    if conf_dict_ts['calc_ref_std']:
                        monthly_ref[i], monthly_std_ref[i] = seaice_ref.compute_seaice(method=method, var=reference.get('varname'), 
                                                                                       calc_std_freq=calc_std_freq, 
                                                                                       get_seasonal_cycle=True)
                        seaice_ref.save_netcdf(monthly_std_ref[i], 'SeaIce', diagnostic_product=f"{reference['source']}_{method}_SeasCycle_std")
                    else:
                        monthly_ref[i] = seaice_ref.compute_seaice(method=method, var=reference.get('varname'), 
                                                                   get_seasonal_cycle=True)
                    
                    seaice_ref.save_netcdf(monthly_ref[i], 'SeaIce', diagnostic_product=f"{reference['source']}_{method}_SeasCycle")

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
                             **plot_ts_seaice)

            psi.plot_seaice(plot_type='seasonal_cycle', save_pdf=False, save_png=True)

    close_cluster(client=client, cluster=cluster, private_cluster=private_cluster, loglevel=loglevel)

    logger.info("Sea Ice diagnostic completed.")