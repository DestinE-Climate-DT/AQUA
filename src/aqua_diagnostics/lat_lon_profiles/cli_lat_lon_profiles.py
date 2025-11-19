"""
Command-line interface for LatLonProfiles diagnostic.

This CLI allows to run the LatLonProfiles diagnostic for zonal or meridional profiles.
Details of the run are defined in a yaml configuration file for a
single or multiple experiments.
"""

import sys
import argparse
from aqua.diagnostics.core import template_parse_arguments, DiagnosticCLI
from aqua.diagnostics.lat_lon_profiles import LatLonProfiles, PlotLatLonProfiles
from aqua.diagnostics.lat_lon_profiles.util_cli import load_var_config

def parse_arguments(args):
    """Parse command-line arguments for LatLonProfiles diagnostic.

    Args:
        args (list): list of command-line arguments to parse.

    Returns:
        argparse.Namespace: Parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(description='LatLonProfiles CLI')
    parser = template_parse_arguments(parser)
    return parser.parse_args(args)

def _create_plot(cli, profiles, profile_ref, freq_type, diagnostic_name):
    """
    Helper function to create and save plots for both longterm and seasonal frequencies.
    
    Args:
        cli: DiagnosticCLI instance with configuration
        profiles (list): List of LatLonProfiles instances for datasets
        profile_ref: LatLonProfiles instance for reference dataset (or None)
        freq_type (str): Frequency type ('longterm' or 'seasonal')
        diagnostic_name (str): Name of the diagnostic
    """
    cli.logger.info(f"Creating {freq_type} plot")
    
    if freq_type == 'longterm':
        # For longterm: single profile per dataset
        data_list = [p.longterm for p in profiles]
        ref_data = profile_ref.longterm if profile_ref else None
        ref_std_data = profile_ref.std_annual if profile_ref else None
        
    else:  # seasonal
        # For seasonal: transform from [[DJF, MAM, JJA, SON], ...] to [DJF_list, MAM_list, JJA_list, SON_list]
        num_seasons = 4
        data_list = []
        for season_idx in range(num_seasons):
            season_data = [p.seasonal[season_idx] for p in profiles]
            data_list.append(season_data)
        
        ref_data = profile_ref.seasonal if profile_ref else None
        ref_std_data = profile_ref.std_seasonal if profile_ref and profile_ref.std_seasonal else None
    
    # Create and run plot
    plot = PlotLatLonProfiles(
        data=data_list,
        ref_data=ref_data,
        ref_std_data=ref_std_data,
        data_type=freq_type,
        diagnostic_name=diagnostic_name,
        loglevel=cli.loglevel
    )
    
    plot.run(
        outputdir=cli.outputdir,
        rebuild=cli.rebuild,
        dpi=cli.dpi,
        format='png' if cli.save_png else 'pdf' if cli.save_pdf else 'png'
    )

def process_variable(cli, var_config, regions, datasets, references,
                     mean_type, diagnostic_name, freq, compute_std,
                     exclude_incomplete, center_time, box_brd,
                     compute_longterm, compute_seasonal, 
                     regions_file_path=None, formula=False):
    """
    Process a single variable or formula across all datasets and regions.
    
    Args:
        cli: DiagnosticCLI instance with prepared configuration
        var_config (dict): Variable configuration
        regions (list): List of regions to process
        datasets (list): List of dataset configurations
        references (list): List of reference dataset configurations
        mean_type (str): Type of mean ('zonal' or 'meridional')
        diagnostic_name (str): Name of the diagnostic
        freq (list): List of frequencies to compute
        compute_std (bool): Whether to compute standard deviation
        exclude_incomplete (bool): Whether to exclude incomplete periods
        center_time (bool): Whether to center time coordinates
        box_brd (bool): Whether to apply box boundary
        compute_longterm (bool): Whether to compute longterm statistics
        compute_seasonal (bool): Whether to compute seasonal statistics
        regions_file_path (str, optional): Path to regions file
        formula (bool): Whether processing a formula (True) or variable (False)
    """
    var_name = var_config.get('name')
    var_units = var_config.get('units')
    var_long_name = var_config.get('long_name')
    var_standard_name = var_config.get('standard_name')
    
    cli.logger.info(f"Processing {'formula' if formula else 'variable'}: {var_name}")
    
    # Loop over regions
    for region in regions:
        cli.logger.info(f"Region: {region}")
        
        # Process datasets
        profiles = []
        for dataset in datasets:
            cli.logger.info(f"Processing dataset: {dataset['model']}/{dataset['exp']}")
            
            dataset_args = cli.dataset_args(dataset)
            
            profile = LatLonProfiles(
                **dataset_args,
                region=region,
                regions_file_path=regions_file_path,
                mean_type=mean_type,
                diagnostic_name=diagnostic_name,
                loglevel=cli.loglevel
            )
            
            profile.run(
                var=var_name,
                formula=formula,
                long_name=var_long_name,
                units=var_units,
                standard_name=var_standard_name,
                std=compute_std,
                freq=freq,
                exclude_incomplete=exclude_incomplete,
                center_time=center_time,
                box_brd=box_brd,
                outputdir=cli.outputdir,
                rebuild=cli.rebuild,
                reader_kwargs=cli.reader_kwargs
            )
            
            profiles.append(profile)
        
        # Process reference dataset (if any)
        profile_ref = None
        if references:
            ref = references[0]  # Take first reference
            cli.logger.info(f"Processing reference: {ref['model']}/{ref['exp']}")
            
            # Get base reference args
            ref_args = cli.dataset_args(ref)
            
            # For reference, use std dates if specified
            if ref.get('std_startdate'):
                ref_args['startdate'] = ref['std_startdate']
            if ref.get('std_enddate'):
                ref_args['enddate'] = ref['std_enddate']
            
            profile_ref = LatLonProfiles(
                **ref_args,
                region=region,
                regions_file_path=regions_file_path,
                mean_type=mean_type,
                diagnostic_name=diagnostic_name,
                loglevel=cli.loglevel
            )
            
            profile_ref.run(
                var=var_name,
                formula=formula,
                long_name=var_long_name,
                units=var_units,
                standard_name=var_standard_name,
                std=True,  # Always compute std for reference
                freq=freq,
                exclude_incomplete=exclude_incomplete,
                center_time=center_time,
                box_brd=box_brd,
                outputdir=cli.outputdir,
                rebuild=cli.rebuild,
                reader_kwargs={}  # No custom reader_kwargs for reference
            )
        
        # Create plots using helper function
        if compute_longterm and 'longterm' in freq:
            _create_plot(cli, profiles, profile_ref, 'longterm', diagnostic_name)
        
        if compute_seasonal and 'seasonal' in freq:
            _create_plot(cli, profiles, profile_ref, 'seasonal', diagnostic_name)


if __name__ == '__main__':
    args = parse_arguments(sys.argv[1:])
    
    # Initialize and prepare CLI
    cli = DiagnosticCLI(
        args,
        diagnostic_name='lat_lon_profiles',
        default_config='config_lat_lon_profiles.yaml',
        log_name='LatLonProfiles CLI').prepare()
    
    cli.open_dask_cluster()
    
    # LatLonProfiles diagnostic
    tool_dict = cli.config_dict['diagnostics'].get('lat_lon_profiles', {})
    
    if tool_dict and tool_dict.get('run', False):
        cli.logger.info("LatLonProfiles diagnostic is enabled.")
        
        # Extract configuration
        diagnostic_name = tool_dict.get('diagnostic_name', 'lat_lon_profiles')
        mean_type = tool_dict.get('mean_type', 'zonal')
        center_time = tool_dict.get('center_time', True)
        exclude_incomplete = tool_dict.get('exclude_incomplete', True)
        box_brd = tool_dict.get('box_brd', True)
        compute_std = tool_dict.get('compute_std', False)
        compute_seasonal = tool_dict.get('seasonal', True)
        compute_longterm = tool_dict.get('longterm', True)
        regions_file_path = tool_dict.get('regions_file_path', None)
        
        # Build frequency list
        freq = []
        if compute_seasonal:
            freq.append('seasonal')
        if compute_longterm:
            freq.append('longterm')
        
        # Get datasets and references
        datasets = cli.config_dict.get('datasets', [])
        references = cli.config_dict.get('references', [])
        
        variables = tool_dict.get('variables', [])
        formulae = tool_dict.get('formulae', [])
        all_vars = [(v, False) for v in variables] + [(f, True) for f in formulae]
        
        # Process all variables and formulae
        for var, is_formula in all_vars:
            cli.logger.info(
                "Running LatLonProfiles diagnostic for %s: %s",
                "formula" if is_formula else "variable", var)
            
            var_config, regions = load_var_config(
                cli.config_dict, 
                var, 
                diagnostic='lat_lon_profiles'
            )
            
            process_variable(
                cli=cli,
                var_config=var_config,
                regions=regions,
                datasets=datasets,
                references=references,
                mean_type=mean_type,
                diagnostic_name=diagnostic_name,
                freq=freq,
                compute_std=compute_std,
                exclude_incomplete=exclude_incomplete,
                center_time=center_time,
                box_brd=box_brd,
                compute_longterm=compute_longterm,
                compute_seasonal=compute_seasonal,
                regions_file_path=regions_file_path,
                formula=is_formula
            )
    
    cli.close_dask_cluster()
    cli.logger.info("LatLonProfiles diagnostic completed.")