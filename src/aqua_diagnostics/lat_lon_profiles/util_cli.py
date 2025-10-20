"""Utility functions for the LatLonProfiles CLI."""

from aqua.diagnostics.lat_lon_profiles import LatLonProfiles, PlotLatLonProfiles
from aqua.logger import log_configure

def load_var_config(config_dict, var, diagnostic='lat_lon_profiles'):
    """Load variable configuration from config dictionary.
    
    Args:
        config_dict (dict): Configuration dictionary.
        var (str or dict): Variable name or variable configuration dictionary.
        diagnostic (str): Diagnostic name.

    Returns:
        tuple: Variable configuration dictionary and list of regions.
    """
    if isinstance(var, dict):
        var_name = var.get('name')
        var_config = var
    else:
        var_name = var
        var_config = config_dict['diagnostics'][diagnostic].get('params', {}).get(var_name, {})
    
    # Get regions
    regions = var_config.get('regions', [None])
    
    return var_config, regions

def _create_profile(catalog, model, exp, source, regrid, startdate, enddate,
                   region, mean_type, diagnostic_name, loglevel):
    """
    Internal helper to create a LatLonProfiles object.
    
    This consolidates the initialization logic used by both regular datasets
    and reference datasets.
    """
    init_args = {
        'catalog': catalog,
        'model': model,
        'exp': exp,
        'source': source,
        'regrid': regrid,
        'startdate': startdate,
        'enddate': enddate,
        'region': region,
        'mean_type': mean_type,
        'diagnostic_name': diagnostic_name,
        'loglevel': loglevel
    }
    return LatLonProfiles(**init_args)






def process_dataset(dataset, var_name, var_units, var_long_name, var_standard_name,
                   region, mean_type, diagnostic_name, regrid, freq, compute_std,
                   exclude_incomplete, center_time, box_brd, outputdir, rebuild,
                   reader_kwargs, loglevel, formula=False, is_reference=False):
    """
    Process a single dataset for LatLonProfiles CLI.

    Args:
        dataset (dict): Dataset configuration dictionary.
        var_name (str): Variable name.
        var_units (str): Variable units.
        var_long_name (str): Variable long name.
        var_standard_name (str): Variable standard name.
        region (str): Region name.
        mean_type (str): Mean type.
        diagnostic_name (str): Diagnostic name.
        regrid (str or None): Regridding method.
        freq (str): Frequency of data.
        compute_std (bool): Whether to compute standard deviation.
        exclude_incomplete (bool): Whether to exclude incomplete data.
        center_time (bool): Whether to center time.
        box_brd (list or None): Box boundaries.
        outputdir (str): Output directory.
        rebuild (bool): Whether to rebuild existing outputs.
        reader_kwargs (dict): Additional reader keyword arguments.
        loglevel (int): Logging level.
        formula (bool): Whether processing a formula (True) or variable (False).
        is_reference (bool): Whether this is a reference dataset.
    
    Returns:
        LatLonProfiles: The processed profile object.
    """
    logger = log_configure(log_level=loglevel, log_name='LatLonProfiles CLI')
    logger.info(f'Processing {"reference" if is_reference else "dataset"}: {dataset}')
    
    # Handle reference vs regular dataset startdate/enddate
    if is_reference:
        startdate = dataset.get('std_startdate')
        enddate = dataset.get('std_enddate')
    else:
        startdate = dataset.get('startdate')
        enddate = dataset.get('enddate')
    
    # Create profile object
    profile = _create_profile(
        catalog=dataset['catalog'],
        model=dataset['model'],
        exp=dataset['exp'],
        source=dataset['source'],
        regrid=regrid if regrid is not None else dataset.get('regrid', None),
        startdate=startdate,
        enddate=enddate,
        region=region,
        mean_type=mean_type,
        diagnostic_name=diagnostic_name,
        loglevel=loglevel
    )
    
    # Run the diagnostic
    run_args = {
        'var': var_name,
        'formula': formula,
        'units': var_units,
        'long_name': var_long_name,
        'standard_name': var_standard_name,
        'std': True if is_reference else compute_std,  # Always compute std for reference
        'freq': freq,
        'exclude_incomplete': exclude_incomplete,
        'center_time': center_time,
        'box_brd': box_brd,
        'outputdir': outputdir,
        'rebuild': rebuild,
        'reader_kwargs': dataset.get('reader_kwargs') or reader_kwargs
    }
    
    profile.run(**run_args)
    return profile
    
def _create_single_plot(plot_type, data_list, ref_data, ref_std_data, 
                       save_pdf, save_png, outputdir, rebuild, dpi, loglevel):
    """
    Internal helper to create and save a single plot.
    
    Args:
        plot_type (str): Either 'longterm' or 'seasonal'
        data_list (list or list of lists): Data to plot
        ref_data: Reference data (DataArray or list of DataArrays)
        ref_std_data: Reference std data (DataArray or list of DataArrays)
        save_pdf (bool): Whether to save PDF
        save_png (bool): Whether to save PNG
        outputdir (str): Output directory
        rebuild (bool): Whether to rebuild existing files
        dpi (int): DPI for saved images
        loglevel (str): Log level
    """
    plot_args = {
        'data': data_list,
        'ref_data': ref_data,
        'ref_std_data': ref_std_data,
        'data_type': plot_type,
        'loglevel': loglevel
    }
    
    plot_obj = PlotLatLonProfiles(**plot_args)
    
    if save_pdf:
        plot_obj.run(outputdir=outputdir, rebuild=rebuild, dpi=dpi, format='pdf', style=None)
    if save_png:
        plot_obj.run(outputdir=outputdir, rebuild=rebuild, dpi=dpi, format='png', style=None)


def create_and_save_plots(profiles, profile_ref, var_name, compute_longterm, compute_seasonal,
                         save_pdf, save_png, outputdir, rebuild, dpi, loglevel):
    """
    Create and save plots for LatLonProfiles CLI.
    
    Args:
        profiles (list): List of LatLonProfiles objects
        profile_ref (LatLonProfiles or None): Reference profile object
        var_name (str): Variable name
        compute_longterm (bool): Whether to compute longterm plots
        compute_seasonal (bool): Whether to compute seasonal plots
        save_pdf (bool): Whether to save PDF
        save_png (bool): Whether to save PNG
        outputdir (str): Output directory
        rebuild (bool): Whether to rebuild existing files
        dpi (int): DPI for saved images
        loglevel (str): Log level
    """
    logger = log_configure(log_level=loglevel, log_name='LatLonProfiles CLI')
    logger.info(f"Plotting LatLonProfiles diagnostic for {var_name}")
    
    # Plot longterm (annual mean) if enabled and computed
    if compute_longterm and hasattr(profiles[0], 'longterm'):
        logger.info("Creating longterm (annual) plot")
        
        longterm_data = [profile.longterm for profile in profiles]
        
        _create_single_plot(
            plot_type='longterm',
            data_list=longterm_data,
            ref_data=profile_ref.longterm if profile_ref else None,
            ref_std_data=profile_ref.std_annual if profile_ref else None,
            save_pdf=save_pdf,
            save_png=save_png,
            outputdir=outputdir,
            rebuild=rebuild,
            dpi=dpi,
            loglevel=loglevel
        )

    # Plot seasonal (4-panel) if enabled and computed
    if compute_seasonal and hasattr(profiles[0], 'seasonal'):
        logger.info("Creating seasonal (4-panel) plot")
        
        # Prepare seasonal data for all 4 seasons
        combined_seasonal_data = []
        combined_ref_data = []
        combined_ref_std_data = []
        
        for season_idx in range(4):  # DJF, MAM, JJA, SON
            season_data = [profile.seasonal[season_idx] for profile in profiles]
            combined_seasonal_data.append(season_data)
            
            if profile_ref:
                combined_ref_data.append(profile_ref.seasonal[season_idx])
                combined_ref_std_data.append(profile_ref.std_seasonal[season_idx])
        
        _create_single_plot(
            plot_type='seasonal',
            data_list=combined_seasonal_data,
            ref_data=combined_ref_data if profile_ref else None,
            ref_std_data=combined_ref_std_data if profile_ref else None,
            save_pdf=save_pdf,
            save_png=save_png,
            outputdir=outputdir,
            rebuild=rebuild,
            dpi=dpi,
            loglevel=loglevel
        )

def process_variable_or_formula(config_dict, var_config, regions, datasets, 
                                mean_type, diagnostic_name, regrid, freq, 
                                compute_std, exclude_incomplete, center_time, 
                                box_brd, outputdir, rebuild, reader_kwargs, 
                                save_pdf, save_png, dpi, compute_longterm, 
                                compute_seasonal, loglevel, formula=False):
    """
    Process a variable or formula for all datasets and regions.
    
    This is the main orchestrator function that:
    1. Processes all datasets
    2. Processes reference data (if available)
    3. Creates and saves plots
    """
    logger = log_configure(log_level=loglevel, log_name='LatLonProfiles CLI')
    
    var_name = var_config.get('name')
    var_units = var_config.get('units', None)
    var_long_name = var_config.get('long_name', None)
    var_standard_name = var_config.get('standard_name', None)
    var_type = 'formula' if formula else 'variable'
    
    logger.info(f"Running LatLonProfiles diagnostic for {var_type} '{var_name}' with mean_type={mean_type}")
    
    for region in regions:
        try:
            logger.info(f"Running in region {region if region else 'global'}")

            # Process all datasets
            profiles = [
                process_dataset(
                    dataset=dataset,
                    var_name=var_name,
                    var_units=var_units,
                    var_long_name=var_long_name,
                    var_standard_name=var_standard_name,
                    region=region,
                    mean_type=mean_type,
                    diagnostic_name=diagnostic_name,
                    regrid=regrid,
                    freq=freq,
                    compute_std=compute_std,
                    exclude_incomplete=exclude_incomplete,
                    center_time=center_time,
                    box_brd=box_brd,
                    outputdir=outputdir,
                    rebuild=rebuild,
                    reader_kwargs=reader_kwargs,
                    loglevel=loglevel,
                    formula=formula,
                    is_reference=False
                )
                for dataset in datasets
            ]

            # Process reference if available
            profile_ref = None
            if 'references' in config_dict and len(config_dict['references']) > 0:
                profile_ref = process_dataset(
                    dataset=config_dict['references'][0],
                    var_name=var_name,
                    var_units=var_units,
                    var_long_name=var_long_name,
                    var_standard_name=var_standard_name,
                    region=region,
                    mean_type=mean_type,
                    diagnostic_name=diagnostic_name,
                    regrid=regrid,
                    freq=freq,
                    compute_std=compute_std,  # Not used for reference
                    exclude_incomplete=exclude_incomplete,
                    center_time=center_time,
                    box_brd=box_brd,
                    outputdir=outputdir,
                    rebuild=rebuild,
                    reader_kwargs={},
                    loglevel=loglevel,
                    formula=False,  # Reference is always a variable
                    is_reference=True
                )

            # Create plots
            if save_pdf or save_png:
                create_and_save_plots(
                    profiles=profiles,
                    profile_ref=profile_ref,
                    var_name=var_name,
                    compute_longterm=compute_longterm,
                    compute_seasonal=compute_seasonal,
                    save_pdf=save_pdf,
                    save_png=save_png,
                    outputdir=outputdir,
                    rebuild=rebuild,
                    dpi=dpi,
                    loglevel=loglevel
                )

        except Exception as e:
            logger.error(f"Error running LatLonProfiles for {var_type} '{var_name}' "
                        f"in region {region if region else 'global'}: {e}")