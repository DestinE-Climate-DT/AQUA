"""Utility functions for the Histogram CLI."""

from aqua.diagnostics.histogram import Histogram, PlotHistogram
from aqua.logger import log_configure


def load_var_config(config_dict, var, diagnostic='histogram'):
    """Load variable configuration from config dictionary.
    
    Args:
        config_dict (dict): Configuration dictionary.
        var (str or dict): Variable name or variable configuration dictionary.
        diagnostic (str): Diagnostic name.

    Returns:
        tuple: Variable configuration dictionary and list of regions.
    """
    if isinstance(var, dict):
        var_config = var
    else:
        var_config = {'name': var}
    
    # Get regions
    regions = var_config.get('regions', [None])
    
    return var_config, regions


def _create_histogram(catalog, model, exp, source, regrid, startdate, enddate,
                      region, lon_limits, lat_limits, bins, range_config, 
                      weighted, diagnostic_name, loglevel):
    """
    Internal helper to create a Histogram object.
    
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
        'lon_limits': lon_limits,
        'lat_limits': lat_limits,
        'bins': bins,
        'range': range_config,
        'weighted': weighted,
        'diagnostic_name': diagnostic_name,
        'loglevel': loglevel
    }
    return Histogram(**init_args)


def process_dataset(dataset, var_name, var_units, var_long_name, var_standard_name,
                   region, lon_limits, lat_limits, diagnostic_name, regrid, 
                   bins, range_config, weighted, density, box_brd, 
                   outputdir, rebuild, reader_kwargs, loglevel, 
                   formula=False, is_reference=False):
    """
    Process a single dataset for Histogram CLI.
    """
    logger = log_configure(log_level=loglevel, log_name='Histogram CLI')
    logger.info(f'Processing {"reference" if is_reference else "dataset"}: {dataset}')
    
    # Handle startdate/enddate
    startdate = dataset.get('startdate', None)
    enddate = dataset.get('enddate', None)
    
    # Create histogram object
    histogram = _create_histogram(
        catalog=dataset['catalog'],
        model=dataset['model'],
        exp=dataset['exp'],
        source=dataset['source'],
        regrid=regrid if regrid is not None else dataset.get('regrid', None),
        startdate=startdate,
        enddate=enddate,
        region=region,
        lon_limits=lon_limits,
        lat_limits=lat_limits,
        bins=bins,
        range_config=range_config,
        weighted=weighted,
        diagnostic_name=diagnostic_name,
        loglevel=loglevel
    )
    
    # Run the diagnostic
    histogram.run(
        var=var_name,
        formula=formula,
        long_name=var_long_name,
        units=var_units,
        standard_name=var_standard_name,
        box_brd=box_brd,
        density=density,
        outputdir=outputdir,
        rebuild=rebuild,
        reader_kwargs=dataset.get('reader_kwargs') or reader_kwargs
    )
    
    return histogram


def create_and_save_plots(histograms, histogram_ref, var_name, 
                         save_pdf, save_png, outputdir, rebuild, dpi, 
                         diagnostic_name, xlogscale, ylogscale, 
                         smooth, smooth_window, xmin, xmax, ymin, ymax,
                         loglevel):
    """
    Create and save histogram plots.

    Args:
        histograms (list): List of Histogram objects.
        histogram_ref (Histogram or None): Reference histogram object.
        var_name (str): Variable name.
        save_pdf (bool): Whether to save PDF format.
        save_png (bool): Whether to save PNG format.
        outputdir (str): Output directory.
        rebuild (bool): Whether to rebuild existing outputs.
        dpi (int): DPI for output figures.
        diagnostic_name (str): Diagnostic name.
        xlogscale (bool): Use logarithmic scale for x-axis.
        ylogscale (bool): Use logarithmic scale for y-axis.
        smooth (bool): Apply smoothing to histogram.
        smooth_window (int): Window size for smoothing.
        xmin (float or None): Minimum x-axis value.
        xmax (float or None): Maximum x-axis value.
        ymin (float or None): Minimum y-axis value.
        ymax (float or None): Maximum y-axis value.
        loglevel (str): Logging level.
    """
    logger = log_configure(log_level=loglevel, log_name='Histogram CLI')
    
    # Collect histogram data from all datasets
    data_list = [hist.histogram_data for hist in histograms]
    ref_data = histogram_ref.histogram_data if histogram_ref is not None else None
    
    # Create plot object
    logger.info(f'Creating histogram plot for variable {var_name}')
    plot = PlotHistogram(
        data=data_list,
        ref_data=ref_data,
        diagnostic_name=diagnostic_name,
        loglevel=loglevel
    )
    
    # Save PNG if requested
    if save_png:
        logger.info('Saving PNG plot')
        plot.run(
            outputdir=outputdir,
            rebuild=rebuild,
            dpi=dpi,
            format='png',
            xlogscale=xlogscale,
            ylogscale=ylogscale,
            smooth=smooth,
            smooth_window=smooth_window,
            xmin=xmin,
            xmax=xmax,
            ymin=ymin,
            ymax=ymax
        )
    
    # Save PDF if requested
    if save_pdf:
        logger.info('Saving PDF plot')
        plot.run(
            outputdir=outputdir,
            rebuild=rebuild,
            dpi=dpi,
            format='pdf',
            xlogscale=xlogscale,
            ylogscale=ylogscale,
            smooth=smooth,
            smooth_window=smooth_window,
            xmin=xmin,
            xmax=xmax,
            ymin=ymin,
            ymax=ymax
        )


def process_variable_or_formula(config_dict, var_config, regions, datasets, 
                                diagnostic_name, regrid, bins, range_config, 
                                weighted, density, box_brd, outputdir, rebuild, 
                                reader_kwargs, save_pdf, save_png, dpi, 
                                xlogscale, ylogscale, smooth, smooth_window,
                                xmin, xmax, ymin, ymax, loglevel, formula=False):
    """
    Process a variable or formula for all datasets and regions.

    Args:
        config_dict (dict): Configuration dictionary.
        var_config (dict): Variable configuration dictionary.
        regions (list): List of regions to process.
        datasets (list): List of dataset configurations.
        diagnostic_name (str): Diagnostic name.
        regrid (str or None): Regridding method.
        bins (int): Number of bins for histogram.
        range_config (tuple or None): Range for histogram bins.
        weighted (bool): Use latitudinal weights.
        density (bool): Compute probability density function.
        box_brd (bool): Apply box boundaries.
        outputdir (str): Output directory.
        rebuild (bool): Whether to rebuild existing outputs.
        reader_kwargs (dict): Additional reader keyword arguments.
        save_pdf (bool): Whether to save PDF format.
        save_png (bool): Whether to save PNG format.
        dpi (int): DPI for output figures.
        xlogscale (bool): Use logarithmic scale for x-axis.
        ylogscale (bool): Use logarithmic scale for y-axis.
        smooth (bool): Apply smoothing to histogram.
        smooth_window (int): Window size for smoothing.
        xmin (float or None): Minimum x-axis value.
        xmax (float or None): Maximum x-axis value.
        ymin (float or None): Minimum y-axis value.
        ymax (float or None): Maximum y-axis value.
        loglevel (str): Logging level.
        formula (bool): Whether processing a formula (True) or variable (False).
    """
    logger = log_configure(log_level=loglevel, log_name='Histogram CLI')
    
    var_name = var_config.get('name')
    var_units = var_config.get('units', None)
    var_long_name = var_config.get('long_name', None)
    var_standard_name = var_config.get('standard_name', None)
    
    # Get lon/lat limits from variable config if specified
    var_lon_limits = var_config.get('lon_limits', None)
    var_lat_limits = var_config.get('lat_limits', None)
    
    logger.info(f'Processing {"formula" if formula else "variable"}: {var_name}')
    
    # Process each region
    for region in regions:
        logger.info(f'Processing region: {region}')
        
        # Use variable-specific limits if provided, otherwise None
        lon_limits = var_lon_limits
        lat_limits = var_lat_limits
        
        # Process all datasets
        histograms = []
        for dataset in datasets:
            hist = process_dataset(
                dataset=dataset,
                var_name=var_name,
                var_units=var_units,
                var_long_name=var_long_name,
                var_standard_name=var_standard_name,
                region=region,
                lon_limits=lon_limits,
                lat_limits=lat_limits,
                diagnostic_name=diagnostic_name,
                regrid=regrid,
                bins=bins,
                range_config=range_config,
                weighted=weighted,
                density=density,
                box_brd=box_brd,
                outputdir=outputdir,
                rebuild=rebuild,
                reader_kwargs=reader_kwargs or dataset.get('reader_kwargs', {}),
                loglevel=loglevel,
                formula=formula,
                is_reference=False
            )
            histograms.append(hist)
        
        # Process reference dataset if present
        histogram_ref = None
        if 'references' in config_dict and config_dict['references']:
            ref_dataset = config_dict['references'][0]
            histogram_ref = process_dataset(
                dataset=ref_dataset,
                var_name=var_name,
                var_units=var_units,
                var_long_name=var_long_name,
                var_standard_name=var_standard_name,
                region=region,
                lon_limits=lon_limits,
                lat_limits=lat_limits,
                diagnostic_name=diagnostic_name,
                regrid=regrid,
                bins=bins,
                range_config=range_config,
                weighted=weighted,
                density=density,
                box_brd=box_brd,
                outputdir=outputdir,
                rebuild=rebuild,
                reader_kwargs=reader_kwargs or ref_dataset.get('reader_kwargs', {}),
                loglevel=loglevel,
                formula=formula,
                is_reference=True
            )
        
        # Create and save plots
        create_and_save_plots(
            histograms=histograms,
            histogram_ref=histogram_ref,
            var_name=var_name,
            save_pdf=save_pdf,
            save_png=save_png,
            outputdir=outputdir,
            rebuild=rebuild,
            dpi=dpi,
            diagnostic_name=diagnostic_name,
            xlogscale=xlogscale,
            ylogscale=ylogscale,
            smooth=smooth,
            smooth_window=smooth_window,
            xmin=xmin,
            xmax=xmax,
            ymin=ymin,
            ymax=ymax,
            loglevel=loglevel
        )