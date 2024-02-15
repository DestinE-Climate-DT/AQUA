import sys
sys.path.append('../')
from src.tropical_rainfall_tools import ToolsClass
import os
import argparse
from aqua.util import load_yaml, get_arg
from aqua import Reader
from aqua.logger import log_configure


def parse_arguments(args):
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(description='Trop. Rainfall CLI')
    parser.add_argument('-c', '--config', type=str,
                        help='yaml configuration file')
    parser.add_argument('-l', '--loglevel', type=str,
                        help='log level [default: WARNING]')

    # This arguments will override the configuration file if provided
    parser.add_argument('--model', type=str, help='model name',
                        required=False)
    parser.add_argument('--exp', type=str, help='experiment name',
                        required=False)
    parser.add_argument('--source', type=str, help='source name',
                        required=False)
    parser.add_argument('--outputdir', type=str, help='output directory',
                        required=False)
    parser.add_argument('--nproc', type=int, required=False,
                        help='the number of processes to run in parallel',
                        default=4)
    return parser.parse_args(args)


def validate_arguments(args):
    """
    Validate the types of command line arguments.

    Args:
        args: Parsed arguments from argparse.

    Raises:
        TypeError: If any argument is not of the expected type.
    """
    if args.config and not isinstance(args.config, str):
        raise TypeError("Config file path must be a string.")
    if args.loglevel and not isinstance(args.loglevel, str):
        raise TypeError("Log level must be a string.")
    if args.model and not isinstance(args.model, str):
        raise TypeError("Model name must be a string.")
    if args.exp and not isinstance(args.exp, str):
        raise TypeError("Experiment name must be a string.")
    if args.source and not isinstance(args.source, str):
        raise TypeError("Source name must be a string.")
    if args.outputdir and not isinstance(args.outputdir, str):
        raise TypeError("Output directory must be a string.")
    if args.nproc and not isinstance(args.nproc, int):
        raise TypeError("The number of processes (nproc) must be an integer.")


if __name__ == '__main__':
    args = parse_arguments(sys.argv[1:])
    validate_arguments(args)

    # change the current directory to the one of the CLI so that relative path works
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    if os.getcwd() != dname:
        os.chdir(dname)
        print(f'Moving from current directory to {dname} to run!')

    try:
        sys.path.insert(0, '../../')
        from tropical_rainfall import Tropical_Rainfall
    except ImportError as import_error:
        print(f"ImportError occurred: {import_error}")
        sys.exit(0)
    except Exception as custom_error:
        print(f"CustomError occurred: {custom_error}")
        sys.exit(0)
    else:
        print("Modules imported successfully.")

    file = get_arg(args, 'config', 'trop_rainfall_config.yml')
    print('Reading configuration yaml file..')
    config = load_yaml(file)

    loglevel = get_arg(
        args, 'loglevel', config['class_attributes']['loglevel'])
    logger = log_configure(log_name="Trop. Rainfall CLI",
                           log_level=loglevel)

    model = get_arg(args, 'model', config['data']['model'])
    exp = get_arg(args, 'exp', config['data']['exp'])
    source = get_arg(args, 'source', config['data']['source'])
    logger.debug(f"Accessing {model} {exp} {source} data")

    nproc = get_arg(args, 'nproc', config['compute_resources']['nproc'])

    freq = config['data']['freq']
    regrid = config['data']['regrid']
    s_year = config['data']['s_year']
    f_year = config['data']['f_year']
    s_month = config['data']['s_month']
    f_month = config['data']['f_month']

    machine = config['machine']
    logger.info(f"The machine is {machine}")

    path_to_output = get_arg(
        args, 'outputdir', config['path'][machine])

    if path_to_output is not None:
        path_to_netcdf = os.path.join(
            path_to_output, 'netcdf/'+model+'_'+exp+'_'+source+'/')
        path_to_pdf = os.path.join(
            path_to_output, 'pdf/'+model+'_'+exp+'_'+source+'/')
    name_of_netcdf = model+'_'+exp+'_'+source
    name_of_pdf = model+'_'+exp+'_'+source

    logger.debug(f"NetCDF folder: {path_to_netcdf}")
    logger.debug(f"PDF folder: {path_to_pdf}")

    trop_lat = config['class_attributes']['trop_lat']
    num_of_bins = config['class_attributes']['num_of_bins']
    first_edge = config['class_attributes']['first_edge']
    width_of_bin = config['class_attributes']['width_of_bin']

    model_variable = config['class_attributes']['model_variable']
    new_unit = config['class_attributes']['new_unit']

    color = config['plot']['color']
    figsize = config['plot']['figsize']
    legend = config['plot']['legend']
    xmax = config['plot']['xmax']
    plot_title = f"{model} {exp} {source} {regrid} {freq}"
    loc = config['plot']['loc']
    pdf_format = config['plot']['pdf_format']

    diag = Tropical_Rainfall(trop_lat=trop_lat, num_of_bins=num_of_bins,
                             first_edge=first_edge, width_of_bin=width_of_bin, loglevel=loglevel)
    reader = Reader(model=model, exp=exp, source=source, loglevel=loglevel, regrid=regrid, nproc=nproc)
    full_dataset = reader.retrieve(var=model_variable)

    test_sample = full_dataset.isel(time=slice(1, 11))
    tools = ToolsClass()

    if isinstance(regrid, str):
        regrid_bool = tools.check_need_for_regridding(test_sample, regrid)
    else:
        regrid_bool = False

    if isinstance(freq, str):
        freq_bool = tools.check_need_for_time_averaging(test_sample, freq)
    else:
        freq_bool = False

    first_year_in_dataset = full_dataset['time.year'][0].values
    last_year_in_dataset = full_dataset['time.year'][-1].values

    s_year = first_year_in_dataset if s_year is None else max(s_year, first_year_in_dataset)
    f_year = last_year_in_dataset if f_year is None else min(f_year, last_year_in_dataset)
    logger.debug(f"s_year/f_year are: {s_year}/{f_year}.")

    s_month = 1 if s_month is None else s_month
    f_month = 12 if f_month is None else f_month

    legend = f"{model} {exp} {source}" if legend is None else legend
    xmax = diag.num_of_bins*diag.width_of_bin if xmax is None else xmax
    logger.debug(f"xmax is : {xmax} {new_unit}.")

    for year in range(s_year, f_year+1):
        data_per_year = full_dataset.sel(time=str(year))
        if data_per_year.time.size != 0:
            for x in range(s_month, f_month+1):
                try:
                    data = data_per_year.sel(time=str(year)+'-'+str(x))
                    if freq_bool:
                        data = reader.timmean(data, freq=freq)
                    if regrid_bool:
                        data = reader.regrid(data)
                    diag.histogram(data, model_variable=model_variable,
                                   path_to_histogram=path_to_netcdf+f"{regrid}/{freq}/histograms/",
                                   threshold = 30, name_of_file=regrid+'_'+freq)
                except KeyError:
                    pass
                except Exception as e:
                        # Handle other exceptions
                        logger.error(f"An unexpected error occurred: {e}")
                logger.debug(f"Current Status: {x}/12 months processed in year {year}.")
        else:
            logger.warning("The specified year is not present in the dataset. " +
                           "Ensure the time range in your selection accurately reflects the available " +
                           "data. Check dataset time bounds and adjust your time selection parameters accordingly.")

    logger.info("The histograms are calculated and saved in storage.")
    try:
        hist_merged = diag.merge_list_of_histograms(path_to_histograms=path_to_netcdf+f"{regrid}/{freq}/histograms/", all=True,
                                                    start_year=s_year, end_year=f_year, start_month=s_month, end_month=f_month)

        add = diag.histogram_plot(hist_merged, figsize=figsize, new_unit=new_unit,
                            legend=legend, color=color, xmax=xmax, plot_title=plot_title, loc=loc,
                            path_to_pdf=path_to_pdf, pdf_format=pdf_format, name_of_file=name_of_pdf)

        mswep_folder_path = f'/work/bb1153/b382267/observations/MSWEP/{regrid}/{freq}'

        # Check if the folder exists
        if not os.path.exists(mswep_folder_path):
            logger.error(f"Error: The folder for MSWEP data with resolution '{regrid}' "
                         f"and frequency '{freq}' does not exist. Histograms for the "
                         "desired resolution and frequency have not been computed yet.")
        else:
            obs_merged = diag.merge_list_of_histograms(path_to_histograms=mswep_folder_path, all=True, start_year=s_year, end_year=f_year,
                                                       start_month=s_month, end_month=f_month)
            logger.info(f"The MSWEP data with resolution '{regrid}' and frequency '{freq}' are prepared for comparison.")

            diag.histogram_plot(hist_merged, figsize=figsize, new_unit=new_unit, add=add,
                                linewidth=2*diag.plots.linewidth, linestyle='--', color='tab:red',
                                legend=f"MSWEP", xmax=xmax, plot_title=plot_title, loc=loc,
                                path_to_pdf=path_to_pdf, pdf_format=pdf_format, name_of_file=name_of_pdf)


        logger.info("The histograms are plotted and saved in storage.")
    except FileNotFoundError as file_error:
        # Handle FileNotFoundError
        logger.error(f"FileNotFoundError occurred: {file_error}")
    except Exception as e:
        # Handle other exceptions
        logger.error(f"An unexpected error occurred: {e}")
    logger.info("Tropical Rainfall Diagnostic is terminated.")
