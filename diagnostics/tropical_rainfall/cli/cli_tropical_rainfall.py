import sys
try:
    # All nesessarry import for a cli diagnostic
    from aqua.util import load_yaml, get_arg
    import os
    import argparse
    from aqua import Reader
    from aqua.logger import log_configure
    sys.path.insert(0, '../../')
    from tropical_rainfall import Tropical_Rainfall
except ImportError as import_error:
    # Handle ImportError
    print(f"ImportError occurred: {import_error}")
    sys.exit(0)
except Exception as custom_error:
    print(f"CustomError occurred: {custom_error}")
    sys.exit(0)
else:
    print("Modules imported successfully.")


def parse_arguments(args):
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(description='Tropical Rainfall CLI')
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

    return parser.parse_args(args)


if __name__ == '__main__':

    print('Running tropical rainfall diagnostic...')
    args = parse_arguments(sys.argv[1:])

    file = get_arg(args, 'config', 'config/trop_rainfall_config.yml')
    print('Reading configuration yaml file..')
    config = load_yaml(file)

    loglevel = get_arg(
        args, 'loglevel', config['class_attributes']['loglevel'])
    logger = log_configure(log_name="Tropical Rainfall CLI",
                           log_level=loglevel)

    model = get_arg(args, 'model', config['data']['model'])
    exp = get_arg(args, 'exp', config['data']['exp'])
    source = get_arg(args, 'source', config['data']['source'])

    logger.debug(f"Accessing {model} {exp} {source} data")

    path_to_output = get_arg(
        args, 'outputdir', config['path']['path_to_output'])
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
    plot_title = model+'_'+exp+'_'+source
    loc = config['plot']['loc']
    pdf_format = config['plot']['pdf_format']

    reader = Reader(model=model, exp=exp, source=source, loglevel=loglevel)
    data = reader.retrieve(var=model_variable)

    try:
        diag = Tropical_Rainfall(trop_lat=trop_lat,       num_of_bins=num_of_bins,
                                 first_edge=first_edge,   width_of_bin=width_of_bin, loglevel=loglevel)
        hist = diag.histogram(data, model_variable=model_variable,  new_unit=new_unit,
                              path_to_histogram=path_to_netcdf+'histograms/', name_of_file=name_of_netcdf)

        hist_merged = diag.merge_list_of_histograms(
            path_to_histograms=path_to_netcdf+'histograms/',  all=True)
        diag.histogram_plot(hist_merged, figsize=figsize, new_unit=new_unit,
                            legend=legend, color=color, xmax=xmax, plot_title=plot_title, loc=loc,
                            path_to_pdf=path_to_pdf, pdf_format=pdf_format, name_of_file=name_of_pdf)
        logger.warning("The histogram is calculated, plotted, and saved in storage.")
    except ZeroDivisionError as zd_error:
        # Handle ZeroDivisionError
        logger.error(f"ZeroDivisionError occurred: {zd_error}")
    except ValueError as value_error:
        # Handle ValueError
        logger.error(f"ValueError occurred: {value_error}")
    except KeyError as key_error:
        # Handle KeyError
        logger.error(f"KeyError occurred: {key_error}")
    except FileNotFoundError as file_error:
        # Handle FileNotFoundError
        logger.error(f"FileNotFoundError occurred: {file_error}")
    except Exception as e:
        # Handle other exceptions
        logger.error(f"An unexpected error occurred: {e}")
    logger.info("Tropical Rainfall Diagnostic is terminated.")
