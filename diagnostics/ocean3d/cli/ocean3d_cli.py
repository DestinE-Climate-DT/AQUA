#!/usr/bin/env python3
# -*- coding: utf-8 -*-import sys
import argparse
import os
import sys

from aqua import Reader
from aqua.util import load_yaml, get_arg, create_folder
from aqua.exceptions import NoObservationError

# This is needed if loading from the cli directory
sys.path.insert(0, '../../..')
sys.path.insert(0, '../..')

from ocean3d import plot_stratification
from ocean3d import plot_spatial_mld_clim

from ocean3d import hovmoller_lev_time_plot
from ocean3d import time_series_multilevs
from ocean3d import multilevel_t_s_trend_plot
from ocean3d import zonal_mean_trend_plot


def parse_arguments(args):
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(description='Ocean3D CLI')

    parser.add_argument('--config', type=str,
                        help='yaml configuration file')

    # This arguments will override the configuration file is provided
    parser.add_argument('--model', type=str, help='Model name')
    parser.add_argument('--exp', type=str, help='Experiment name')
    parser.add_argument('--source', type=str, help='Source name')
    parser.add_argument('--outputdir', type=str,
                        help='Output directory')

    return parser.parse_args(args)


if __name__ == '__main__':

    print("Running ocean3d diagnostic...")
    args = parse_arguments(sys.argv[1:])

    # Read configuration file
    file = get_arg(args, 'config', 'config.yaml')
    print('Reading configuration yaml file..')
    ocean3d_config = load_yaml(file)

    model = get_arg(args, 'model', ocean3d_config['model'])
    exp = get_arg(args, 'exp', ocean3d_config['exp'])
    source = get_arg(args, 'source', ocean3d_config['source'])
    outputdir = get_arg(args, 'outputdir', ocean3d_config['outputdir'])

    create_folder(outputdir)

    print(f"Reader selecting for model={model}, exp={exp}, source={source}")
    try:
        reader = Reader(model, exp, source, fix=True)
    except KeyError:
        # NOTE: This should be a proper NoDataError
        print("NoDataError: No data available")
        sys.exit(0)

    data = reader.retrieve()

    # HACK: FESOM data has nz1 as the vertical dimension
    #       Check issue #531 for more details
    if model == 'FESOM':
        data = data.rename({"nz1": "lev"})

    try:
        hovmoller_lev_time_plot(data=data, region="Global Ocean", anomaly=False,
                                standardise=False, output=True,
                                output_dir=outputdir)

        hovmoller_lev_time_plot(data=data, region="Global Ocean",
                                anomaly=True, standardise=False,
                                anomaly_ref='Tmean', output=True,
                                output_dir=outputdir)

        hovmoller_lev_time_plot(data=data, region="Global Ocean",
                                anomaly=True, standardise=True,
                                anomaly_ref='Tmean', output=True,
                                output_dir=outputdir)
    except AttributeError:
        print("NoDataError: so or ocpt not found in the Dataset.")
        print("Not plotting hovmoller_lev_time_plot")

    try:
        time_series_multilevs(data=data, region='Global Ocean', anomaly=False,
                              standardise=False, anomaly_ref="FullValue",
                              customise_level=False, levels=list, output=True,
                              output_dir=outputdir)

        time_series_multilevs(data=data, region='Global Ocean', anomaly=True,
                              standardise=False, anomaly_ref="t0",
                              customise_level=False, levels=list, output=True,
                              output_dir=outputdir)
    except AttributeError:
        print("NoDataError: so or ocpt not found in the Dataset.")
        print("Not plotting time_series_multilevs")
    except ValueError:
        print("ValueError: No levels provided")
        print("Not plotting time_series_multilevs")

    try:
        multilevel_t_s_trend_plot(data=data, region='Global Ocean',
                                  customise_level=False,
                                  levels=None, output=True,
                                  output_dir=outputdir)
    except AttributeError:
        print("NoDataError: so or ocpt not found in the Dataset.")
        print("Not plotting multilevel_t_s_trend_plot")

    try:
        plot_stratification(data, region="Labrador Sea", time="February",
                            output=True, output_dir=outputdir)
        plot_stratification(data, region="Labrador Sea", time="DJF",
                            output=True, output_dir=outputdir)
    except NoObservationError:
        print("NoObservationError: No observation available")
        print("Not plotting plot_stratification")
    except AttributeError:
        print("NoDataError: so or ocpt not found in the Dataset.")
        print("Not plotting plot_stratification")

    try:
        plot_spatial_mld_clim(data, region="labrador_gin_seas", time="Mar",
                            overlap=True, output=True, output_dir=outputdir)
        plot_spatial_mld_clim(data, region="labrador_gin_seas", time="FMA",
                            overlap=True, output=True, output_dir=outputdir)
    except NoObservationError:
        print("NoObservationError: No observation available")
        print("Not plotting plot_spatial_mld_clim")
    except AttributeError:
        print("NoDataError: so or ocpt not found in the Dataset.")
        print("Not plotting plot_spatial_mld_clim")
