#!/usr/bin/env python3
# -*- coding: utf-8 -*-import sys

import argparse
import os
import sys

from aqua import Reader
from aqua.util import load_yaml, get_arg, create_folder
from aqua.exceptions import NoObservationError

from ocean3d import plot_stratification
from ocean3d import plot_spatial_mld_clim

from ocean3d import hovmoller_lev_time_plot
from ocean3d import time_series_multilevs
from ocean3d import multilevel_t_s_trend_plot
from ocean3d import zonal_mean_trend_plot

from aqua.util import find_vert_coord
from aqua.logger import log_configure



def parse_arguments(args):
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(description='Ocean3D CLI')

    parser.add_argument('--config', type=str,
                        help='yaml configuration file')
    parser.add_argument('-l', '--loglevel', type=str,
                        help='log level [default: WARNING]')
    
    # This arguments will override the configuration file is provided
    parser.add_argument('--model', type=str, help='Model name')
    parser.add_argument('--exp', type=str, help='Experiment name')
    parser.add_argument('--source', type=str, help='Source name')
    parser.add_argument('--outputdir', type=str,
                        help='Output directory')

    return parser.parse_args(args)

def ocean3d_diags(data, region = None,
                latS: float = None,
                latN: float = None,
                lonW: float = None,
                lonE: float = None,
                output_dir = str):
    
    
    hovmoller_lev_time_plot(data=data, lonE=lonE, lonW=lonW, latS=latS, latN=latN, region=region, anomaly=False, standardise=False, output=True, output_dir=output_dir)
    hovmoller_lev_time_plot(data=data, lonE=lonE, lonW=lonW, latS=latS, latN=latN, region=region, anomaly=False, standardise=True, output=True, output_dir=output_dir)
    hovmoller_lev_time_plot(data=data, lonE=lonE, lonW=lonW, latS=latS, latN=latN, region=region, anomaly= True, anomaly_ref= "t0", standardise=False, output=True, output_dir=output_dir)
    hovmoller_lev_time_plot(data=data, lonE=lonE, lonW=lonW, latS=latS, latN=latN, region=region, anomaly= True, anomaly_ref= "tmean", standardise=False, output=True, output_dir=output_dir)
    hovmoller_lev_time_plot(data=data, lonE=lonE, lonW=lonW, latS=latS, latN=latN, region=region, anomaly= True, anomaly_ref= "t0", standardise=True, output=True, output_dir=output_dir)
    hovmoller_lev_time_plot(data=data, lonE=lonE, lonW=lonW, latS=latS, latN=latN, region=region, anomaly= True, anomaly_ref= "tmean", standardise=True, output=True, output_dir=output_dir)
    
    time_series_multilevs(data=data, lonE=lonE, lonW=lonW, latS=latS, latN=latN, region=region, anomaly= False, standardise= False, customise_level=False, levels=list, output = True,  output_dir = output_dir)
    time_series_multilevs(data=data, lonE=lonE, lonW=lonW, latS=latS, latN=latN, region=region, anomaly= True, standardise= False, anomaly_ref="tmean", customise_level=False, levels=list, output = True,  output_dir = output_dir)
    time_series_multilevs(data=data, lonE=lonE, lonW=lonW, latS=latS, latN=latN, region=region, anomaly= True, standardise= False, anomaly_ref="t0", customise_level=False, levels=list, output = True,  output_dir = output_dir)
    time_series_multilevs(data=data, lonE=lonE, lonW=lonW, latS=latS, latN=latN, region=region, anomaly= True, standardise= True, anomaly_ref="tmean", customise_level=False, levels=list, output = True,  output_dir = output_dir)
    time_series_multilevs(data=data, lonE=lonE, lonW=lonW, latS=latS, latN=latN, region=region, anomaly= True, standardise= True, anomaly_ref="t0", customise_level=False, levels=list, output = True,  output_dir = output_dir)
    
    multilevel_t_s_trend_plot(data=data, lonE=lonE, lonW=lonW, latS=latS, latN=latN, region=region, customise_level=False, levels=None, output= True, output_dir =  output_dir)
    
    zonal_mean_trend_plot(data=data, lonE=lonE, lonW=lonW, latS=latS, latN=latN, region=region, output= True, output_dir= output_dir)
    
    for time in range(1,18):  # 1 to 12 is the months, then each number directs the seasonals and the yearly climatology
        plot_stratification(mod_data= data, lonE=lonE, lonW=lonW, latS=latS, latN=latN, region=region, time = time, output= True, output_dir= output_dir)
        # plot_spatial_mld_clim(mod_data= data, lonE=lonE, lonW=lonW, latS=latS, latN=latN, region=region, time = time, overlap= True,output= True, output_dir= output_dir)
    
    

def get_value_with_default(dictionary, key, default_value):
    try:
        return dictionary[key]
    except KeyError:
        return default_value
    
if __name__ == '__main__':
    args = parse_arguments(sys.argv[1:])

    loglevel = get_arg(args, 'loglevel', 'WARNING')
    logger = log_configure(log_name='Ocean3D CLI', log_level=loglevel)
    
    logger.info("Running ocean3d diagnostic...")

    # Read configuration file
    file = get_arg(args, 'config', 'config.yaml')
    logger.info('Reading configuration yaml file..')
    
    ocean3d_config = load_yaml(file)


    model = get_arg(args, 'model', ocean3d_config['model'])
    exp = get_arg(args, 'exp', ocean3d_config['exp'])
    source = get_arg(args, 'source', ocean3d_config['source'])
    outputdir = get_arg(args, 'outputdir', ocean3d_config['outputdir'])
    


    custom_regions = get_value_with_default(ocean3d_config, "custom_region", [])
    predefined_regions = get_value_with_default(ocean3d_config, "predefined_regions", [])

    
    create_folder(outputdir)

    logger.info(f"Reader selecting for model={model}, exp={exp}, source={source}")
    try:
        reader = Reader(model, exp, source, fix=True)
        data = reader.retrieve()

        vertical_coord = find_vert_coord(data)[0]
        data = data.rename({vertical_coord: "lev"})
    except KeyError:
        # NOTE: This should be a proper NoDataError
        logger.error("NoDataError: No data available")
        sys.exit(0)



    try:
        if custom_regions is not None:
            custom_regions = ocean3d_config["custom_region"] ### add fix if not present
            custom_region_dict = {}
            for custom_region in custom_regions:
                for coord in custom_region:
                    custom_region_dict.update(coord)
                lonE= custom_region_dict["lonE"]
                lonW= custom_region_dict["lonW"]
                latS= custom_region_dict["latS"]
                latN= custom_region_dict["latN"]

                ocean3d_diags(data, region=None, latS=latS, latN=latN, lonW=lonW, lonE=lonE, output_dir=outputdir)

    except AttributeError:
        logger.error("NoDataError: so or ocpt not found in the Dataset.")
        logger.error("Not producting ocean diagnostics")
            

        if predefined_regions is not None:
            predefined_regions = ocean3d_config["predefined_regions"] ### add fix if not present
            for predefined_region in predefined_regions:
                    ocean3d_diags(data, region=predefined_region, output_dir=outputdir)
    except AttributeError:
        logger.error("NoDataError: so or ocpt not found in the Dataset.")
        logger.error("Not plotting hovmoller_lev_time_plot")

