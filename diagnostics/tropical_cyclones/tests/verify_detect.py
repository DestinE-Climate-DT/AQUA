#!/usr/bin/env python3

import pandas as pd
import numpy as np
import xarray as xr
import os
import sys

from tropical_cyclones import TCs
from aqua.util import load_yaml
from aqua.logger import log_configure
mainlogger = log_configure('INFO', log_name='MAIN')

if __name__ == '__main__':

    

    # load the config file
    tdict = load_yaml('tester.yml')

    # initialise tropical class with streaming options
    tropical = TCs(tdict=tdict, streaming=True, 
                    stream_step=tdict['stream']['streamstep'], 
                    stream_unit="days", 
                    stream_startdate=tdict['time']['startdate'], 
                    loglevel = "INFO",
                    nproc=1)
    
    tropical.loop_streaming(tdict)

    # retrieve the data and call detect nodes on the first chunk of data
    #tropical.data_retrieve()
    #tropical.detect_nodes_zoomin()

    #tropical.stitch_nodes_zoomin(startdate='2020-01-20', enddate='2020-01-31',
    #                             n_days_freq=tdict['stitch']['n_days_freq'], 
    #                             n_days_ext=tdict['stitch']['n_days_ext'])