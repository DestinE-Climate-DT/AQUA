"""CLI interface to run the TempestExtemes TCs tracking"""

import copy
import numpy as np
import pandas as pd
from tropical_cyclones import TCs
from aqua.util import load_yaml
from aqua.logger import log_configure


mainlogger = log_configure('INFO', log_name='MAIN')

# load the config file
tdict = load_yaml('config/config_tcs.yaml')

# initialise tropical class with streaming options
tropical = TCs(tdict=tdict, streaming=True, stream_step=tdict['stream']['streamstep'], stream_unit="days",
               stream_startdate=tdict['time']['startdate'], loglevel="WARNING")

# retrieve the data and call detect nodes on the first chunk of data
tropical.data_retrieve()
tropical.detect_nodes_zoomin()

# parameters for stitch nodes (to save tracks of selected variables in netcdf)
n_days_stitch = tdict['stitch']['n_days_freq'] + tdict['stitch']['n_days_ext']
last_run_stitch = pd.Timestamp(tropical.startdate)

# loop to simulate streaming
while len(np.unique(tropical.data2d.time.dt.day)) == tdict['stream']['streamstep']:
    tropical.data_retrieve()
    mainlogger.warning(
        "New streaming from %s to %s", pd.Timestamp(tropical.stream_startdate).strftime('%Y%m%dT%H'), pd.Timestamp(tropical.stream_enddate).strftime('%Y%m%dT%H'))
    timecheck = (tropical.data2d.time.values >
                 np.datetime64(tdict['time']['enddate']))

    if timecheck.any():
        tropical.stream_enddate = tropical.data2d.time.values[np.where(timecheck)[
            0][0]-1]
        mainlogger.warning(
            'Modifying the last stream date %s', tropical.stream_enddate)

    tropical.detect_nodes_zoomin()

    if timecheck.any():
        break

    # add one hour since time ends at 23
    dayspassed = (tropical.stream_enddate + np.timedelta64(1,
                  'h') - last_run_stitch) / np.timedelta64(1, 'D')

    if dayspassed >= n_days_stitch:
        end_run_stitch = last_run_stitch + \
            np.timedelta64(tdict['stitch']['n_days_freq'], 'D')
        mainlogger.warning(
            'Running stitch nodes from %s to %s', last_run_stitch, end_run_stitch)
        tropical.stitch_nodes_zoomin(startdate=last_run_stitch, enddate=end_run_stitch,
                                     n_days_freq=tdict['stitch']['n_days_freq'], n_days_ext=tdict['stitch']['n_days_ext'])
        last_run_stitch = copy.deepcopy(end_run_stitch)
# problem: bring the time handling to pandas to avoid mismatch with dates
end_run_stitch = np.datetime64(tdict['time']['enddate'])
mainlogger.warning(
    'Running stitch nodes from %s to %s', last_run_stitch, end_run_stitch)
tropical.stitch_nodes_zoomin(startdate=pd.Timestamp(last_run_stitch), enddate=pd.Timestamp(end_run_stitch),
                             n_days_freq=tdict['stitch']['n_days_freq'], n_days_ext=tdict['stitch']['n_days_ext'])
