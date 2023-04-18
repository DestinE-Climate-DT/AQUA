"""CLI interface to run the TempestExtemes TCs tracking"""

from TCs_class_methods import TCs
from aqua.util import load_yaml
from aqua.logger import log_configure
import numpy as np
from pandas import Timestamp
import copy

mainlogger = log_configure('INFO', log_name='MAIN')
tdict = load_yaml('config/config_levante.yml')
n_days_stitch = tdict['stitch']['n_days_freq'] + tdict['stitch']['n_days_ext']
stream_step = 3

tropical = TCs(tdict=tdict, streaming=True, stream_step=stream_step, stream_unit="days", 
               stream_startdate=tdict['time']['startdate'], loglevel = "WARNING")

tropical.data_retrieve()
tropical.detect_nodes_zoomin()
last_run_stitch = np.datetime64(tropical.startdate)
while len(np.unique(tropical.data2d.time.dt.day)) == stream_step:
    tropical.data_retrieve()
    mainlogger.warning(f'New loop: {tropical.stream_startdate}----{tropical.stream_enddate}!')
    timecheck = (tropical.data2d.time.values > np.datetime64(tdict['time']['enddate']))
    
    if timecheck.any():
        tropical.stream_enddate = tropical.data2d.time.values[np.where(timecheck)[0][0]-1] 
        mainlogger.warning(f'Modifying the last stream date {tropical.stream_enddate}') 

    tropical.detect_nodes_zoomin()

    if timecheck.any():
        break

    dayspassed = (tropical.stream_enddate - last_run_stitch) / np.timedelta64(1, 'D')
    if (dayspassed >= n_days_stitch):
        end_run_stitch = last_run_stitch + np.timedelta64(tdict['stitch']['n_days_freq'], 'D')
        mainlogger.warning(f'Running stitch nodes {last_run_stitch}---{end_run_stitch}')
        tropical.stitch_nodes_zoomin(startdate=Timestamp(last_run_stitch), enddate=Timestamp(end_run_stitch),
            n_days_freq=tdict['stitch']['n_days_freq'], n_days_ext=tdict['stitch']['n_days_ext'])
        last_run_stitch = copy.deepcopy(end_run_stitch)

end_run_stitch = np.datetime64(tdict['time']['enddate'])
mainlogger.warning(f'Running stitch nodes {last_run_stitch}---{end_run_stitch}')
tropical.stitch_nodes_zoomin(startdate=Timestamp(last_run_stitch), enddate=Timestamp(end_run_stitch),
            n_days_freq=tdict['stitch']['n_days_freq'], n_days_ext=tdict['stitch']['n_days_ext'])
        
