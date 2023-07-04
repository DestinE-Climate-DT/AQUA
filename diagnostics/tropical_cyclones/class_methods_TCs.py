import os
import xarray as xr
import subprocess
from aqua import Reader
from aqua.logger import log_configure
from glob import glob
import numpy as np
import pandas as pd
from datetime import datetime


class TCs():

    """
    This class contains all methods related to the TCs (Tropical Cyclones) diagnostic based on tempest-estremes tracking. It provides two main functions - "detect_nodes_zoomin" and "stitch_nodes_zoomin" - for detecting the nodes of TCs and producing tracks of selected variables stored in netcdf files, respectively.

    Attributes:

    tdict (dict): A dictionary containing various configurations for the TCs diagnostic. If tdict is provided, the configurations will be loaded from it, otherwise the configurations will be set based on the input arguments.
    paths (dict): A dictionary containing file paths for input and output files.
    model (str): The name of the weather model to be used for the TCs diagnostic. Default is "IFS".
    exp (str): The name of the weather model experiment to be used for the TCs diagnostic. Default is "tco2559-ng5".
    boxdim (int): The size of the box centred over the TCs centres in the detect_nodes_zoomin method. Default is 10.
    lowgrid (str): The low-resolution grid used for detecting the nodes of TCs. Default is 'r100'.
    highgrid (str): The high-resolution grid used for detecting the nodes of TCs. Default is 'r010'.
    var2store (list): The list of variables to be stored in netcdf files. Default is None.
    streaming (bool): A flag indicating whether the TCs diagnostic is performed in streaming mode. Default is False.
    frequency (str): The time frequency for processing the TCs diagnostic. Default is '6h'.
    startdate (str): The start date for processing the TCs diagnostic.
    enddate (str): The end date for processing the TCs diagnostic.
    stream_step (int): The number of stream units to move forward in each step in streaming mode. Default is 1.
    stream_unit (str): The unit of stream_step in streaming mode. Default is 'days'.
    stream_startdate (str): The start date for processing the TCs diagnostic in streaming mode.
    loglevel (str): The logging level for the TCs diagnostic. Default is 'INFO'.

    Methods:

    init(self, tdict=None, paths=None, model="IFS", exp="tco2559-ng5", boxdim=10, lowgrid='r100', highgrid='r010', var2store=None, streaming=False, frequency='6h', startdate=None, enddate=None, stream_step=1, stream_unit='days', stream_startdate=None, loglevel='INFO'): Constructor method that initializes the class attributes based on the input arguments or tdict dictionary.
    detect_nodes_zoomin(self): Method for detecting the nodes of TCs and storing variables in a box centred over the TCs centres at each time step.
                               Wrapper which calls the readwrite_from_intake, run_detect_nodes and store_detect_nodes methods in a time loop;
    stitch_nodes_zoomin(self): Method for producing tracks of selected variables stored in netcdf files.
    catalog_init(self): "catalog_init": initializes the Reader object for retrieving the atmospheric data needed (i.e. the input and output vars).
    data_retrieve(self, reset_stream=False): retrieves atmospheric data from the Reader objects and assigns them to the data2d, data3d, and fullres attributes of the Detector object. It updates the stream_startdate and stream_enddate attributes if streaming is True.
    set_time_window: updates the n_days_freq and n_days_ext attributes of the Detector object to set the time window for the tempest extremes analysis.
    readwrite_from_intake: regrids the atmospheric data, saves it to disk as a netCDF file, and updates the tempest_dictionary and tempest_filein attributes of the Detector object.
    run_detect_nodes: runs the tempest extremes DetectNodes command on the regridded atmospheric data specified by the tempest_dictionary and tempest_filein attributes, saves the output to disk, and updates the tempest_fileout attribute of the Detector object.

    """
    def __init__(self, tdict = None, 
                 paths = None, model="IFS", exp="tco2559-ng5", 
                 boxdim = 10, lowgrid='r100', highgrid='r010', var2store=None, 
                 streaming=False, frequency= '6h', 
                 startdate=None, enddate=None,
                 stream_step=1, stream_unit='days', stream_startdate=None,
                 loglevel = 'INFO'):
        
        self.logger = log_configure(loglevel, 'TCs')
        self.loglevel = loglevel
    

        if tdict is not None:
            self.paths = tdict['paths']
            self.model = tdict['dataset']['model']
            self.exp = tdict['dataset']['exp']
            self.source2d = tdict['dataset']['source2d']
            self.source3d = tdict['dataset']['source3d']
            self.boxdim = tdict['detect']['boxdim']
            self.lowgrid =  tdict['grids']['lowgrid']
            self.highgrid = tdict['grids']['highgrid']
            self.var2store = tdict['varlist']
            self.frequency = tdict['time']['frequency']
            self.startdate = tdict['time']['startdate']
            self.enddate = tdict['time']['enddate']


        else:

            if paths is None:
                raise Exception('Without paths defined you cannot go anywhere!')
            else:
                self.paths = paths
            if startdate is None or enddate is None: 
                raise Exception('Define startdate and/or enddate')
            self.model = model 
            self.exp = exp
            self.boxdim = boxdim
            self.lowgrid = lowgrid
            self.highgrid = highgrid
            self.var2store = var2store
            self.frequency = frequency
            self.startdate = startdate
            self.enddate = enddate

        self.streaming=streaming
        if self.streaming:
            self.stream_step=stream_step
            self.stream_units=stream_unit
            self.stream_startdate=stream_startdate

        for path in self.paths:
            os.makedirs(self.paths[path], exist_ok=True)

        self.catalog_init()

    def loop_streaming(self, tdict):

        # retrieve the data and call detect nodes on the first chunk of data
        self.data_retrieve()
        self.detect_nodes_zoomin()

        # parameters for stitch nodes (to save tracks of selected variables in netcdf)
        n_days_stitch = tdict['stitch']['n_days_freq'] + tdict['stitch']['n_days_ext']
        last_run_stitch = pd.Timestamp(self.startdate)

        # loop to simulate streaming
        while len(np.unique(self.data2d.time.dt.day)) == tdict['stream']['streamstep']:
            self.data_retrieve()
            self.logger.warning(f"New streaming from {pd.Timestamp(self.stream_startdate).strftime('%Y%m%dT%H')} to {pd.Timestamp(self.stream_enddate).strftime('%Y%m%dT%H')}")
            timecheck = (self.data2d.time.values > np.datetime64(tdict['time']['enddate']))
            
            if timecheck.any():
                self.stream_enddate = self.data2d.time.values[np.where(timecheck)[0][0]-1] 
                self.logger.warning(f'Modifying the last stream date {self.stream_enddate}') 

            # call to Tempest DetectNodes
            self.detect_nodes_zoomin()

            if timecheck.any():
                break
            
            # add one hour since time ends at 23
            dayspassed = (self.stream_enddate + np.timedelta64(1, 'h')- last_run_stitch) / np.timedelta64(1, 'D')

            # call Tempest StitchNodes every n_days_freq days time period and save TCs tracks in a netcdf file
            if (dayspassed >= n_days_stitch):
                end_run_stitch = last_run_stitch + np.timedelta64(tdict['stitch']['n_days_freq'], 'D')
                self.logger.warning(f'Running stitch nodes from {last_run_stitch} to {end_run_stitch}')
                self.stitch_nodes_zoomin(startdate=last_run_stitch, enddate=end_run_stitch,
                    n_days_freq=tdict['stitch']['n_days_freq'], n_days_ext=tdict['stitch']['n_days_ext'])
                last_run_stitch = copy.deepcopy(end_run_stitch)

        # end of the loop for the last chunk of data
        end_run_stitch = np.datetime64(tdict['time']['enddate'])
        self.logger.warning(f'Running stitch nodes from {last_run_stitch} to {end_run_stitch}')
        self.stitch_nodes_zoomin(startdate=pd.Timestamp(last_run_stitch), enddate=pd.Timestamp(end_run_stitch),
                    n_days_freq=tdict['stitch']['n_days_freq'], n_days_ext=tdict['stitch']['n_days_ext'])

    def detect_nodes_zoomin(self):
        """
        Detect nodes for the zoomed-in time range.

        Parameters:
        - self: Reference to the current instance of the class.

        Returns:
        None
        """
        if self.streaming:
            timerange = pd.date_range(start=self.stream_startdate, end=self.stream_enddate, freq=self.frequency)
        else:
            timerange = pd.date_range(start=self.startdate, end=self.enddate, freq=self.frequency)
        
        for tstep in timerange.strftime('%Y%m%dT%H'):
            self.logger.warning(f"processing time step {tstep}")
            self.readwrite_from_intake(tstep)
            self.run_detect_nodes(tstep)
            clean_files([self.tempest_filein])
            self.read_lonlat_nodes()
            self.store_detect_nodes(tstep)
        

    def stitch_nodes_zoomin(self, startdate, enddate, n_days_freq, n_days_ext):
        """
        Wrapper for run_stitch_nodes and store_stitch_nodes for selected time period.

        Args:
        - self: Reference to the current instance of the class.
        - startdate: Start date of the chosen period.
        - enddate: End date of the chosen period.
        - n_days_freq: Number of days for the frequency of time windows.
        - n_days_ext: Number of days for the extension of time windows.

        Returns:
            None
        """
        self.set_time_window(n_days_freq=n_days_freq, n_days_ext=n_days_ext)

        # periods specifies you want 1 block from startdate to enddate
        for block in pd.date_range(start=startdate, end=enddate, periods=1):
            dates_freq, dates_ext = self.time_window(block)
            self.logger.warning(f'running stitch nodes from {block.strftime("%Y%m%d")}-{dates_freq[-1].strftime("%Y%m%d")}')
            self.prepare_stitch_nodes(block, dates_freq, dates_ext)
            self.run_stitch_nodes(maxgap='6h')
            self.reorder_tracks()
            self.store_stitch_nodes(block, dates_freq)


    def catalog_init(self):
        """
        Initialize the catalog for data retrieval based on the specified model.

        Args:
        - self: Reference to the current instance of the class.

        Returns:
            None

        Raises:
        - Exception: If the specified model is not supported.
        """

        if self.streaming == True:
            self.logger.warning(f'Initialised streaming for {self.stream_step} {self.stream_units} starting on {self.stream_startdate}')
        if self.model in 'IFS':
            self.varlist2d = ['msl', '10u', '10v']
            self.reader2d = Reader(model=self.model, exp=self.exp, source=self.source2d, 
                                regrid=self.lowgrid, streaming=self.streaming, stream_step=self.stream_step, 
                                stream_unit=self.stream_units, stream_startdate=self.stream_startdate, loglevel=self.loglevel)
            self.varlist3d = ['z']
            self.reader3d = Reader(model=self.model, exp=self.exp, source=self.source3d, 
                                regrid=self.lowgrid, streaming=self.streaming, loglevel=self.loglevel,
                                stream_step=self.stream_step, stream_unit=self.stream_units, stream_startdate=self.stream_startdate)
            self.reader_fullres = Reader(model=self.model, exp=self.exp, source=self.source2d, 
                                        regrid=self.highgrid,
                                        streaming=self.streaming, stream_step=self.stream_step, loglevel=self.loglevel,
                                        stream_unit=self.stream_units, stream_startdate=self.stream_startdate)
        else:
            raise Exception(f'Model {self.model} not supported')
        

    def data_retrieve(self, reset_stream=False):
        """
        Retrieve the necessary 2D and 3D data for analysis.

        Args:
        - self: Reference to the current instance of the class.
        - reset_stream (optional): Boolean flag indicating whether to reset the stream. Default is False.

        Returns:
            None
        """

        if reset_stream:
            self.reader2d.reset_stream()
            self.reader3d.reset_stream()
            self.reader_fullres.reset_stream()
        
        # now retrieve 2d and 3d data needed  
        else:
            self.data2d = self.reader2d.retrieve(vars = self.varlist2d)
            self.data3d = self.reader3d.retrieve(vars = self.varlist3d)
            self.fullres = self.reader_fullres.retrieve( var = self.var2store)

        if self.streaming:     
            self.stream_enddate = self.data2d.time[-1].values
            self.stream_startdate = self.data2d.time[0].values

    def set_time_window(self, n_days_freq = 30, n_days_ext = 10):
        """
        Set the time window parameters for frequency and extension.

        Parameters:
        - self: Reference to the current instance of the class.
        - n_days_freq (optional): Number of days for the frequency of time windows. Default is 30.
        - n_days_ext (optional): Number of days for the extension of time windows. Default is 10.

        Returns:
        None
        """

        self.n_days_freq = n_days_freq
        self.n_days_ext = n_days_ext
        
    def readwrite_from_intake(self, timestep):
        """
        Read and write data from intake reader for the specified timestep.

        Argss:
        - self: Reference to the current instance of the class.
        - timestep: Timestep for which to read and write the data.

        Returns:
            None
        """

        self.logger.info(f'Running readwrite_from_intake() for {timestep}')

        outfield = 0
        fileout = os.path.join(self.paths['regdir'], f'regrid_{timestep}.nc')

        for var in self.varlist2d:
            self.logger.info(f'Regridding 2D data for {var}')
            lowres = self.reader2d.regrid(self.data2d[var].sel(time=timestep))
            if isinstance(outfield, xr.Dataset):
                if var in '10u':
                    varout = 'u10m'
                elif var in '10v':
                    varout = 'v10m'
                else: 
                    varout = var
                outfield = xr.merge([outfield, lowres.to_dataset(name=varout)])
            else:
                outfield = lowres.to_dataset(name=var)
        
        if self.exp=="tco2559-ng5":
            for var in self.varlist3d:
                self.logger.info(f'Regridding 3D data for {var}')
                lowres = self.reader3d.regrid(self.data3d[var].sel(time=timestep, plev=[30000,50000]))
                outfield = xr.merge([outfield, lowres.to_dataset(name=var)])

            outfield['plev'] = outfield['plev'].astype(float)
            outfield['plev'].attrs['units'] = 'Pa'
            
        elif self.exp=="tco2559-ng5-cycle3":
            for var in self.varlist3d:
                self.logger.info(f'Regridding 3D data for {var}')
                lowres = self.reader3d.regrid(self.data3d[var].sel(time=timestep, plev=[30000,50000]))
                outfield = xr.merge([outfield, lowres.to_dataset(name=var)])

            outfield['plev'] = outfield['plev'].astype(float)
            outfield['plev'].attrs['units'] = 'Pa'
            
        # check if output file exists
        if os.path.exists(fileout):
            os.remove(fileout)
            
        #then write netcdf file for tempest
        self.logger.info('Writing low res to disk..')
        outfield.to_netcdf(fileout)
        outfield.close()
        
        self.tempest_dictionary = {'lon': 'lon', 'lat': 'lat', 
                    'psl': 'msl', 'zg': 'z',
                    'uas': 'u10m', 'vas': 'v10m'}
        self.tempest_filein=fileout
        

    def run_detect_nodes(self, timestep) : 

        """"
        Basic function to call from command line tempest extremes DetectNodes
        Args:
            tempest_dictionary: python dictionary with variable names for tempest commands
            tempest_filein: file (netcdf) with low res data
            tempest_fileout: output file (.txt) from DetectNodes command
        Returns: 
            detect_string: output file from DetectNodes in string format 
        """
        
        self.logger.info(f'Running run_detect_nodes() for timestep {timestep}')
        
        tempest_filein = self.tempest_filein
        tempest_dictionary = self.tempest_dictionary
        tempest_fileout = os.path.join(self.paths['tmpdir'], 'tempest_output_' + timestep + '.txt')
        self.tempest_fileout = tempest_fileout

        
        detect_string= f'DetectNodes --in_data {tempest_filein} --timefilter 6hr --out {tempest_fileout} --searchbymin {tempest_dictionary["psl"]} ' \
        f'--closedcontourcmd {tempest_dictionary["psl"]},200.0,5.5,0;_DIFF({tempest_dictionary["zg"]}(30000Pa),{tempest_dictionary["zg"]}(50000Pa)),-58.8,6.5,1.0 --mergedist 6.0 ' \
        f'--outputcmd {tempest_dictionary["psl"]},min,0;_VECMAG({tempest_dictionary["uas"]},{tempest_dictionary["vas"]}),max,2 --latname {tempest_dictionary["lat"]} --lonname {tempest_dictionary["lon"]}'

        subprocess.run(detect_string.split(), stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)


    def read_lonlat_nodes(self):

        """
        Read from txt files output of DetectNodes the position of the centers of the TCs

        Args:

            tempest_fileout: output file from tempest DetectNodes

        Returns: 
        out: dictionary with 'date', 'lon' and 'lat' of the TCs centers
        """

        with open(self.tempest_fileout) as f:
            lines = f.readlines()
        first = lines[0].split('\t')
        date = first[0] + first[1].zfill(2) + first[2].zfill(2) + first[4].rstrip().zfill(2)
        lon_lat = [line.split('\t')[3:] for line in lines[1:]]
        self.tempest_nodes = {'date': date, 'lon': [val[0] for val in lon_lat], 'lat': [val[1] for val in lon_lat]}

    def store_detect_nodes(self, timestep, write_fullres=True):

        self.logger.info(f'Running store_detect_nodes() for timestep {timestep}')
        
        # in case you want to write netcdf file with ullres field after Detect Nodes
        if write_fullres:
          # loop on variables to write to disk only the subset of high res files
            for var in self.var2store:

                subselect = self.fullres[var].sel(time=timestep)
                data = self.reader_fullres.regrid(subselect)
                data.name = var
                xfield = self.store_fullres_field(0, data, self.tempest_nodes)
                self.logger.info(f'store_fullres_field for timestep {timestep}')
                store_file = os.path.join(self.paths['fulldir'], f'TC_{var}_{timestep}.nc')
                write_fullres_field(xfield, store_file)


    def store_fullres_field(self, mfield, xfield, nodes): 

        """
        Create xarray object that keep only the values of a field around the TC nodes
        
        Args:
            mfield: xarray object (set to 0 at the first timestep of a loop)
            xfield: xarray object to be concatenated with mfield
            nodes: dictionary with date, lon, lat of the TCs centres
            boxdim: length of the lat lon box (required for lonlatbox funct)

        Returns:
            outfield: xarray object with values only in the box around the TC nodes centres for all time steps
        """

        mask = xfield * 0
        for k in range(0, len(nodes['lon'])) :
            # add safe condition: keep only data between 50S and 50N
            #if (float(nodes['lat'][k]) > -50) and (float(nodes['lat'][k]) < 50): 
            box = lonlatbox(nodes['lon'][k], nodes['lat'][k], self.boxdim)
            mask = mask + xr.where((xfield.lon > box[0]) & (xfield.lon < box[1]) & (xfield.lat > box[2]) & (xfield.lat < box[3]), True, False)

        outfield = xfield.where(mask>0)

        if isinstance(mfield, xr.DataArray):
            outfield = xr.concat([mfield, outfield], dim = 'time')
    
        return outfield
    
    def prepare_stitch_nodes(self, block, dates_freq, dates_ext):      
        """
        Prepares the stitch nodes for the given block and time window by gathering relevant file paths and filenames.

        Args:
            block: Block for which the stitch nodes are being prepared.
            dates_freq: DatetimeIndex with daily frequency for the time window.
            dates_ext: Extended DatetimeIndex with the time window around the initial date.

        Returns:
            None
        """

        # create list of file paths to include in glob pattern
        file_paths = [os.path.join(self.paths['tmpdir'], f"tempest_output_{date}T??.txt") for date in dates_ext.strftime('%Y%m%d')]
        # use glob to get list of filenames that match the pattern
        filenames = []
        for file_path in file_paths:
            filenames.extend(sorted(glob(file_path)))

        self.tempest_filenames = filenames
        self.track_file = os.path.join(self.paths['tmpdir'], f'tempest_track_{block.strftime("%Y%m%d")}-{dates_freq[-1].strftime("%Y%m%d")}.txt')


    def time_window(self, initial_date):
        """
        Creates a time window around the initial date by extending the dates index.

        Args:
            initial_date: Initial date of the time window.

        Returns:
            dates_freq: DatetimeIndex with daily frequency starting from the initial date.
            dates_ext: Extended DatetimeIndex with the time window around the initial date.
        """

        # create DatetimeIndex with daily frequency
        dates_freq = pd.date_range(start=initial_date, periods=self.n_days_freq, freq='D')

        before = dates_freq.shift(-self.n_days_ext, freq='D')[0:self.n_days_ext]
        after = dates_freq.shift(+self.n_days_ext, freq='D')[-self.n_days_ext:]

        # concatenate the indexes to create a single index
        dates_ext = before.append(dates_freq).append(after)
        return dates_freq, dates_ext

    def run_stitch_nodes(self, maxgap = '24h', mintime = '54h'):

        """"
        Basic function to call from command line tempest extremes StitchNodes

        Args:
            infiles_list: .txt file (output from DetectNodes) with all TCs centres dates&coordinates
            tempest_fileout: output file (.txt) from StitchNodes command
            dir: directory where to store the temporary file with all concatenated detect nodes

        Returns: 
        stitch_string: output file from StitchNodes in string format 
        """

        full_nodes = os.path.join(self.paths['tmpdir'],'full_nodes.txt')
        if os.path.exists(full_nodes):
                os.remove(full_nodes)

        with open(full_nodes, 'w') as outfile:
            for fname in sorted(self.tempest_filenames):
                with open(fname) as infile:
                    outfile.write(infile.read())

        stitch_string = f'StitchNodes --in {full_nodes} --out {self.track_file} --in_fmt lon,lat,slp,wind --range 8.0 --mintime {mintime} ' \
            f'--maxgap {maxgap} --threshold wind,>=,10.0,10;lat,<=,50.0,10;lat,>=,-50.0,10'
        
        subprocess.run(stitch_string.split(), stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    
    def reorder_tracks(self):

        """
        Open the total track files, reorder tracks in time then creates a dict with time and lons lats pair of every track

        Args:
            track_file: input track file from tempest StitchNodes
        
        Returns:
            reordered_tracks: python dictionary with date lon lat of TCs centres after StitchNodes has been run
        """

        with open(self.track_file) as file:
            lines = file.read().splitlines()
            parts_list = [line.split("\t") for line in lines if len(line.split("\t")) > 6]
            #print(parts_list)
            tracks ={'slon': [parts[3] for parts in parts_list],
                'slat':  [parts[4] for parts in parts_list],
                'date': [parts[7] + parts[8].zfill(2) + parts[9].zfill(2) + parts[10].zfill(2) for parts in parts_list],
            }

        reordered_tracks = {}
        for tstep in tracks['date'] : 
            #idx = tracks['date'].index(tstep)
            idx = [i for i, e in enumerate(tracks['date']) if e == tstep]
            reordered_tracks[tstep] = {}
            reordered_tracks[tstep]['date'] = tstep
            reordered_tracks[tstep]['lon'] = [tracks['slon'][k] for k in idx]
            reordered_tracks[tstep]['lat'] = [tracks['slat'][k] for k in idx]
            
        self.reordered_tracks=reordered_tracks

    def store_stitch_nodes(self, block, dates_freq, write_fullres=True):
        """
        Store stitched tracks for each variable around the Nodes in NetCDF files.

        Args:
        - self: Reference to the current instance of the class.
        - block: Block representing a specific time period.
        - dates_freq: Frequencies of dates used for storing the tracks.
        - write_fullres (optional): Boolean flag indicating whether to write full-resolution fields. Default is True.

        Returns:
                None
        """

        if write_fullres:
            for var in self.var2store : 
                self.logger.warning(f"storing stitched tracks for {var}")
                # initialise full_res fields at 0 before the loop
                xfield = 0
                for idx in self.reordered_tracks.keys():
                    #print(datetime.strptime(idx, '%Y%m%d%H').strftime('%Y%m%d'))
                    #print (dates.strftime('%Y%m%d'))
                    if datetime.strptime(idx, '%Y%m%d%H').strftime('%Y%m%d') in dates_freq.strftime('%Y%m%d'):

                        timestep = datetime.strptime(idx, '%Y%m%d%H').strftime('%Y%m%dT%H')
                        fullres_file = os.path.join(self.paths['fulldir'], f'TC_{var}_{timestep}.nc')
                        fullres_field = xr.open_mfdataset(fullres_file)[var]

                        # get the full res field and store the required values around the Nodes
                        xfield = self.store_fullres_field(xfield, fullres_field, self.reordered_tracks[idx])

                self.logger.info(f"writing netcdf file")

                # store the file
                store_file = os.path.join(self.paths['fulldir'], f'tempest_tracks_{var}_{block.strftime("%Y%m%d")}-{dates_freq[-1].strftime("%Y%m%d")}.nc')
                write_fullres_field(xfield, store_file)

def clean_files(filelist):
    """
    Removes the specified files from the filesystem.

    Args:
    - filelist (str or list): A single filename or a list of filenames to be removed.

    Returns:
        None

    """
    if isinstance(filelist, str):
        filelist = [filelist]

    for fileout in filelist :
        if os.path.exists(fileout):
            os.remove(fileout)

def lonlatbox(lon, lat, delta) : 
    """
    Define the list for the box to retain high res data in the vicinity of the TC centres

    Args:
        lon: longitude of the TC centre
        lat: latitude of the TC centre
        delta: length in degrees of the lat lon box

    Returns: 
       box: list with the box coordinates
    """
    return [float(lon) - delta, float(lon) +delta, float(lat) -delta, float(lat) + delta]

def write_fullres_field(gfield, filestore): 

    """
    Writes the high resolution file (netcdf) format with values only within the box
    Args:
        gfield: field to write
        filestore: file to save
    """

    time_encoding = {'units': 'days since 1970-01-01',
                 'calendar': 'standard',
                 'dtype': 'float64'}
    var_encoding = {"zlib": True, "complevel": 1}
    if isinstance(gfield, int):
        print(f"No tracks to write")
    else:
        gfield.where(gfield!=0).to_netcdf(filestore,  encoding={'time': time_encoding, gfield.name: var_encoding})
        gfield.close()
