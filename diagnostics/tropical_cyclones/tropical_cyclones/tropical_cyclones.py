import os

import copy
import numpy as np
import pandas as pd
import xarray as xr
from aqua import Reader
from aqua.logger import log_configure
from .detect_nodes import DetectNodes
from .stitch_nodes import StitchNodes
from .tools.tcs_utils import lonlatbox
from .aqua_dask import AquaDask


class TCs(DetectNodes, StitchNodes):
    """
    This class contains all methods related to the TCs (Tropical Cyclones)
    diagnostic based on tempest-estremes tracking. It provides two main functions -
    "detect_nodes_zoomin" and "stitch_nodes_zoomin" - for detecting the nodes of TCs and
    producing tracks of selected variables stored in netcdf files, respectively.
    """

    def __init__(self, tdict=None,
                 paths=None, model=None, exp="None", source2d="None", source3d="None",
                 boxdim=10, lowgrid='r100', highgrid='r010', var2store=None,
                 streaming=False, frequency='6h',
                 startdate=None, enddate=None,
                 stream_step=1, stream_unit='days', stream_startdate=None,
                 loglevel='INFO',
                 orography=False,
                 nproc=1):
        """
        Constructor method that initializes the class attributes based on the
        input arguments or tdict dictionary.

        Args:
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

        Returns:
            A TCs object
        """

        self.logger = log_configure(loglevel, 'TCs')
        self.loglevel = loglevel

        self.nproc = nproc
        self.aquadask = AquaDask(nproc=nproc)

        if tdict is not None:
            self.paths = tdict['paths']
            self.model = tdict['dataset']['model']
            self.exp = tdict['dataset']['exp']
            self.source2d = tdict['dataset']['source2d']
            self.source3d = tdict['dataset']['source3d']
            self.boxdim = tdict['detect']['boxdim']
            self.lowgrid = tdict['grids']['lowgrid']
            self.highgrid = tdict['grids']['highgrid']
            self.var2store = tdict['varlist']
            self.frequency = tdict['time']['frequency']
            self.startdate = tdict['time']['startdate']
            self.enddate = tdict['time']['enddate']
            self.orography=orography
            if self.orography:
                self.orography_file = tdict['orography']['file_path'] + tdict['orography']['file_name']
        else:
            if paths is None:
                raise ValueError('Without paths defined you cannot go anywhere!')
            else:
                self.paths = paths
            if startdate is None or enddate is None:
                raise ValueError('Define startdate and/or enddate')
            self.model = model
            self.exp = exp
            self.source2d = source2d
            self.source3d = source3d
            self.boxdim = boxdim
            self.lowgrid = lowgrid
            self.highgrid = highgrid
            self.var2store = var2store
            self.frequency = frequency
            self.startdate = startdate
            self.enddate = enddate

        self.streaming = streaming
        if self.streaming:
            self.stream_step = stream_step
            self.stream_units = stream_unit
            self.stream_startdate = stream_startdate

        # create directory structure
        self.paths['tmpdir'] = os.path.join(
            self.paths['tmpdir'], self.model, self.exp)
        self.paths['fulldir'] = os.path.join(
            self.paths['fulldir'], self.model, self.exp)

        for path in self.paths:
            os.makedirs(self.paths[path], exist_ok=True)

        self.catalog_init()

    def loop_streaming(self, tdict):
        """
        Wrapper for data retrieve, DetectNodes and StitchNodes.
        Simulates streaming data processing by retrieving data in chunks
        and performing TCs node detection and stitching looping over time steps.

        Args:
            self: The current object instance.
            tdict: A dictionary containing various parameters for streaming and stitching.

        Returns:
            None
        """

        # do this to remove the last letter from streamstep! e.g. tdict['stream']['streamstep'] is defined as "10D" but we want only the value 10!
        numbers = [int(i) for i in tdict['stream']['streamstep'] if i.isdigit()]
        streamstep_n = int(''.join(map(str, numbers)))

        # Check if the character after the number is 'D'
        # if not expressed as "D", raise value error, since we need days for the time loop!
        if tdict['stream']['streamstep'][len(numbers)] != 'D':
            raise ValueError("Critical error! Stream step must be specified in days as 'D' in the config file!")
        
        # retrieve the data and call detect nodes on the first chunk of data
        self.data_retrieve()
        self.detect_nodes_zoomin()

        # parameters for stitch nodes (to save tracks of selected variables in netcdf)
        n_days_stitch = tdict['stitch']['n_days_freq'] + \
            2*tdict['stitch']['n_days_ext']
        last_run_stitch = self.stream_startdate


        # loop to simulate streaming
        while len(np.unique(self.data2d.time.dt.day)) == streamstep_n:   
            self.logger.warning(
                "New streaming from %s to %s", pd.to_datetime(self.stream_startdate), pd.to_datetime(self.stream_enddate))

            # retrieve data and call to Tempest DetectNodes
            self.data_retrieve()
            self.detect_nodes_zoomin()

            # add one hour since time ends at 23
            dayspassed = (np.datetime64(self.stream_enddate) + np.timedelta64(1, 'h') - np.datetime64(last_run_stitch)) / np.timedelta64(1, 'D')

            # call Tempest StitchNodes every n_days_freq days time period and save TCs tracks in a netcdf file

            if dayspassed >= n_days_stitch:
                end_run_stitch = np.datetime64(last_run_stitch) + \
                    np.timedelta64(tdict['stitch']['n_days_freq'], 'D')
                self.logger.warning(
                    'Running stitch nodes from %s to %s', pd.to_datetime(last_run_stitch), pd.to_datetime(end_run_stitch))
                self.stitch_nodes_zoomin(startdate=pd.to_datetime(last_run_stitch), enddate=pd.to_datetime(end_run_stitch),
                                        n_days_freq=tdict['stitch']['n_days_freq'], n_days_ext=tdict['stitch']['n_days_ext'])
                last_run_stitch = copy.deepcopy(end_run_stitch)

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
        self.logger.warning('Model %s - Exp: %s', self.model, self.exp)

        if self.streaming:
            self.logger.warning(
                'Initialised streaming for %s %s starting on %s', self.stream_step, self.stream_units, pd.to_datetime(self.stream_startdate))
        if self.model in 'IFS':
            self.varlist2d = ['msl', '10u', '10v', 'z']
            self.reader2d = Reader(model=self.model, exp=self.exp, source=self.source2d,
                                         regrid=self.lowgrid,
                                         streaming=self.streaming, aggregation=self.stream_step, loglevel=self.loglevel,
                                         startdate=self.startdate, enddate=self.enddate)
            self.varlist3d = ['z']
            self.reader3d = Reader(model=self.model, exp=self.exp, source=self.source3d,
                                         regrid=self.lowgrid,
                                         streaming=self.streaming, aggregation=self.stream_step, loglevel=self.loglevel,
                                         startdate=self.startdate, enddate=self.enddate)
            self.reader_fullres = Reader(model=self.model, exp=self.exp, source=self.source2d,
                                         regrid=self.highgrid,
                                         streaming=self.streaming, aggregation=self.stream_step, loglevel=self.loglevel,
                                         startdate=self.startdate, enddate=self.enddate)
            
        elif self.model in 'IFS-NEMO':
            self.varlist2d = ['msl', '10u', '10v']
            self.reader2d = Reader(model=self.model, exp=self.exp, source=self.source2d,
                                         regrid=self.lowgrid,
                                         streaming=self.streaming, aggregation=self.stream_step, loglevel=self.loglevel,
                                         startdate=self.startdate, enddate=self.enddate)
            self.varlist3d = ['z']
            self.reader3d = Reader(model=self.model, exp=self.exp, source=self.source3d,
                                         regrid=self.lowgrid,
                                         streaming=self.streaming, aggregation=self.stream_step, loglevel=self.loglevel,
                                         startdate=self.startdate, enddate=self.enddate)
            self.reader_fullres = Reader(model=self.model, exp=self.exp, source=self.source2d,
                                         regrid=self.highgrid,
                                         streaming=self.streaming, aggregation=self.stream_step, loglevel=self.loglevel,
                                         startdate=self.startdate, enddate=self.enddate)
        else:
            raise ValueError(f'Model {self.model} not supported')

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
            self.data2d = self.reader2d.retrieve(var=self.varlist2d)
            self.data3d = self.reader3d.retrieve(var=self.varlist3d, level=[300, 500])
            self.fullres = self.reader_fullres.retrieve(var=self.var2store)
            if isinstance(self.data2d, type(None)):
                self.logger.warning("End of streaming")
                raise SystemExit

        if self.streaming:
            self.stream_enddate = self.data2d.time[-1].values
            self.stream_startdate = self.data2d.time[0].values

            
        #if orography is provided in a file access it without reader
            
        if self.orography:
            self.logger.info("orography retrieved from file")
            self.orog = xr.open_dataset(self.orography_file)
            #rename var for detect nodes
            self.orog = self.orog.rename({'z': 'zs'})



    def store_fullres_field(self, xfield, nodes):
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
        for k in range(0, len(nodes['lon'])):
            # add safe condition: keep only data between 50S and 50N
            # if (float(nodes['lat'][k]) > -50) and (float(nodes['lat'][k]) < 50):
            box = lonlatbox(nodes['lon'][k], nodes['lat'][k], self.boxdim)
            mask = mask + xr.where((xfield.lon > box[0]) & (xfield.lon < box[1]) &
                                   (xfield.lat > box[2]) & (xfield.lat < box[3]), True, False)

        outfield = xfield.where(mask > 0)

        # if isinstance(mfield, xr.DataArray):
        #    outfield = xr.concat([mfield, outfield], dim = 'time')

        return outfield
