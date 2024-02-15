import os
import subprocess
import xarray as xr
import pandas as pd
from glob import glob
from time import time
from datetime import datetime
from .tools.tcs_utils import write_fullres_field


class StitchNodes():
    """
    Class Mixin to take care of stitch nodes
    """

    def stitch_nodes_zoomin(self, startdate, enddate, n_days_freq, n_days_ext):
        """
        Method for producing tracks of selected variables stored in netcdf files.
        Wrapper for run_stitch_nodes and store_stitch_nodes for selected time period.

        Args:
            startdate: Start date of the chosen period.
            enddate: End date of the chosen period.
            n_days_freq: Number of days for the frequency of time windows.
            n_days_ext: Number of days for the extension of time windows.

        Returns:
            None
        """


        self.set_time_window(n_days_freq=n_days_freq, n_days_ext=n_days_ext)

        # periods specifies you want 1 block from startdate to enddate
        for block in pd.date_range(start=startdate, end=enddate, periods=1):
            tic = time()
            dates_freq, dates_ext = self.time_window(block)
            self.logger.info(
                f'running stitch nodes from {block.strftime("%Y-%m-%d")}-{enddate.strftime("%Y-%m-%d")}')
            self.prepare_stitch_nodes(block, dates_freq, dates_ext)
            self.run_stitch_nodes(maxgap='6h')
            self.reorder_tracks()
            self.store_stitch_nodes(block, dates_freq)
            toc = time()
            self.logger.info(
                'StitchNodes done in {:.4f} seconds'.format(toc - tic))

    def set_time_window(self, n_days_freq=30, n_days_ext=10):
        """
        Set the time window parameters for frequency and extension.

        Parameters:
            n_days_freq (optional): Number of days for the frequency of time windows.
            n_days_ext (optional): Number of days for the extension of time windows.

        Returns:
            None
        """

        self.n_days_freq = n_days_freq
        self.n_days_ext = n_days_ext

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
        dates_freq = pd.date_range(
            start=initial_date, periods=self.n_days_freq, freq='D')

        before = dates_freq.shift(-self.n_days_ext,
                                  freq='D')[0:self.n_days_ext]
        after = dates_freq.shift(+self.n_days_ext, freq='D')[-self.n_days_ext:]

        # concatenate the indexes to create a single index
        dates_ext = before.append(dates_freq).append(after)

        self.logger.info(dates_freq)
        self.logger.info(dates_ext)
        return dates_freq, dates_ext

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
        file_paths = [os.path.join(
            self.paths['tmpdir'], f"tempest_output_{date}T??.txt") for date in dates_ext.strftime('%Y%m%d')]
        # use glob to get list of filenames that match the pattern
        filenames = []
        for file_path in file_paths:
            filenames.extend(sorted(glob(file_path)))

        self.logger.info(filenames)

        self.tempest_filenames = filenames
        self.track_file = os.path.join(
            self.paths['tmpdir'], f'tempest_track_{block.strftime("%Y%m%d")}-{dates_freq[-1].strftime("%Y%m%d")}.txt')

    def run_stitch_nodes(self, maxgap='24h', mintime='54h'):
        """"
        Basic function to call from command line tempest extremes StitchNodes.

        Args:
            maxgap (str): The maximum time gap allowed between consecutive nodes.
            mintime (str): The minimum track duration required for a node to be included.

        Returns:
            None
        """

        self.logger.info('Running stitch nodes...')
        full_nodes = os.path.join(self.paths['tmpdir'], 'full_nodes.txt')
        if os.path.exists(full_nodes):
            os.remove(full_nodes)

        with open(full_nodes, 'w') as outfile:
            for fname in sorted(self.tempest_filenames):
                with open(fname) as infile:
                    outfile.write(infile.read())
                    
        # if the orography is found run stitch nodes accordingly
        if 'z' in self.lowres2d.data_vars or self.orography:
            stitch_string = f'StitchNodes --in {full_nodes} --out {self.track_file} --in_fmt lon,lat,slp,wind,zs --range 8.0 --mintime {mintime} ' \
                f'--maxgap {maxgap} --threshold wind,>=,10.0,10;lat,<=,50.0,10;lat,>=,-50.0,10;zs,<=,1500.0,10'
            self.logger.info(stitch_string)
            
        # if the orography is found run stitch nodes accordingly
        else:
            stitch_string = f'StitchNodes --in {full_nodes} --out {self.track_file} --in_fmt lon,lat,slp,wind --range 8.0 --mintime {mintime} ' \
                f'--maxgap {maxgap} --threshold wind,>=,10.0,10;lat,<=,50.0,10;lat,>=,-50.0,10'
            self.logger.info(stitch_string)

        subprocess.run(stitch_string.split(), stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        
        self.logger.warning(f'Tracked into {self.track_file}!')

    def reorder_tracks(self):
        """
        Open the total track files, reorder tracks in time then creates a dict with time and lons lats pair of every track.

        Args:
            track_file: input track file from tempest StitchNodes

        Returns:
            Python dictionary with date lon lat of TCs centres after StitchNodes has been run
        """
        # if the orography is found run stitch nodes accordingly
        if 'z' in self.lowres2d.data_vars or self.orography:
            with open(self.track_file) as file:
                lines = file.read().splitlines()
                parts_list = [line.split("\t")
                            for line in lines if len(line.split("\t")) > 6]
                # print(parts_list)
                tracks = {'slon': [parts[3] for parts in parts_list],
                        'slat':  [parts[4] for parts in parts_list],
                        'date': [parts[8] + parts[9].zfill(2) + parts[10].zfill(2) + parts[11].zfill(2) for parts in parts_list],
                        }
        # if orography is not found adapt the parsing of the columns (orog column is missing)
        else:
            with open(self.track_file) as file:
                lines = file.read().splitlines()
                parts_list = [line.split("\t")
                            for line in lines if len(line.split("\t")) > 6]
                # print(parts_list)
                tracks = {'slon': [parts[3] for parts in parts_list],
                        'slat':  [parts[4] for parts in parts_list],
                        'date': [parts[7] + parts[8].zfill(2) + parts[9].zfill(2) + parts[10].zfill(2) for parts in parts_list],
                        }

        reordered_tracks = {}
        for tstep in tracks['date']:
            # idx = tracks['date'].index(tstep)
            idx = [i for i, e in enumerate(tracks['date']) if e == tstep]
            reordered_tracks[tstep] = {}
            reordered_tracks[tstep]['date'] = tstep
            reordered_tracks[tstep]['lon'] = [tracks['slon'][k] for k in idx]
            reordered_tracks[tstep]['lat'] = [tracks['slat'][k] for k in idx]

        self.reordered_tracks = reordered_tracks

    def store_stitch_nodes(self, block, dates_freq, write_fullres=True):
        """
        Store stitched tracks for each variable around the Nodes in NetCDF files.

        Args:
            block: Block representing a specific time period.
            dates_freq: Frequencies of dates used for storing the tracks.
            write_fullres (optional): Boolean flag indicating whether to write full-resolution fields. Default is True.

        Returns:
            None
        """

        if write_fullres:
            datalist = []
            for idx in self.reordered_tracks.keys():
                # print(datetime.strptime(idx, '%Y%m%d%H').strftime('%Y%m%d'))
                # print (dates.strftime('%Y%m%d'))
                if datetime.strptime(idx, '%Y%m%d%H').strftime('%Y%m%d') in dates_freq.strftime('%Y%m%d'):

                    timestep = datetime.strptime(
                        idx, '%Y%m%d%H').strftime('%Y%m%dT%H')
                    self.logger.info('Processing timestep %s', timestep)
                    fullres_file = os.path.join(
                        self.paths['fulldir'], f'TC_fullres_{timestep}.nc')
                    fullres_field = xr.open_mfdataset(fullres_file)

                    # get the full res field and store the required values around the Nodes
                    datalist.append(self.store_fullres_field(
                        fullres_field, self.reordered_tracks[idx]))

            if len(datalist) > 0:
                xfield = xr.concat(datalist, dim='time')
                store_file = os.path.join(self.paths['fulldir'],
                                          f'tempest_tracks_{block.strftime("%Y%m%d")}-{dates_freq[-1].strftime("%Y%m%d")}.nc')
                write_fullres_field(xfield, store_file, self.aquadask.dask)
                fullres_field.close()
            # clean_files([fullres_file])

            # for var in self.var2store :
            #     #self.logger.warning(f"storing stitched tracks for {var}")
            #     # initialise full_res fields at 0 before the loop
            #     xfield = 0
            #     for idx in self.reordered_tracks.keys():
            #         #print(datetime.strptime(idx, '%Y%m%d%H').strftime('%Y%m%d'))
            #         #print (dates.strftime('%Y%m%d'))
            #         if datetime.strptime(idx, '%Y%m%d%H').strftime('%Y%m%d') in dates_freq.strftime('%Y%m%d'):

            #             timestep = datetime.strptime(idx, '%Y%m%d%H').strftime('%Y%m%dT%H')
            #             fullres_file = os.path.join(self.paths['fulldir'], f'TC_{var}_{timestep}.nc')
            #             fullres_field = xr.open_mfdataset(fullres_file)[var]

            #             # get the full res field and store the required values around the Nodes
            #             xfield = self.store_fullres_field(xfield, fullres_field, self.reordered_tracks[idx])

            #     self.logger.info(f"writing netcdf file")

            #     # store the file
            #     store_file = os.path.join(self.paths['fulldir'], f'tempest_tracks_{var}_{block.strftime("%Y%m%d")}-{dates_freq[-1].strftime("%Y%m%d")}.nc')
            #     write_fullres_field(xfield, store_file, self.nproc)
