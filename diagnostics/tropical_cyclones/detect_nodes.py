import os
import subprocess
from time import time
import xarray as xr
import pandas as pd
from .tcs_utils import clean_files, write_fullres_field


class DetectNodes():
    """
    Class Mixin to take care of detect nodes.
    """

    def detect_nodes_zoomin(self):
        """
        Method for detecting the nodes of TCs and storing variables in a box centred over the TCs centres at each time step.
        Wrapper to read, prepare data and run DetectNodes at each time step

        Args:
            None

        Returns:
            None
        """
        if self.streaming:
            timerange = pd.date_range(start=self.stream_startdate,
                                      end=self.stream_enddate,
                                      freq=self.frequency)
        else:
            timerange = pd.date_range(start=self.startdate,
                                      end=self.enddate,
                                      freq=self.frequency)

        self.aquadask.set_dask()
        for tstep in timerange.strftime('%Y%m%dT%H'):
            tic = time()
            self.logger.warning(f"processing time step {tstep}")
            self.readwrite_from_intake(tstep)
            self.run_detect_nodes(tstep)
            #clean_files([self.tempest_filein])
            self.read_lonlat_nodes()
            self.store_detect_nodes(tstep)
            toc = time()
            self.logger.info(
                'DetectNodes done in {:.4f} seconds'.format(toc - tic))
        self.aquadask.close_dask()

    def readwrite_from_intake(self, timestep):
        """
        Read and write data from intake reader for the specified timestep.

        Args:
            timestep: Timestep for which to read and write the data.

        Returns:
            None
        """

        self.logger.info(f'Running readwrite_from_intake() for {timestep}')

        fileout = os.path.join(self.paths['tmpdir'], f'regrid_{timestep}.nc')

        if self.model == 'IFS':
            # TO BE IMPROVED: check pressure levels units
            # an import could be using metpy
            # lowres3d = lowres3d.metpy.convert_coordinate_units('plev', 'Pa')

            # this assumes that only required 2D data has been retrieved
            lowres2d = self.reader2d.regrid(self.data2d.sel(time=timestep))
            
            lowres2d.to_netcdf("prova.nc")
            # rename some variables to run DetectNodes command
            if '10u' in lowres2d.data_vars:
                lowres2d = lowres2d.rename({'10u': 'u10m'})
            if '10v' in lowres2d.data_vars:
                lowres2d = lowres2d.rename({'10v': 'v10m'})
            # this is required to avoid conflict between z 3D and z 2D (orography)
            if 'z' in lowres2d.data_vars:
                lowres2d = lowres2d.rename({'z': 'zs'})
                
            lowres3d = self.reader3d.regrid(
                self.data3d.sel(time=timestep, plev=[30000, 50000]))
            outfield = xr.merge([lowres2d, lowres3d])

        else:
            raise KeyError(f'Atmospheric model {self.model} not supported')

        # check if output file exists
        if os.path.exists(fileout):
            os.remove(fileout)

        # then write netcdf file for tempest
        self.logger.info('Writing low res to disk..')
        outfield.to_netcdf(fileout)
        outfield.close()

        self.tempest_dictionary = {
            'lon': 'lon', 'lat': 'lat',
            'psl': 'msl', 'zg': 'z', 'orog': 'zs',
            'uas': 'u10m', 'vas': 'v10m'}
        self.tempest_filein = fileout

    def read_lonlat_nodes(self):
        """
        Read from txt files output of DetectNodes the position of the centers of the TCs

        Args:
            tempest_fileout: output file from tempest DetectNodes

        Returns:
            Dictionary with 'date', 'lon' and 'lat' of the TCs centers
        """

        with open(self.tempest_fileout) as f:
            lines = f.readlines()
        first = lines[0].split('\t')
        date = first[0] + first[1].zfill(2) + \
            first[2].zfill(2) + first[4].rstrip().zfill(2)
        lon_lat = [line.split('\t')[3:] for line in lines[1:]]
        self.tempest_nodes = {'date': date,
                              'lon': [val[0] for val in lon_lat],
                              'lat': [val[1] for val in lon_lat]}

    def run_detect_nodes(self, timestep):
        """"
        Basic function to call from command line tempest extremes DetectNodes.
        Runs the tempest extremes DetectNodes command on the regridded atmospheric data specified by the tempest_dictionary and tempest_filein attributes,
        saves the output to disk, and updates the tempest_fileout attribute of the Detector object.

        Args:
            tempest_dictionary: python dictionary with variable names for tempest commands
            tempest_filein: file (netcdf) with low res data
            tempest_fileout: output file (.txt) from DetectNodes command

        Returns:
            output file from DetectNodes in string format
        """

        self.logger.info(f'Running run_detect_nodes() for timestep {timestep}')

        tempest_filein = self.tempest_filein
        tempest_dictionary = self.tempest_dictionary
        tempest_fileout = os.path.join(
            self.paths['tmpdir'], 'tempest_output_' + timestep + '.txt')
        self.tempest_fileout = tempest_fileout

        detect_string = f'DetectNodes --in_data {tempest_filein} --timefilter 6hr --out {tempest_fileout} --searchbymin {tempest_dictionary["psl"]} ' \
            f'--closedcontourcmd {tempest_dictionary["psl"]},200.0,5.5,0;_DIFF({tempest_dictionary["zg"]}(30000Pa),{tempest_dictionary["zg"]}(50000Pa)),-58.8,6.5,1.0 --mergedist 6.0 ' \
            f'--outputcmd {tempest_dictionary["psl"]},min,0;_VECMAG({tempest_dictionary["uas"]},{tempest_dictionary["vas"]}),max,2;{tempest_dictionary["orog"]},min,0" --latname {tempest_dictionary["lat"]} --lonname {tempest_dictionary["lon"]}'

        subprocess.run(detect_string.split(), stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)

    def store_detect_nodes(self, timestep, write_fullres=True):
        """
        Store the output of DetectNodes in a netcdf file.

        Args:
            timestep: Timestep for which to store the data.
            write_fullres: Boolean to write the full resolution data to disk.

        Returns:
            None
        """

        self.logger.info(
            f'Running store_detect_nodes() for timestep {timestep}')

        # in case you want to write netcdf file with ullres field after Detect Nodes
        if write_fullres:
            # loop on variables to write to disk only the subset of high res files
            subselect = self.fullres.sel(time=timestep)
            data = self.reader_fullres.regrid(subselect)
            self.logger.info(f'store_fullres_field for timestep {timestep}')
            xfield = self.store_fullres_field(data, self.tempest_nodes)
            store_file = os.path.join(
                self.paths['fulldir'], f'TC_fullres_{timestep}.nc')
            write_fullres_field(xfield, store_file, self.aquadask.dask)

            # for var in self.var2store:

            #     subselect = self.fullres[var].sel(time=timestep)
            #     data = self.reader_fullres.regrid(subselect)
            #     xfield = self.store_fullres_field(0, data, self.tempest_nodes)
            #     self.logger.info(f'store_fullres_field for timestep {timestep}')
            #     store_file = os.path.join(self.paths['fulldir'], f'TC_{var}_{timestep}.nc')
            #     write_fullres_field(xfield, store_file)
