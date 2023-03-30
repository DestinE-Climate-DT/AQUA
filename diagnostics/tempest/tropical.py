import os
import xarray as xr
import subprocess
import logging
from aqua import Reader
from aqua.util import log_configure
from glob import glob
import pandas as pd
from datetime import datetime


class TCs():

    def __init__(self, tdict = None, 
                 paths = None, model="IFS", exp="tco2559-ng5", 
                 boxdim = 10, lowgrid='r100', highgrid='r100', var2store=None, 
                 loglevel = 'WARNING'):
        
        loglevel = log_configure(loglevel)

        if tdict is not None:
            self.paths = tdict['paths']
            self.model = tdict['dataset']['model']
            self.exp = tdict['dataset']['exp']
            self.boxdim = tdict['detect']['boxdim']
            self.lowgrid =  tdict['grids']['lowgrid']
            self.highgrid = tdict['grids']['highgrid']
            self.var2store = tdict['varlist']

        else:

            if paths is None:
                raise Exception('Without paths defined you cannot go anywhere!')
            else:
                self.paths = paths
            self.model = model 
            self.exp = exp
            self.boxdim = boxdim
            self.lowgrid = lowgrid
            self.highgrid = highgrid
            self.var2store = var2store

        for path in self.paths:
            os.makedirs(self.paths[path], exist_ok=True)

    def detect_nodes_zoomin(self, start_date, end_date, frequency):

        self.catalog_init()
        for tstep in pd.date_range(start=start_date, end=end_date, freq=frequency).strftime('%Y%m%dT%H'):
            logging.warning(tstep)
            self.readwrite_from_intake(tstep)
            self.run_detect_nodes(tstep)
            clean_files([self.tempest_filein])
            self.read_lonlat_nodes()
            self.store_detect_nodes(tstep)

    def stitch_nodes_zoomin(self, start_date, end_date, n_days_freq, n_days_ext):

        self.set_time_window(n_days_freq=n_days_freq, n_days_ext=n_days_ext)

        for block in pd.date_range(start=start_date, end=end_date, freq=str(n_days_freq)+'D'):
            logging.warning(block)
            dates_freq, dates_ext = self.time_window(block)
            self.prepare_stitch_nodes(block, dates_freq, dates_ext)
            self.run_stitch_nodes(maxgap='6h')
            self.reorder_tracks()
            self.store_stitch_nodes(block, dates_freq)


    def catalog_init(self, streaming=False):

        if self.model in 'IFS':
            self.varlist2d = ['msl', '10u', '10v']
            self.reader2d = Reader(model=self.model, exp=self.exp, source="ICMGG_atm2d", 
                                   regrid=self.lowgrid, vars = self.varlist2d)
            self.varlist3d = ['z']
            self.reader3d = Reader(model=self.model, exp=self.exp, source="ICMU_atm3d", 
                                   regrid=self.lowgrid, vars = self.varlist3d)
            self.reader_fullres = Reader(model=self.model, exp=self.exp, source="ICMGG_atm2d", 
                                         regrid=self.highgrid, var = self.var2store)
        else:
            raise Exception(f'Model {self.model} not supported')
        

    def set_time_window(self, n_days_freq = 30, n_days_ext = 10):

        self.n_days_freq = n_days_freq
        self.n_days_ext = n_days_ext
        
    def readwrite_from_intake(self, timestep):

        logging.info(f'Running readwrite_from_intake() for {timestep}')

        outfield = 0
        data2d = self.reader2d.retrieve()
        fileout = os.path.join(self.paths['regdir'], f'regrid_{timestep}.nc')

        for var in self.varlist2d:
            logging.info('Accessing 2D data..')
            lowres = self.reader2d.regrid(data2d[var].sel(time=timestep))
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
        
        data3d = self.reader3d.retrieve()
        for var in self.varlist3d:
            logging.info('Accessing 3D data..')
            lowres = self.reader3d.regrid(data3d[var].sel(time=timestep, level=[300,500]))
            outfield = xr.merge([outfield, lowres.to_dataset(name=var)])
            
        # check if output file exists
        if os.path.exists(fileout):
            os.remove(fileout)

        #level_var = outfield['level']
        outfield['level'] = outfield['level'].astype(float)
        outfield['level'].attrs['units'] = 'hPa'
        logging.info('Writing low res to disk..')
        outfield.to_netcdf(fileout)
        outfield.close()
        
        self.tempest_dictionary = {'lon': 'lon', 'lat': 'lat', 
                    'psl': 'msl', 'zg': 'z',
                    'uas': 'u10m', 'vas': 'v10m'}
        self.tempest_filein=fileout

    def run_detect_nodes(self, timestep) : 

        logging.info(f'Running run_detect_nodes() for {timestep}')

        """"
        Basic function to call from command line tempest extremes DetectNodes
        Args:
            tempest_dictionary: python dictionary with variable names for tempest commands
            tempest_filein: file (netcdf) with low res data
            tempest_fileout: output file (.txt) from DetectNodes command
        Returns: 
        detect_string: output file from DetectNodes in string format 
        """
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

        logging.info(f'Running store_detect_nodes() for {timestep}')
        fulldata = self.reader_fullres.retrieve().sel(time=timestep)
        
        # in case you want to write netcdf file with ullres field after Detect Nodes
        if write_fullres:
          # loop on variables to write to disk only the subset of high res files
          for var in self.var2store : 

                if var in ['tp']:     
                    varfile = 'tprate'
                else:
                    varfile = var

                data = self.reader_fullres.regrid(fulldata[varfile])
                data.name = var
                xfield = self.store_fullres_field(0, data, self.tempest_nodes)

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

        # create list of file paths to include in glob pattern
        file_paths = [os.path.join(self.paths['tmpdir'], f"tempest_output_{date}T??.txt") for date in dates_ext.strftime('%Y%m%d')]
        # use glob to get list of filenames that match the pattern
        filenames = []
        for file_path in file_paths:
            filenames.extend(sorted(glob(file_path)))

        self.tempest_filenames = filenames
        self.track_file = os.path.join(self.paths['tmpdir'], f'tempest_track_{block.strftime("%Y%m%d")}-{dates_freq[-1].strftime("%Y%m%d")}.txt')


    def time_window(self, initial_date):

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

        if write_fullres:
            for var in self.var2store : 
                print(var)
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

                print('Storing output')

                # store the file
                store_file = os.path.join(self.paths['fulldir'], f'tempest_tracks_{var}_{block.strftime("%Y%m%d")}-{dates_freq[-1].strftime("%Y%m%d")}.nc')
                write_fullres_field(xfield, store_file)



def clean_files(filelist):

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
                 'dtype': 'float64',
                 'zlib': True}

    gfield.where(gfield!=0).to_netcdf(filestore,  encoding={'time': time_encoding})
    gfield.close()


