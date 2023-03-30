import sys
sys.path.append('/home/b/b382216/work/AQUA')
from aqua import Reader
import xarray as xr
import os
from glob import glob
import subprocess
import pandas as pd
from datetime import datetime

def detect_nodes_zoomin(retrieve_dictionary, dirs, varlist, original_dictionary, lowgrid, highgrid, boxdim, write_fullres=False):

    # loop on timerecords
    for tstep in pd.date_range(start=f'{retrieve_dictionary["init_year"]}-{retrieve_dictionary["init_month"]}-{retrieve_dictionary["init_day"]}',  
                        end=f'{retrieve_dictionary["end_year"]}-{retrieve_dictionary["end_month"]}-{retrieve_dictionary["end_day"]}', freq=retrieve_dictionary["frequency"]).strftime('%Y%m%dT%H'):

        print(tstep)
        # read from catalog, interpolate, write to disk and create a dictionary with useful information to run tempest commands
        tempest_dictionary = readwrite_from_intake(model='IFS', exp = 'tco2559-ng5', timestep=tstep, grid=lowgrid, tgtdir=dirs['regdir'])

        # define the tempest detect nodes output
        txt_file = os.path.join(dirs['tmpdir'], 'tempest_output_' + tstep + '.txt')

        # run the node detection on the low res files
        tempest_command = run_detect_nodes(tempest_dictionary, tempest_dictionary['regrid_file'], txt_file)

        # remove the low res files
        clean_files([tempest_dictionary['regrid_file']])
        
        # identify the nodes
        tempest_nodes = read_lonlat_nodes(txt_file)

        # load the highres files
        #reader2d = Reader(model='IFS', exp = 'tco2559-ng5', source="ICMGG_atm2d")
        reader2d = Reader(model='IFS', exp = 'tco2559-ng5', source="ICMGG_atm2d", regrid=highgrid, var = varlist)
        fulldata = reader2d.retrieve().sel(time=tstep)
        
        # in case you want to write netcdf file with ullres field after Detect Nodes
        if write_fullres:
          # loop on variables to write to disk only the subset of high res files
          for var in varlist : 

              varfile = original_dictionary[var]

              data = reader2d.regrid(fulldata[varfile])
              data.name = var
              xfield = store_fullres_field(0, data, tempest_nodes, boxdim=boxdim)

              store_file = os.path.join(dirs['tmpdir'], f'TC_{var}_{tstep}.nc')
              write_fullres_field(xfield, store_file)

def stitch_nodes_zoomin(retrieve_dictionary, dirs, varlist, boxdim, n_days_ext, n_days_freq, write_fullres):

    # loop on each time stamp in dates
    for block in pd.date_range(start=f'{retrieve_dictionary["init_year"]}-{retrieve_dictionary["init_month"]}-{retrieve_dictionary["init_day"]}', 
                               end=f'{retrieve_dictionary["end_year"]}-{retrieve_dictionary["end_month"]}', freq=str(n_days_freq)+'D'):

        # create DatetimeIndex with daily frequency
        dates = pd.date_range(start=block, periods=n_days_freq, freq='D')

        before = dates.shift(-n_days_ext, freq='D')[0:n_days_ext]
        after = dates.shift(+n_days_ext, freq='D')[-n_days_ext:]

        # concatenate the indexes to create a single index
        date_index = before.append(dates).append(after)

        # create list of file paths to include in glob pattern
        file_paths = [os.path.join(dirs['tmpdir'], f"tempest_output_{date}T??.txt") for date in date_index.strftime('%Y%m%d')]
        # use glob to get list of filenames that match the pattern
        filenames = []
        for file_path in file_paths:
            filenames.extend(sorted(glob(file_path)))
        #print(filenames)

        track_file = os.path.join(dirs['tmpdir'], f'tempest_track_{block.strftime("%Y%m%d")}-{dates[-1].strftime("%Y%m%d")}.txt')

        # run stitch nodes, MAXGAP set to 6h to match the input files res
        stitch_string = run_stitch_nodes(filenames, track_file, maxgap = '6h')

        # create DatetimeIndex with daily frequency
        dates = pd.date_range(start=block, periods=n_days_freq, freq='D')
        
        # create output file with output from stitch nodes 
        track_file = os.path.join(dirs['tmpdir'], f'tempest_track_{block.strftime("%Y%m%d")}-{dates[-1].strftime("%Y%m%d")}.txt')

        # reordered_tracks is a dict containing the concatenated (in time) tracks
        # at eatch time step are associated all lons/lats

        reordered_tracks = reorder_tracks(track_file)

        if write_fullres:
          for var in varlist : 
              print(var)
              # initialise full_res fields at 0 before the loop
              xfield = 0
              for idx in reordered_tracks.keys():
                  #print(datetime.strptime(idx, '%Y%m%d%H').strftime('%Y%m%d'))
                  #print (dates.strftime('%Y%m%d'))
                  if datetime.strptime(idx, '%Y%m%d%H').strftime('%Y%m%d') in dates.strftime('%Y%m%d'):

                      timestep = datetime.strptime(idx, '%Y%m%d%H').strftime('%Y%m%dT%H')
                      print (timestep)
                      fullres_file = os.path.join(dirs['tmpdir'], f'TC_{var}_{timestep}.nc')
                      fullres_field = xr.open_mfdataset(fullres_file)[var]

                      # get the full res field and store the required values around the Nodes
                      xfield = store_fullres_field(xfield, fullres_field, reordered_tracks[idx], boxdim)

              print('Storing output')

              # store the file
              store_file = os.path.join(dirs['tmpdir'], f'tempest_tracks_{var}_{block.strftime("%Y%m%d")}-{dates[-1].strftime("%Y%m%d")}.nc')
              write_fullres_field(xfield, store_file)

def readwrite_from_intake(model, exp, timestep, grid, tgtdir): 

  """
  Given a climate model, an experiment, a timestamp, a grid, and a target directory,
  this function reads data from the intake catalog, stores them in a netCDF file, 
  and returns a dictionary of processed data that can be analyzed with TempestExtreme.

  Args:
    	model: A string representing the climate model, e.g. "IFS".
      exp: A string representing the experiment to retrieve data from.
      timestep: A pandas Timestamp object representing the time of interest.
      grid: A string representing the grid to use for regridding.
      tgtdir: A string representing the target directory where the output file will be saved.

  return: 
      Outdict: A dictionary containing the processed data with keys "lon", "lat", "psl", "zg", "uas", "vas", and "regrid_file".
  """

  
  if model in 'IFS':
    varlist2d = ['msl', '10u', '10v']
    reader2d = Reader(model=model, exp=exp, source="ICMGG_atm2d", regrid=grid, vars = varlist2d)
    varlist3d = ['z']
    reader3d = Reader(model=model, exp=exp, source="ICMU_atm3d", regrid=grid, vars = varlist3d)
    

  outfield = 0
  data2d = reader2d.retrieve()
  fileout=os.path.join(tgtdir, f'regrid_{timestep}.nc')

  for var in varlist2d:
    lowres = reader2d.regrid(data2d[var].sel(time=timestep))
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
 
  data3d = reader3d.retrieve()
  for var in varlist3d:
    lowres = reader3d.regrid(data3d[var].sel(time=timestep, level=[300,500]))
    outfield = xr.merge([outfield, lowres.to_dataset(name=var)])
     
  # check if output file exists
  if os.path.exists(fileout):
    os.remove(fileout)

  #level_var = outfield['level']
  outfield['level'] = outfield['level'].astype(float)
  outfield['level'].attrs['units'] = 'hPa'
  outfield.to_netcdf(fileout)
  outfield.close()
  
  outdict = {'lon': 'lon', 'lat': 'lat', 
            'psl': 'msl', 'zg': 'z',
            'uas': 'u10m', 'vas': 'v10m',
            'regrid_file': fileout}
  
  return outdict


# def readwrite_from_lowres(filein, fileout) : 

#     """
#     Read and write low resolution data to mimic access from FDB

#     Args: 
#         filein: input file at low resolution
#         fileout: input file at low resolution (netcdf)

#     Returns: 
#         outdict: dictionary with variable and dimensiona names for fileout
#     """

#     xfield = xr.open_mfdataset(filein)

#     # check if output file exists
#     if os.path.exists(fileout):
#         os.remove(fileout)

#     xfield.to_netcdf(fileout)
#     xfield.close()
    
#     outdict = {'lon': 'lon', 'lat': 'lat', 
#             'psl': 'MSL', 'zg': 'Z',
#             'uas': 'U10M', 'vas': 'V10M'}

#     return outdict

def run_detect_nodes(tempest_dictionary, tempest_filein, tempest_fileout) : 

    """"
    Basic function to call from command line tempest extremes DetectNodes
    Args:
        tempest_dictionary: python dictionary with variable names for tempest commands
        tempest_filein: file (netcdf) with low res data
        tempest_fileout: output file (.txt) from DetectNodes command
    Returns: 
       detect_string: output file from DetectNodes in string format 
    """
    
    detect_string= f'DetectNodes --in_data {tempest_filein} --timefilter 6hr --out {tempest_fileout} --searchbymin {tempest_dictionary["psl"]} ' \
    f'--closedcontourcmd {tempest_dictionary["psl"]},200.0,5.5,0;_DIFF({tempest_dictionary["zg"]}(30000Pa),{tempest_dictionary["zg"]}(50000Pa)),-58.8,6.5,1.0 --mergedist 6.0 ' \
    f'--outputcmd {tempest_dictionary["psl"]},min,0;_VECMAG({tempest_dictionary["uas"]},{tempest_dictionary["vas"]}),max,2 --latname {tempest_dictionary["lat"]} --lonname {tempest_dictionary["lon"]}'

    subprocess.run(detect_string.split(), stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)

    return detect_string

def run_stitch_nodes(infiles_list, trackfile, maxgap = '24h', mintime = '54h'):

    """"
    Basic function to call from command line tempest extremes StitchNodes

    Args:
        infiles_list: .txt file (output from DetectNodes) with all TCs centres dates&coordinates
        tempest_fileout: output file (.txt) from StitchNodes command
        dir: directory where to store the temporary file with all concatenated detect nodes

    Returns: 
       stitch_string: output file from StitchNodes in string format 
    """

    full_nodes = os.path.join('full_nodes.txt')
    if os.path.exists(full_nodes):
            os.remove(full_nodes)

    with open(full_nodes, 'w') as outfile:
        for fname in sorted(infiles_list):
            with open(fname) as infile:
                outfile.write(infile.read())

    stitch_string = f'StitchNodes --in {full_nodes} --out {trackfile} --in_fmt lon,lat,slp,wind --range 8.0 --mintime {mintime} ' \
        f'--maxgap {maxgap} --threshold wind,>=,10.0,10;lat,<=,50.0,10;lat,>=,-50.0,10'
    
    subprocess.run(stitch_string.split())

    return stitch_string

def read_lonlat_nodes(tempest_fileout):

    """
    Read from txt files output of DetectNodes the position of the centers of the TCs

    Args:

        tempest_fileout: output file from tempest DetectNodes

    Returns: 
       out: dictionary with 'date', 'lon' and 'lat' of the TCs centers
    """

    with open(tempest_fileout) as f:
        lines = f.readlines()
    first = lines[0].split('\t')
    date = first[0] + first[1].zfill(2) + first[2].zfill(2) + first[4].rstrip().zfill(2)
    lon_lat = [line.split('\t')[3:] for line in lines[1:]]
    out = {'date': date, 'lon': [val[0] for val in lon_lat], 'lat': [val[1] for val in lon_lat]}

    return out


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

def store_fullres_field(mfield, xfield, nodes, boxdim): 

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
        box = lonlatbox(nodes['lon'][k], nodes['lat'][k], boxdim)
        mask = mask + xr.where((xfield.lon > box[0]) & (xfield.lon < box[1]) & (xfield.lat > box[2]) & (xfield.lat < box[3]), True, False)

    outfield = xfield.where(mask>0)

    if isinstance(mfield, xr.DataArray):
        outfield = xr.concat([mfield, outfield], dim = 'time')
    
    return outfield
  

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

def reorder_tracks(track_file):

    """
    Open the total track files, reorder tracks in time then creates a dict with time and lons lats pair of every track

    Args:
        track_file: input track file from tempest StitchNodes
    
    Returns:
        reordered_tracks: python dictionary with date lon lat of TCs centres after StitchNodes has been run
    """

    with open(track_file) as file:
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
        
    return reordered_tracks


def clean_files(filelist):

    for fileout in filelist :
        if os.path.exists(fileout):
            os.remove(fileout)


### FROM HERE FUNCTION FOR ANALYSIS
# from https://github.com/zarzycki/cymep

import numpy as np
import re

def getTrajectories(filename,nVars,headerDelimStr,isUnstruc):
  print("Getting trajectories from TempestExtremes file...")
  print("Running getTrajectories on '%s' with unstruc set to '%s'" % (filename, isUnstruc))
  print("nVars set to %d and headerDelimStr set to '%s'" % (nVars, headerDelimStr))

  # Using the newer with construct to close the file automatically.
  with open(filename) as f:
      data = f.readlines()

  # Find total number of trajectories and maximum length of trajectories
  numtraj=0
  numPts=[]
  for line in data:
    if headerDelimStr in line:
      # if header line, store number of points in given traj in numPts
      headArr = line.split()
      numtraj += 1
      numPts.append(int(headArr[1]))
    else:
      # if not a header line, and nVars = -1, find number of columns in data point
      if nVars < 0:
        nVars=len(line.split())
  
  maxNumPts = max(numPts) # Maximum length of ANY trajectory

  print("Found %d columns" % nVars)
  print("Found %d trajectories" % numtraj)

  # Initialize storm and line counter
  stormID=-1
  lineOfTraj=-1

  # Create array for data
  if isUnstruc:
    prodata = np.empty((nVars+1,numtraj,maxNumPts))
  else:
    prodata = np.empty((nVars,numtraj,maxNumPts))

  prodata[:] = np.NAN

  for i, line in enumerate(data):
    if headerDelimStr in line:  # check if header string is satisfied
      stormID += 1      # increment storm
      lineOfTraj = 0    # reset trajectory line to zero
    else:
      ptArr = line.split()
      for jj in range(nVars):
        if isUnstruc:
          prodata[jj+1,stormID,lineOfTraj]=ptArr[jj]
        else:
          prodata[jj,stormID,lineOfTraj]=ptArr[jj]
      lineOfTraj += 1   # increment line

  print("... done reading data")
  return numtraj, maxNumPts, prodata


def getNodes(filename,nVars,isUnstruc):
  print("Getting nodes from TempestExtremes file...")

  # Using the newer with construct to close the file automatically.
  with open(filename) as f:
      data = f.readlines()

  # Find total number of trajectories and maximum length of trajectories
  numnodetimes=0
  numPts=[]
  for line in data:
    if re.match(r'\w', line):
      # if header line, store number of points in given traj in numPts
      headArr = line.split()
      numnodetimes += 1
      numPts.append(int(headArr[3]))
    else:
      # if not a header line, and nVars = -1, find number of columns in data point
      if nVars < 0:
        nVars=len(line.split())

    maxNumPts = max(numPts) # Maximum length of ANY trajectory

  print("Found %d columns" % nVars)
  print("Found %d trajectories" % numnodetimes)
  print("Found %d maxNumPts" % maxNumPts)

  # Initialize storm and line counter
  stormID=-1
  lineOfTraj=-1

  # Create array for data
  if isUnstruc:
    prodata = np.empty((nVars+5,numnodetimes,maxNumPts))
  else:
    prodata = np.empty((nVars+4,numnodetimes,maxNumPts))

  prodata[:] = np.NAN

  nextHeadLine=0
  for i, line in enumerate(data):
    if re.match(r'\w', line):  # check if header string is satisfied
      stormID += 1      # increment storm
      lineOfTraj = 0    # reset trajectory line to zero
      headArr = line.split()
      YYYY = int(headArr[0])
      MM = int(headArr[1])
      DD = int(headArr[2])
      HH = int(headArr[4])
    else:
      ptArr = line.split()
      for jj in range(nVars-1):
        if isUnstruc:
          prodata[jj+1,stormID,lineOfTraj]=ptArr[jj]
        else:
          prodata[jj,stormID,lineOfTraj]=ptArr[jj]
      if isUnstruc:
        prodata[nVars+1,stormID,lineOfTraj]=YYYY
        prodata[nVars+2,stormID,lineOfTraj]=MM
        prodata[nVars+3,stormID,lineOfTraj]=DD
        prodata[nVars+4,stormID,lineOfTraj]=HH
      else:
        prodata[nVars  ,stormID,lineOfTraj]=YYYY
        prodata[nVars+1,stormID,lineOfTraj]=MM
        prodata[nVars+2,stormID,lineOfTraj]=DD
        prodata[nVars+3,stormID,lineOfTraj]=HH
      lineOfTraj += 1   # increment line

  print("... done reading data")
  return numnodetimes, maxNumPts, prodata