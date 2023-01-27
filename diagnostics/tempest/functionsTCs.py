import xarray as xr
import os
import subprocess
from time import time
from glob import glob

def readwrite_from_lowres(filein, fileout) : 

    """
    Read and write low resolution data to mimic access from FDB

    Args: 
        filein: input file at low resolution
        fileout: input file at low resolution (netcdf)

    Returns: 
        outdict: dictionary with variable and dimensiona names for fileout
    """

    xfield = xr.open_mfdataset(filein)

    # check if output file exists
    if os.path.exists(fileout):
        os.remove(fileout)

    xfield.to_netcdf(fileout)
    xfield.close()
    
    outdict = {'lon': 'lon', 'lat': 'lat', 
            'psl': 'MSL', 'zg': 'Z',
            'uas': 'U10M', 'vas': 'V10M'}

    return outdict

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

def run_stitch_nodes(infiles_list, trackfile):

    """"
    Basic function to call from command line tempest extremes StitchNodes

    Args:
        infiles_list: .txt file (output from DetectNodes) with all TCs centres dates&coordinates
        tempest_fileout: output file (.txt) from StitchNodes command

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

    
    stitch_string = f'StitchNodes --in {full_nodes} --out {trackfile} --in_fmt lon,lat,slp,wind --range 8.0 --mintime 54h ' \
        f'--maxgap 24h --threshold wind,>=,10.0,10;lat,<=,50.0,10;lat,>=,-50.0,10'
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
    compression = {'MSL': {'zlib': True}}
    gfield.where(gfield!=0).to_netcdf(filestore, encoding=compression)
    gfield.close()

def reorder_tracks(track_file):

    """
    Open the total track files, reorder tracks in time then creates a dict with time and lons lats pair of every track

    Args:
        track_file: input track file from tempest StitchNodes
    
    Returns:
        reordered_tracks: python dictionary with date lon lat of TCs centres after StitchNodes has also run
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
    for tstep in sorted(set(tracks['date'])) : 
        #idx = tracks['date'].index(tstep)
        idx = [i for i, e in enumerate(tracks['date']) if e == tstep]
        reordered_tracks[tstep] = {}
        reordered_tracks[tstep]['lon'] = [tracks['slon'][k] for k in idx]
        reordered_tracks[tstep]['lat'] = [tracks['slat'][k] for k in idx]
        
    return reordered_tracks


def clean_files(filelist):

    for fileout in filelist :
        if os.path.exists(fileout):
            os.remove(fileout)

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

def plotting():
    # tempest settings
    nVars=-1
    headerStr='start'
    isUnstruc = 0

    # Extract trajectories from tempest file and assign to arrays
    # USER_MODIFY
    nstorms, ntimes, traj_data = getTrajectories(trajfile,nVars,headerStr,isUnstruc)
    xlon   = traj_data[2,:,:]
    xlat   = traj_data[3,:,:]
    xpres  = traj_data[4,:,:]/100.
    xwind  = traj_data[5,:,:]
    xyear  = traj_data[7,:,:]
    xmonth = traj_data[8,:,:]

    # Initialize axes
    ax = plt.axes(projection=ccrs.PlateCarree())
    # ax.set_extent([-180, 180, -75, 75], crs=None)

    # Set title and subtitle
    plt.title('Example of a Trajectory Plot')


    # Set land feature and change color to 'lightgrey'
    # See link for extensive list of colors:
    # https://matplotlib.org/3.1.0/gallery/color/named_colors.html
    ax.add_feature(cfeature.LAND, color='lightgrey')
    gl = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=True,
                  linewidth=0.5, color='k', alpha=0.5, linestyle='--')
    gl.xlabels_top = False
    gl.ylabels_left = False
    #gl.xlines = False
    gl.xlocator = mticker.FixedLocator([-180, -90, 0, 90, 180])
    gl.xformatter = LONGITUDE_FORMATTER
    gl.yformatter = LATITUDE_FORMATTER
    gl.ylabel_style = {'size': 12, 'color': 'black'}
    gl.xlabel_style = {'size': 12, 'color': 'black'}



    # Plot each trajectory
    for i in range(nstorms):

            # doesn't work with cartopy!
            #plt.plot(xlon[i], xlat[i], linewidth=0.4)



        plt.scatter(x=xlon[i], y=xlat[i],
                                                    color="black",
                                                    s=30,
                                                    linewidths=0.5,
                                                    marker=".",
                                                    alpha=0.8,
                                                    transform=ccrs.PlateCarree()) ## Important


    plt.savefig(plotdir + "prova_TC_2010.png", bbox_inches='tight', dpi=350)