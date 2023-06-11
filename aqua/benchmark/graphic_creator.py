#1. install ffmpeg
import numpy as np
import xarray

import pandas as pd

import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib.colors import LogNorm

import cartopy
import cartopy.feature as cfeature
import cartopy.crs as ccrs

from .time_functions import time_into_plot_title, time_interpreter, month_convert_num_to_str, hour_convert_num_to_str, time_units_converter 

"""The module contains functions to create animations and images:
     - animation_creator,
     - image_creator,
     - lon_lat_regrider
     
.. moduleauthor:: AQUA team <natalia.nazarova@polito.it>

"""

def data_size(data):
    """Returning the size of the Dataset or DataArray

    Args:
        data (xarray): Dataset or dataArray, the size of which we would like to return
    Returns:
        int: size of data
    """   
    if 'DataArray' in str(type(data)):
            _size = data.size
    elif 'Dataset' in str(type(data)): 
        names = list(data.dims) #_coord_names)
        size = 1
        for i in names:
            size *= data[i].size
    return size

""" """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """
def animation_creator(ds, vmin = None, vmax = None, trop_lat = 10,  time_ind_max = None,  nSeconds = 10, save=False,  contour  = True, 
                      label = 'test', title = 'Tropical precipitation', resol = '110m'):
    """Creating the animation of the dataset

    Args:
        ds (xarray):                    The Dataset.
        vmin (float, optional):         The minimal data value of the colormap. Defaults to None.
        vmax (float, optional):         The maximal data value of the colormap. Defaults to None.
        trop_lat (int/float, optional): Tropical latitudes borders. Defaults to 10.
        time_ind_max (int, optional):   The maximal time index of Dataset for animation. Defaults to None.
        nSeconds (int, optional):       The duration of the animation in seconds. Defaults to 10.
        contour (bool, optional):       The contour of continents. Defaults to True.
        label (str, optional):          The name of created animation in the filesystem. Defaults to 'test'.
        title (str, optional):          The title of the animation. Defaults to 'Tropical precipitation'.
        resol (str, optional):          The resolution of contour of continents. Defaults to '110m'.

    Returns:
        mp4: amination
    """    
    if vmin != None:
        ds = ds.where( ds > vmin, drop=True) 

    if time_ind_max != None:
        fps = int(time_ind_max/nSeconds)
    else:
        fps = int(ds.time.size/nSeconds)

    snapshots = [ds[number,:,:] for number in range(0, fps*nSeconds )]

    # First set up the figure, the axis, and the plot element we want to animate
    fig = plt.figure( figsize=(20,8) )

    ax = plt.axes(projection=ccrs.PlateCarree())

    if  contour:
        ax.coastlines(resolution=resol)
    # branch, animation for different variables 
    
    ax.gridlines()

    a = snapshots[0]
    
    #ax.axhspan(-trop_lat, trop_lat, facecolor='grey', alpha=0.3)
    im = plt.imshow(a, interpolation='none',  aspect='auto', vmin=vmin, vmax=vmax, #alpha = 1., 
                    extent = (-180, 180, - trop_lat, trop_lat), origin = 'upper')


    fig.colorbar(im).set_label(ds.units, fontsize = 14)
    
    def animate_func(i):
        if i % fps == 0:
            print( '.', end ='' )
        im.set_array(snapshots[i])
        time = time_into_plot_title(ds, i)
        plt.title(title+',     '+time, fontsize = 16)
        return [im]

    

    dpi = 100
    fig.set_size_inches(12, 8, True)
    writer = animation.writers['ffmpeg'](fps=30, extra_args=['-vcodec', 'libx264'])
    
    anim = animation.FuncAnimation(fig, 
                            animate_func, 
                            frames = nSeconds * fps,
                            interval = 1000 / fps, # in ms
                            )

    #plt.title(title,     fontsize =18)
    plt.xlabel('longitude',                 fontsize =18)
    plt.ylabel('latitude',                  fontsize =18)
    plt.xticks([-180, -120, -60, 0, 60, 120, 180],  fontsize=14)   #, 240, 300, 360
    plt.yticks([-90, -60, -30, 0, 30, 60, 90],      fontsize=14) 
   
    if save:
        anim.save('anim.mp4',  dpi=dpi, fps=30) #  writer=writer,, savefig_kwargs={'bbox_inches' : 'tight'}) #./notebooks/figures/animation
        plt.show()
    else:
        plt.show()
    print('Done!')
    return  True

""" """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """
def image_creator(ds, vmin = None, vmax = None, trop_lat = 10, log=False, figsize =1, contour  = True,   label = 'test',  
                  title = 'Tropical precipitation', resol = '110m'):
    """Creating the image of the Dataset.

    Args:
        ds (xarray):                    The Dataset.
        vmin (float, optional):         The minimal data value of the colormap. Defaults to None.
        vmax (float, optional):         The maximal data value of the colormap. Defaults to None.
        trop_lat (int/float, optional): Tropical latitudes borders. Defaults to 10.
        figsize (int, optional):        The scale factor of the size of the created image. Defaults to 1.
        contour (bool, optional):       The contour of continents. Defaults to True.
        label (str, optional):          The name of created animation in the filesystem. Defaults to 'test'.
        title (str, optional):          The title of the animation. Defaults to 'Tropical precipitation'.
        resol (str, optional):          The resolution of contour of continents. Defaults to '110m'.
    """    
    if vmin != None:
        ds = ds.where( ds > vmin, drop=True) 

    if ds.time.size!=1:
        snapshot = ds[0,:,:] 
    else:
        snapshot = ds
    # First set up the figure, the axis, and the plot element we want to animate
    fig = plt.figure( figsize=(8*figsize,5*figsize) )
    
    ax = plt.axes(projection=ccrs.PlateCarree())
    
    if  contour:
        ax.coastlines(resolution=resol)
    
    ax.gridlines() 

    if log:
        im = plt.imshow(snapshot, interpolation='nearest',  aspect='auto',  norm=LogNorm(vmin=vmin, vmax=vmax), alpha=0.9,
                        extent = (-180, 180, - trop_lat, trop_lat), origin = 'upper')
    else:
        im = plt.imshow(snapshot, interpolation='nearest',  aspect='auto',  vmin=vmin, vmax=vmax, alpha=0.9,
                        extent = (-180, 180, - trop_lat, trop_lat), origin = 'upper')

    fig.colorbar(im)

    plt.title( title,     fontsize =18)
    plt.xlabel('longitude',                 fontsize =18)
    plt.ylabel('latitude',                  fontsize =18)
    plt.xticks([-180, -120, -60, 0, 60, 120, 180],  fontsize=14)   #, 240, 300, 360
    plt.yticks([-90, -60, -30, 0, 30, 60, 90],      fontsize=14) 
    plt.savefig('../notebooks/figures/'+str(label)+'.png')
    print('Done!')


#def interpolate(inp, fi):
#    i, f = int(fi // 1), fi % 1  # Split floating-point index into whole & fractional parts.
#    j = i+1 if f > 0 else i  # Avoid index error.
#    inp2 = inp + [inp[-1]]
#    return inp2[i] + f*(inp2[i+1]-inp2[i])


def lon_lat_regrider(data, space_grid_factor = None,  new_length=None, coord_name = 'lat'):
    """The space regrider of the Dataset

    Args:
        data (xarray):                      The Dataset to be space regrided.
        space_grid_factor (int, optional):  The resolution of the new space grid. If the input value is negative, the space grid will be less dense 
                                            in space_grid_factor time. If the input value is positive, the space grid will be dense 
                                            in space_grid_factor times. Defaults to None.
        coord_name (str, optional):         The name of space coordinate. Defaults to 'lat'.

    Returns:
        xarray: The space regrided Dataset.
    """    
    # work only for lat and lon only for now. Check the line with interpolation command and modify it in the future
    if isinstance(space_grid_factor, int):
        if space_grid_factor>1:
            del_c = float((float(data[coord_name][1])- float(data[coord_name][0]))/2)
            ds = []
            new_dataset = data.copy(deep=True)
            new_dataset[coord_name] = data[coord_name][:] 
            for i in range(1, space_grid_factor):
                #new_dataset = new_dataset.interp(lat=new_dataset[coord_name][:]+del_c, method="linear", kwargs={"fill_value": "extrapolate"})
                if coord_name == 'lat':  
                    new_dataset = new_dataset.interp(lat=new_dataset['lat'][:]+del_c, method="linear", kwargs={"fill_value": "extrapolate"})
                elif coord_name == 'lon':
                    new_dataset = new_dataset.interp(lon=new_dataset['lon'][:]+del_c, method="linear", kwargs={"fill_value": "extrapolate"}) 
                ds.append(new_dataset)
                del_c = del_c/2
            combined = xarray.concat(ds, dim=coord_name)
            combined = combined.sortby(combined[coord_name])
            return combined
        elif space_grid_factor<1:
            space_grid_factor = abs(space_grid_factor)
            if coord_name == 'lat': 
                new_dataset = data.isel(lat=[i for i in range(0, data.lat.size, space_grid_factor)])
            elif coord_name == 'lon':
                new_dataset = data.isel(lon=[i for i in range(0, data.lon.size, space_grid_factor)])
            return new_dataset
    elif new_length is not None:
        #old_lenght =  data[coord_name].size 
        #new_dataset[coord_name] = data[coord_name][:] 
        if  data[coord_name][0]>0 and data[coord_name][-1]<0:
            old_lenght =  data[coord_name][0].values-data[coord_name][-1].values    
            delta = (old_lenght-1) / (new_length-1)
            new_dataset = data.copy(deep=True)
            new_coord=[data[coord_name][0].values - i*delta for i in range(0, new_length)]
        else:
            old_lenght =  data[coord_name][-1].values - data[coord_name][0].values 
            delta = (old_lenght-1) / (new_length-1)
            new_dataset = data.copy(deep=True)
            new_coord=[data[coord_name][0].values + i*delta for i in range(0, new_length)]
        #old_coord = data[coord_name]
        #new_coord=[interpolate(old_coord, i*delta) for i in range(0,new_length)]
        #new_dataset = new_dataset.interp(lat=new_dataset[coord_name][:]+del_c, method="linear", kwargs={"fill_value": "extrapolate"})
        if coord_name == 'lat':  
            new_dataset = new_dataset.interp(lat=new_coord, method="linear", kwargs={"fill_value": "extrapolate"})
        elif coord_name == 'lon':
            new_dataset = new_dataset.interp(lon=new_coord, method="linear", kwargs={"fill_value": "extrapolate"}) 
        return new_dataset
    else:
        return data

def space_regrider(data, space_grid_factor=None,  new_length=None,  lat_length=None, lon_length=None):
    if space_grid_factor is not None:
        new_dataset_lat = lon_lat_regrider(data, space_grid_factor=space_grid_factor, coord_name = 'lat')
        new_dataset_lat_lon = lon_lat_regrider(new_dataset_lat, space_grid_factor=space_grid_factor, coord_name = 'lon')
        return new_dataset_lat_lon
    elif new_length is not None:
        if lat_length is None and lon_length is None:
            new_dataset_lat = lon_lat_regrider(data, new_length=new_length, coord_name = 'lat')
            new_dataset_lat_lon = lon_lat_regrider(new_dataset_lat, new_length=new_length, coord_name = 'lon')
            return new_dataset_lat_lon
    elif new_length is None:
        if lat_length is not None or lon_length is not None:
            new_dataset_lat = lon_lat_regrider(data, new_length=lat_length, coord_name = 'lat')
            new_dataset_lat_lon = lon_lat_regrider(new_dataset_lat, new_length=lon_length, coord_name = 'lon')
            return new_dataset_lat_lon
    else:
        return data


def new_time_coordinate(data, dummy_data, freq = None, time_length=None, factor=None):
    if data.time.size>1 and dummy_data.time.size>1: 
        if data['time'][0]>dummy_data['time'][0]:
            starting_time = str(data['time'][0].values)
        elif data['time'][0]<=dummy_data['time'][0]:
            starting_time = str(dummy_data['time'][0].values)

        if freq is None:
            if time_interpreter(data)==time_interpreter(dummy_data):
                freq=time_interpreter(data)
            else:
                if (data['time'][1]-data['time'][0])>(dummy_data['time'][1]-dummy_data['time'][0]):
                    freq=time_interpreter(data)
                else:
                    freq=time_interpreter(dummy_data)

        if time_length is None:
            if factor is None:
                if data['time'][-1]<dummy_data['time'][-1]:
                    final_time = str(data['time'][-1].values)
                elif data['time'][-1]>=dummy_data['time'][-1]:
                    final_time = str(dummy_data['time'][-1].values)

                return pd.date_range(start=starting_time, end=final_time, freq=freq) 
            elif isinstance(factor, int) or isinstance(factor, float):
                time_length=data.time.size*abs(factor)
                return pd.date_range(starting_time, freq=freq, periods=time_length) 
        else:
            return pd.date_range(starting_time, freq=freq, periods=time_length) 

def time_regrider(data, time_grid_factor =None, new_time_unit = None, time_length=None):
    """The time regrider of the Dataset

    Args:
        data (xarray):                      The Dataset
        time_grid_factor (int, optional):   The resolution of the new time grid. If the input value is negative, the time grid will be less dense 
                                            in time_grid_factor time. If the input value is positive, the time grid will be dense 
                                            in time_grid_factor times. Defaults to None.
        new_time_unit (str, optional):      The string, which contains the time unit and number. For example, '3H' or '1D'. 
                                            Defaults to None.

    Returns:
        xarray: The time regrided Dataset.
    """    
    # Add units converter!!!
    if new_time_unit!=None and  time_grid_factor ==None:
        old_unit = time_interpreter(data)
        old_number = float("".join([char for char in old_unit if char.isnumeric()]))
        old_time_unit_name = "".join([char for char in old_unit if char.isalpha()]) 
        new_number = float("".join([char for char in new_time_unit if char.isnumeric()]))
        new_time_unit_name = "".join([char for char in new_time_unit if char.isalpha()]) 
        
        if int(old_number/new_number)>0: 
            if old_time_unit_name==new_time_unit_name:
                time_grid_factor  = int(old_number/new_number)
        else:
            if old_time_unit_name==new_time_unit_name:
                time_grid_factor  = -  int(new_number/old_number) 

    if isinstance(time_grid_factor , int) and  time_length is None:
        if time_grid_factor >1:
            del_t = int((int(data['time'][1])- int(data['time'][0]))/time_grid_factor )
            ds = []
            for i in range(1, time_grid_factor ):
                new_dataset = data.copy(deep=True)
                new_dataset['time'] = data['time'][:]+del_t
                new_dataset.values = data.interp(time=new_dataset['time'][:])
                ds.append(new_dataset)
            combined = xarray.concat(ds, dim='time')
            return combined
        else:
            time_grid_factor  = abs(time_grid_factor )
            new_dataset = data[::time_grid_factor ]
            for i in range(0, data['time'].size):
                new_dataset[i]=data[i:i+time_grid_factor ].mean()
                return new_dataset      
    #elif time_length is not None:
    #    old_lenght =  data['time'].size     
    #    delta = (old_lenght-1) / (time_length)
    #    new_dataset = data.copy(deep=True)
    ##    old_coord = data['time'][:] 
    #    new_coord=[interpolate(old_coord, i*delta) for i in range(0,time_length)]
    #    new_dataset = new_dataset.interp(time=new_coord, method="linear", kwargs={"fill_value": "extrapolate"})
    #    return new_dataset
    



def mirror_dummy_grid(data,  dummy_data, space_grid_factor=None, time_freq=None, time_length=None, time_grid_factor=None):
    if 'xarray' in str(type(dummy_data)): 
        if space_grid_factor is not None:
            dummy_data=space_regrider(data=dummy_data, space_grid_factor=space_grid_factor)
            #new_dataset_lat_lon=space_regrider(data, space_grid_factor=space_grid_factor)
            new_dataset_lat_lon=space_regrider(data, lat_length=dummy_data.lat.size, lon_length=dummy_data.lon.size)
        else:
            new_dataset_lat_lon=space_regrider(data, lat_length=dummy_data.lat.size, lon_length=dummy_data.lon.size)
        
        #new_dataset_lat = lon_lat_regrider(data, new_length=dummy_data.lat.size, coord_name = 'lat')
        #new_dataset_lat_lon = lon_lat_regrider(new_dataset_lat, new_length=dummy_data.lon.size, coord_name = 'lon')
    

        #new_time_coordinate(data, dummy_data, freq=None, time_length=None, factor=None):
        if data.time.size>1 and dummy_data.time.size>1: 
            new_time_coord = new_time_coordinate(data=data, dummy_data=dummy_data, freq=time_freq, 
                                                time_length=time_length, factor=time_grid_factor)
            new_data = new_dataset_lat_lon.interp(time=new_time_coord, method="linear", kwargs={"fill_value": "extrapolate"})
            new_dummy_data = dummy_data.interp(time=new_time_coord, method="linear", kwargs={"fill_value": "extrapolate"})
            #new_dataset = time_regrider(new_dataset_lat_lon, time_length=dummy_data.time.size)

            return new_data, new_dummy_data
        else:
            return new_dataset_lat_lon, dummy_data
    
        # 1) swap the length? choose minimal? 
        # 2) regride the dummmy data at first

    #if space_grid_factor is None:
    #    return new_dataset_lat_lon
    ##elif space_grid_factor is not None:
    #    new_dataset_lat_reg = lon_lat_regrider(new_dataset_lat_lon, space_grid_factor=space_grid_factor, coord_name = 'lat')
    #    new_dataset_lat_lon_reg = lon_lat_regrider(new_dataset_lat_reg, space_grid_factor=space_grid_factor, coord_name = 'lon')
    #    return new_dataset_lat_lon_reg





