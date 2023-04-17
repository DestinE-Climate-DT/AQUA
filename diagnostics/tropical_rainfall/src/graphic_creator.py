import numpy as np
import xarray

import matplotlib.pyplot as plt
import matplotlib.animation as animation

import cartopy
import cartopy.feature as cfeature
import cartopy.crs as ccrs

from diagnostics.tropical_rainfall.src.time_functions import time_into_plot_title, time_interpreter, month_convert_num_to_str, hour_convert_num_to_str, time_units_converter 
#list of functions 
    # data_size
    # animation_creator
    # image_creator

def data_size(data):
    if 'DataArray' in str(type(data)):
            _size = data.size
    elif 'Dataset' in str(type(data)): 
        _names = list(data.dims) #_coord_names)
        _size = 1
        for i in _names:
            _size *= data[i].size
    return _size

""" """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """
def animation_creator(ds, vmin = None, vmax = None, trop_lat = 10,  time_ind_max = None,  nSeconds = 10,  contour  = True, label = 'test', title = 'Tropical precipitation', resol = '110m'):
    """ 
        The function ***median_per_timestep*** takes few arguments, ***data***, ***variable_1*** and ***trop_lat, s_year, f_year, s_month, f_month***, 
        and returns the median value of the argument ***variable_1*** for each time step.
        
        Args:
            data        (Dataset)   :   The Dataset.

            trop_lat    (int/float) :   Tropical latitudes borders. By definition,  *** trop_lat = 10***.

            threshold 

            nSeconds
            
            label

            resol

            s_year      (int)       :   The starting/first year of the desired Dataset. By definition,  *** s_year = self.s_year = None ***.

            f_year      (int)       :   The final/last year of the desired Dataset. By definition,  *** f_year = self.f_year = None ***.

            s_month     (int)       :   The starting/first month of the desired Dataset. By definition,  *** s_month = self.s_month = None ***.

            f_month     (int)       :   The final/last month of the desired Dataset. By definition,  *** f_month = self.f_month = None ***.

        
        Return:
            movie      (mp4)        :   
        
    """
    import numpy as np
    ds = ds.where( ds > vmin, drop=True) 

    if time_ind_max != None:
        fps = int(time_ind_max/nSeconds)
    else:
        fps = int(ds.time.size/nSeconds)
    #fps = 20

    snapshots = [ds[number,:,:] for number in range(0, fps*nSeconds )]

    # First set up the figure, the axis, and the plot element we want to animate
    fig = plt.figure( figsize=(20,8) )

    ax = plt.axes(projection=ccrs.PlateCarree())

    if  contour:
        ax.coastlines(resolution=resol)
    
        # use data at this scale
        land = cartopy.feature.NaturalEarthFeature('physical', 'land', \
            scale=resol, edgecolor='k', facecolor=cfeature.COLORS['land'])
        ocean = cartopy.feature.NaturalEarthFeature('physical', 'ocean', \
            scale=resol, edgecolor='none', facecolor=cfeature.COLORS['water'])

        #ax.add_feature(land, alpha =0.2, facecolor='beige')
        #ax.add_feature(ocean,  alpha =0.2,   linewidth=0.2 )

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
   
    
    anim.save('../notebooks/figures/animation/'+str(label)+'_anim.mp4', writer=writer,  dpi=dpi) #

    print('Done!')
    return  True

""" """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """
def image_creator(ds, vmin = None, vmax = None, trop_lat = 10, figsize =1, contour  = True,   label = 'test',  title = 'Tropical precipitation', resol = '110m'):
    """ 
        The function ***median_per_timestep*** takes few arguments, ***data***, ***variable_1*** and ***trop_lat, s_year, f_year, s_month, f_month***, 
        and returns the median value of the argument ***variable_1*** for each time step.
        
        Args:
            data        (Dataset)   :   The Dataset.

            trop_lat    (int/float) :   Tropical latitudes borders. By definition,  *** trop_lat = 10***.

            nSeconds
            
            label

            resol

            s_year      (int)       :   The starting/first year of the desired Dataset. By definition,  *** s_year = self.s_year = None ***.

            f_year      (int)       :   The final/last year of the desired Dataset. By definition,  *** f_year = self.f_year = None ***.

            s_month     (int)       :   The starting/first month of the desired Dataset. By definition,  *** s_month = self.s_month = None ***.

            f_month     (int)       :   The final/last month of the desired Dataset. By definition,  *** f_month = self.f_month = None ***.

        
        Return:
            image       (-)        :   
        
    """
    if vmin != None:
        ds = ds.where( ds > vmin, drop=True) 

    if ds.time.size!=1:
        snapshot = ds[0,:,:] 
    else:
        snapshot = ds
    #try:
    #    snapshot = ds[0,:,:] 
    #except IndexError:
    #    snapshot = ds[0,:]
    # First set up the figure, the axis, and the plot element we want to animate
    fig = plt.figure( figsize=(8*figsize,5*figsize) )
    
    ax = plt.axes(projection=ccrs.PlateCarree())
    
    if  contour:
        ax.coastlines(resolution=resol)
    
        # use data at this scale
        land = cartopy.feature.NaturalEarthFeature('physical', 'land', \
            scale=resol, edgecolor='k', facecolor=cfeature.COLORS['land'])
        ocean = cartopy.feature.NaturalEarthFeature('physical', 'ocean', \
            scale=resol, edgecolor='none', facecolor=cfeature.COLORS['water'])

        #ax.add_feature(land, alpha =0.2, facecolor='beige')
        #ax.add_feature(ocean,  alpha =0.2,   linewidth=0.2 )
    
    ax.gridlines() 

    # branch, animation for different variables 
    
    ax.axhspan(-trop_lat, trop_lat, facecolor='grey', alpha=0.3)
    im = plt.imshow(snapshot, interpolation='none',  aspect='auto', vmin=vmin, vmax=vmax, #alpha = 1., 
                    extent = (-180, 180, - trop_lat, trop_lat), origin = 'upper')

    fig.colorbar(im)

    plt.title( title,     fontsize =18)
    plt.xlabel('longitude',                 fontsize =18)
    plt.ylabel('latitude',                  fontsize =18)
    plt.xticks([-180, -120, -60, 0, 60, 120, 180],  fontsize=14)   #, 240, 300, 360
    plt.yticks([-90, -60, -30, 0, 30, 60, 90],      fontsize=14) 
    plt.savefig('../notebooks/figures/'+str(label)+'.png')
    print('Done!')


def lon_lat_regrider(data, step = None, coord_name = 'lat'):
    # work only for lat and lon only for now. Check the line with interpolation command and modify it in the future
    if isinstance(step, int):
        if step>1:
            del_c = float((float(data[coord_name][1])- float(data[coord_name][0]))/2)
            ds = []
            new_dataset = data.copy(deep=True)
            new_dataset[coord_name] = data[coord_name][:] 
            for i in range(1, step):
                if coord_name == 'lat':  
                    new_dataset = new_dataset.interp(lat=new_dataset['lat'][:]+del_c, method="linear", kwargs={"fill_value": "extrapolate"})
                elif coord_name == 'lon':
                    new_dataset = new_dataset.interp(lon=new_dataset['lon'][:]+del_c, method="linear", kwargs={"fill_value": "extrapolate"}) 
                ds.append(new_dataset)
                del_c = del_c/2
            combined = xarray.concat(ds, dim=coord_name)
            combined = combined.sortby(combined[coord_name])
            return combined
        elif step<1:
            step = abs(step)
            if coord_name == 'lat': 
                new_dataset = data.isel(lat=[i for i in range(0, data.lat.size, step)])
            elif coord_name == 'lon':
                new_dataset = data.isel(lon=[i for i in range(0, data.lon.size, step)])
            return new_dataset












