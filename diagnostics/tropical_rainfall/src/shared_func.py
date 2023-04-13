import numpy as np

import matplotlib.pyplot as plt
import matplotlib.animation as animation



import cartopy
import cartopy.feature as cfeature
import cartopy.crs as ccrs

def xarray_attribute_update(xr1, xr2):
    combined_attrs = {**xr1.attrs, **xr2.attrs}
    history_attr  = xr1.attrs['history'] +  xr2.attrs['history']
    xr1.attrs = combined_attrs
    xr1.attrs['history'] = history_attr
    return xr1

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
def time_interpreter(dataset):
    if dataset['time'].size==1:
        return 'False. Load more timesteps then one'
    try:
        if np.count_nonzero(dataset['time.second'] == dataset['time.second'][0]) == dataset.time.size:
            if np.count_nonzero(dataset['time.minute'] == dataset['time.minute'][0]) == dataset.time.size:
                if  np.count_nonzero(dataset['time.hour'] == dataset['time.hour'][0]) == dataset.time.size:
                    if np.count_nonzero(dataset['time.day'] == dataset['time.day'][0] ) == dataset.time.size or \
                        np.count_nonzero([dataset['time.day'][i] in [1, 28, 29, 30, 31] for i in range(0, len(dataset['time.day']))]) == dataset.time.size:
                                            
                        if np.count_nonzero(dataset['time.month'] == dataset['time.month'][0]) == dataset.time.size:
                            return 'Y'
                        else:
                            return 'M'  
                    else:
                        return 'D'
                else:
                    timestep = dataset.time[1] - dataset.time[0]
                    n_hours = int(timestep/(60 * 60 * 10**9) )
                    return str(n_hours)+'H'
            else:
                timestep = dataset.time[1] - dataset.time[0]
                n_minutes = int(timestep/(60  * 10**9) )
                return str(n_minutes)+'m'
                # separate branch, PR, not part of this diagnostic
        else:
            return 1
    
    except KeyError and AttributeError:
        timestep = dataset.time[1] - dataset.time[0]
        if timestep >=28 and timestep <=31:
            return 'M' 



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
    
    ax.axhspan(-trop_lat, trop_lat, facecolor='grey', alpha=0.3)
    im = plt.imshow(a, interpolation='none',  aspect='auto', vmin=vmin, vmax=vmax, #alpha = 1., 
                    extent = (-180, 180, - trop_lat, trop_lat), origin = 'upper')

    fig.colorbar(im)

    def animate_func(i):
        if i % fps == 0:
            print( '.', end ='' )
        im.set_array(snapshots[i])
        return [im]

    dpi = 100
    fig.set_size_inches(10, 10, True)
    writer = animation.writers['ffmpeg'](fps=30, extra_args=['-vcodec', 'libx264'])
    
    anim = animation.FuncAnimation(fig, 
                            animate_func, 
                            frames = nSeconds * fps,
                            interval = 1000 / fps, # in ms
                            )

    plt.title(title,     fontsize =18)
    plt.xlabel('longitude',                 fontsize =18)
    plt.ylabel('latitude',                  fontsize =18)
    plt.xticks([-180, -120, -60, 0, 60, 120, 180],  fontsize=14)   #, 240, 300, 360
    plt.yticks([-90, -60, -30, 0, 30, 60, 90],      fontsize=14) 
   
    
    anim.save('../notebooks/figures/animation/'+str(label)+'_anim.mp4', writer=writer,  dpi=dpi) #

    print('Done!')
    return  True

""" """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """
def image_creator(ds, vmin = None, vmax = None, trop_lat = 10, contour  = True,   label = 'test',  title = 'Tropical precipitation', resol = '110m'):
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
    ds = ds.where( ds > vmin, drop=True) 

    try:
        snapshot = ds[0,:,:] 
    except IndexError:
        snapshot = ds[0,:]
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













