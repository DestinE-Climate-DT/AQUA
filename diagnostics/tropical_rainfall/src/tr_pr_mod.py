import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
import time
import fast_histogram 

import boost_histogram as bh #pip
import dask.array as da
import dask_histogram as dh # pip

import dask_histogram.boost as dhb
import dask


import cartopy
import cartopy.feature as cfeature
import cartopy.crs as ccrs

import matplotlib.animation as animation


class TR_PR_Diagnostic:
    """ 
        Initialization of class object.
        The class has the following attributes:

            trop_lat        (int/float) :   Tropical latitudes borders. By definition,  *** trop_lat = self.trop_lat = 10***.

            s_year          (int)       :   The starting/first year of the desired Dataset. By definition,  *** s_year = self.s_year = None ***.

            f_year          (int)       :   The final/last year of the desired Dataset. By definition,  *** f_year = self.f_year = None ***.

            s_month         (int)       :   The starting/first month of the desired Dataset. By definition,  *** s_month = self.s_month = None ***.

            f_month         (int)       :   The final/last month of the desired Dataset. By definition,  *** f_month = self.f_month = None ***.
            num_of_bins     (int)       : 
            
                The number of bins in the histogram. By definition, ***num_of_bins = None***.
            first_edge      (int)       :   The first edge of the first bin of the histogram. By definition, ***first_edge = None***. 
            width_of_bin    (int)       :   Histogram bin width. By definition, ***width_of_bin = None***.

            
        The class contains the following methods:
            ds_per_lat_range
            ds_per_time_range
            ds_into_array
            mean_per_timestep
            median_per_timestep
            preprocessing
            hist1d_fast
            hist1d_np
            hist1d_pyplot
            hist_plot
            hist_figure
            hist_calculation_array_right
            hist_calculation_array_left

            

    """
    #attributes = inspect.getmembers(diag, lambda a:not(inspect.isroutine(a)))

    def attributes_update(self,   trop_lat = 10, s_year = None, f_year = None, s_month = None, f_month = None, num_of_bins = None, first_edge = None, width_of_bin = None):
        
        if trop_lat:    self.trop_lat = trop_lat

        if s_year:      self.s_year = s_year
        if f_year:      self.f_year = f_year
        if s_month:     self.s_month = s_month
        if f_month:     self.f_month = f_month 


        if num_of_bins:     self.num_of_bins = num_of_bins
        if first_edge:      self.first_edge = first_edge
        if width_of_bin:    self.width_of_bin = width_of_bin 

        # check if its possible to generilize 


    def __init__(self,
            trop_lat = 10, 

            s_year = None,
            f_year = None, 
            s_month = None,
            f_month = None, 

            num_of_bins = None, 
            first_edge = None, 
            width_of_bin = None):


        # Attributes are assigned to all objects of the class
        self.trop_lat   = trop_lat 
           
        self.s_year     = s_year
        self.f_year     = f_year   
        self.s_month    = s_month
        self.f_month    = f_month  
        
        self.num_of_bins    = num_of_bins
        self.first_edge     = first_edge
        self.width_of_bin   = width_of_bin

    

    
    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ 
    def coordinate_names(self, data):

        coord_lat, coord_lon = None, None

        if 'Dataset' in str(type(data)): 
            for i in data._coord_names:
                if 'lat' in i:
                    coord_lat = i
                if 'lon' in i:
                    coord_lon = i 
            

        elif 'DataArray' in str(type(data)):
            for i in data.coords:
                if 'lat' in i:
                    coord_lat = i
                if 'lon' in i:
                    coord_lon = i 
        return coord_lat, coord_lon

        
    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ 
    def ds_per_lat_range(self, data, trop_lat = None):
        """ 
            The function ***ds_per_lat_range*** takes two arguments, ***data*** and ***trop_lat***, 
            and returns the given Dataset only for selected tropical latitudes.
            
            Args:
                data        (Dataset)   :   The Dataset.
                trop_lat    (int/float) :   Tropical latitudes borders. By definition,  *** trop_lat = self.trop_lat = 10***. 
            
            Return:
                Dataset                 :   Given Dataset only for selected tropical latitudes. 
            
        """
        
        #If the user has specified a function argument ***trop_lat***, then the argument becomes a new class attribute. 
        coord_lat, coord_lon = self.coordinate_names(data)
        self.attributes_update(trop_lat = trop_lat)

        data_trop = data.where(abs(data[coord_lat]) <= self.trop_lat, drop=True)  
        return data_trop
    


    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ 
    def ds_per_time_range(self, data, s_year = None, f_year = None,
        s_month = None, f_month = None):
        """ 
            The function ***ds_per_time_range*** takes few arguments, ***data*** and ***s_year, f_year, s_month, f_month***, 
            and returns the given Dataset only for selected time range.
            
            Args:
                data        (Dataset)   :   The Dataset.

                s_year      (int)       :   The starting/first year of the desired Dataset. By definition,  *** s_year = self.s_year = None ***.

                f_year      (int)       :   The final/last year of the desired Dataset. By definition,  *** f_year = self.f_year = None ***.

                s_month     (int)       :   The starting/first month of the desired Dataset. By definition,  *** s_month = self.s_month = None ***.

                f_month     (int)       :   The final/last month of the desired Dataset. By definition,  *** f_month = self.f_month = None ***.
            
            Return:
                Dataset                 :   Given Dataset only for selected time range. 
            
        """
        
        #If the user has specified a function argument ***s_year,  f_year, s_month, f_month***, then the argument becomes a new class attributes.
        self.attributes_update(s_year=s_year, f_year=f_year, s_month=s_month, f_month=f_month)

        if self.s_year != None and self.f_year == None:
            data= data.where(data['time.year'] == self.s_year, drop=True)
        elif self.s_year != None and self.f_year != None:
            data = data.where(data['time.year'] >= self.s_year, drop=True)
            data = data.where(data['time.year'] <= self.f_year, drop=True)
        if self.s_month != None and self.f_month != None:
            data = data.where(data['time.month'] >= self.s_month, drop=True)
            data = data.where(data['time.month'] <= self.f_month, drop=True)  
        return data

    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ 
    def ds_into_array(self, data, variable_1 = 'pr', sort = False):
        """ 
            The function ***ds_into_array*** takes two arguments, ***data*** and ***variable_1***, 
            and returns the sorted Dataarray only for selected ***variable_1***.
            
            Args:
                data        (Dataset)   :   The Dataset.
                variable_1  (str)       :   The variable of the Dataset. By definition, ***variable_1 = 'pr'***.
                                                   
            Return:
                Dataarray               :   Sorted Dataarray only for selected ***variable_1***. 
            
        """
        coord_lat, coord_lon = self.coordinate_names(data)

        if 'Dataset' in str(type(data)):
            data  = data[variable_1]

        data_1d  = data.stack(total=['time', coord_lat, coord_lon])
        if sort == True:
            data_1d  = data_1d.sortby(data_1d)
        return data_1d

    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ 
    def mean_per_timestep(self, data, variable_1 = 'pr', trop_lat = None, 
        s_year = None, f_year = None, s_month = None, f_month = None):
        """ 
            The function ***mean_per_timestep*** takes few arguments, ***data***, ***variable_1*** and ***trop_lat, s_year, f_year, s_month, f_month***, 
            and returns the average value of the argument ***variable_1*** for each time step.
            
            Args:
                data        (Dataset)   :   The Dataset.
                variable_1  (str)       :   The variable of the Dataset. By definition, ***variable_1 = 'pr'***.
                                            
                trop_lat    (int/float) :   Tropical latitudes borders. By definition,  *** trop_lat = self.trop_lat = 10***.

                s_year      (int)       :   The starting/first year of the desired Dataset. By definition,  *** s_year = self.s_year = None ***.

                f_year      (int)       :   The final/last year of the desired Dataset. By definition,  *** f_year = self.f_year = None ***.

                s_month     (int)       :   The starting/first month of the desired Dataset. By definition,  *** s_month = self.s_month = None ***.

                f_month     (int)       :   The final/last month of the desired Dataset. By definition,  *** f_month = self.f_month = None ***.
            
            Return:
                object      (float)     :   The average value of the argument ***variable_1*** for each time step. 
            
        """
        if 'lat' in data.dims:
            #If the user has specified a function argument **trop_lat, s_year, f_year, s_month, f_month***, then the argument becomes a new class attributes.
            self.attributes_update(trop_lat = trop_lat, s_year = s_year, f_year = f_year, s_month = s_month, f_month = f_month)
        
        
            coord_lat, coord_lon = self.coordinate_names(data)

            ds = self.ds_per_lat_range(data, self.trop_lat)
            ds = self.ds_per_time_range(ds, self.s_year, self.f_year, self.s_month, self.f_month)
            if 'Dataset' in str(type(data)):
                ds = ds[variable_1]

            return ds.mean(coord_lat).mean(coord_lon)
        else:
            for i in data.dims:
                coord = i
            return data.median(coord)



    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """
    def median_per_timestep(self, data, variable_1 = 'pr', trop_lat = None, 
        s_year = None, f_year = None, s_month = None, f_month = None):
        """ 
            The function ***median_per_timestep*** takes few arguments, ***data***, ***variable_1*** and ***trop_lat, s_year, f_year, s_month, f_month***, 
            and returns the median value of the argument ***variable_1*** for each time step.
            
            Args:
                data        (Dataset)   :   The Dataset.

                variable_1  (str)       :   The variable of the Dataset. By definition, ***variable_1 = 'pr'***.

                trop_lat    (int/float) :   Tropical latitudes borders. By definition,  *** trop_lat = self.trop_lat = 10***.

                s_year      (int)       :   The starting/first year of the desired Dataset. By definition,  *** s_year = self.s_year = None ***.

                f_year      (int)       :   The final/last year of the desired Dataset. By definition,  *** f_year = self.f_year = None ***.

                s_month     (int)       :   The starting/first month of the desired Dataset. By definition,  *** s_month = self.s_month = None ***.

                f_month     (int)       :   The final/last month of the desired Dataset. By definition,  *** f_month = self.f_month = None ***.
            
            Return:
                object      (float)     :   The median value of the argument ***variable_1*** for each time step. 
            
        """

        if 'lat' in data.dims:
            #If the user has specified a function argument **trop_lat, s_year, f_year, s_month, f_month***, then the argument becomes a new class attributes.
            self.attributes_update(trop_lat = trop_lat, s_year = s_year, f_year = f_year, s_month = s_month, f_month = f_month)
        
            coord_lat, coord_lon = self.coordinate_names(data)

            ds = self.ds_per_lat_range(data, self.trop_lat)
            ds = self.ds_per_time_range(ds, self.s_year, self.f_year, self.s_month, self.f_month)
            if 'Dataset' in str(type(data)):
                ds = ds[variable_1]

            return ds.median(coord_lat).median(coord_lon)
        
        else:
            for i in data.dims:
                coord = i
            return data.median(coord)
    
    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """
    def animated(self, data, variable_1 = 'pr', trop_lat = None,  threshold = 0.5, time_ind_max = None,  nSeconds = 10, label = 'test', resol = '110m',
        s_year = None, f_year = None, s_month = None, f_month = None, first_edge = None,  num_of_bins = None,  width_of_bin = None):
        """ 
            The function ***median_per_timestep*** takes few arguments, ***data***, ***variable_1*** and ***trop_lat, s_year, f_year, s_month, f_month***, 
            and returns the median value of the argument ***variable_1*** for each time step.
            
            Args:
                data        (Dataset)   :   The Dataset.

                variable_1  (str)       :   The variable of the Dataset. By definition, ***variable_1 = 'pr'***.

                trop_lat    (int/float) :   Tropical latitudes borders. By definition,  *** trop_lat = self.trop_lat = 10***.

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
        #If the user has specified a function argument **trop_lat, s_year, f_year, s_month, f_month, first_edge, num_of_bins, width_of_bin***, 
        # then the argument becomes a new class attributes.
        self.attributes_update(trop_lat = trop_lat, s_year = s_year, f_year = f_year, s_month = s_month, f_month = f_month, first_edge = first_edge, num_of_bin = num_of_bins, width_of_bin = width_of_bin)
        
        ds = self.ds_per_lat_range(data)[variable_1]

        _vmin = ds.mean() * threshold
        _vmax = self.first_edge  + self.num_of_bins * self.width_of_bin


        ds = ds.where( ds > _vmin, drop=True) 

        if time_ind_max != None:
            fps = int(time_ind_max/nSeconds)
        else:
            fps = int(ds.time.size/nSeconds)
        #fps = 20

        snapshots = [ds[number,:,:] for number in range(0, fps*nSeconds )]

        # First set up the figure, the axis, and the plot element we want to animate
        fig = plt.figure( figsize=(20,8) )

        ax = plt.axes(projection=ccrs.PlateCarree())
        ax.coastlines(resolution=resol)
        ax.gridlines()

        # use data at this scale
        land = cartopy.feature.NaturalEarthFeature('physical', 'land', \
            scale=resol, edgecolor='k', facecolor=cfeature.COLORS['land'])
        ocean = cartopy.feature.NaturalEarthFeature('physical', 'ocean', \
            scale=resol, edgecolor='none', facecolor=cfeature.COLORS['water'])

        ax.add_feature(land, alpha =0.2, facecolor='beige')
        ax.add_feature(ocean,  alpha =0.2,   linewidth=0.2 )

        # branch, animation for different variables 
        


        a = snapshots[0]
        
        ax.axhspan(-self.trop_lat, self.trop_lat, facecolor='grey', alpha=0.3)
        im = plt.imshow(a, interpolation='none', alpha = 1.,  aspect='auto', vmin=_vmin, vmax=_vmax, extent = (-180, 180, - self.trop_lat, self.trop_lat), origin = 'upper')

        fig.colorbar(im)
        def animate_func(i):
            if i % fps == 0:
                print( '.', end ='' )
            im.set_array(snapshots[i])
            return [im]


        anim = animation.FuncAnimation(fig, 
                               animate_func, 
                               frames = nSeconds * fps,
                               interval = 1000 / fps, # in ms
                               )

        plt.title('Tropical precipitation',     fontsize =18)
        plt.xlabel('longitude',                 fontsize =18)
        plt.ylabel('latitude',                  fontsize =18)
        plt.xticks([-180, -120, -60, 0, 60, 120, 180],  fontsize=14)   #, 240, 300, 360
        plt.yticks([-90, -60, -30, 0, 30, 60, 90],      fontsize=14) 
        anim.save(str(label)+'_anim.mp4', fps=fps, extra_args=['-vcodec', 'libx264'])

        print('Done!')
        return  True
    




    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """
    def preprocessing(self, data, preprocess = True, variable_1 = 'pr', trop_lat = None, 
        s_year = None, f_year = None, s_month = None, f_month = None, sort = False, dask_array = False):
        """ 
            The function ***preprocessing*** takes few arguments, ***data***, ***_preprocess***,  ***variable_1*** and ***trop_lat, s_year, f_year, s_month, f_month***, 
            and returns sorted Dataarray only for selected ***variable_1*** for selected tropical latitudes and for selected time range.
            
            Args:
                data        (Dataset)   :   The Dataset.
                preprocess     (bool)   :   If ***_preprocess=True***, then function *** preprocessing *** converse the Dataset into DataArray.
                                            By definition, ***_preprocess=True***.  

                variable_1  (str)       :   The variable of the Dataset. By definition, ***variable_1 = 'pr'***.
                                

                trop_lat    (int/float) :   Tropical latitudes borders. By definition,  *** trop_lat = self.trop_lat = 10***.
                                            
                s_year      (int)       :   The starting/first year of the desired Dataset. By definition,  *** s_year = self.s_year = None ***.

                f_year      (int)       :   The final/last year of the desired Dataset. By definition,  *** f_year = self.f_year = None ***.

                s_month     (int)       :   The starting/first month of the desired Dataset. By definition,  *** s_month = self.s_month = None ***.

                f_month     (int)       :   The final/last month of the desired Dataset. By definition,  *** f_month = self.f_month = None ***.

            
            Return:
                object      (Dataarray) :   Sorted Dataarray only for selected ***variable_1*** for selected tropical latitudes and for selected time range. 
            
        """
        #If the user has specified a function argument **trop_lat, s_year, f_year, s_month, f_month***, 
        # then the argument becomes a new class attributes.
        self.attributes_update(trop_lat = trop_lat, s_year = s_year, f_year = f_year, s_month = s_month, f_month = f_month)
        
        if preprocess == True: 
            ds = self.ds_per_lat_range(data, self.trop_lat)
            ds = self.ds_per_time_range(ds, self.s_year, self.f_year, self.s_month, self.f_month)
            ds = self.ds_into_array(ds, variable_1, sort)
            if dask_array == True:
                ds = da.from_array(ds)
            return ds 
        else:
            print("Nothong to preprocess")


    """ """ """ """ """ """ """ """ """ """
    def hist_np_digitize(self, data, preprocess = True, trop_lat = 10, variable_1 = 'pr',  num_of_bins = None,  s_year = None, f_year = None, s_month = None, f_month = None, 
        first_edge = None,  width_of_bin  = None,   _add = None, *, start = []):

        _time_1 = time.time()

        #If the user has specified a function argument **trop_lat, s_year, f_year, s_month, f_month***, 
        # then the argument becomes a new class attributes.
        self.attributes_update(trop_lat = trop_lat, s_year = s_year, f_year = f_year, s_month = s_month, f_month = f_month, first_edge = first_edge, num_of_bins = num_of_bins,  width_of_bin = width_of_bin)

        if preprocess == True:
            data = self.preprocessing(data, preprocess, variable_1, trop_lat, s_year, f_year, s_month, f_month,  sort = False, dask_array = False)

        size = data.size
        
        bins = [self.first_edge  + self.width_of_bin*i for i in range(0, self.num_of_bins+1)]

        frequency_bin =  xr.DataArray([np.count_nonzero(np.digitize(data, bins) == i) for i in range(1, self.num_of_bins+1)], coords=[bins[0:-1]], dims=["bin"])
        
        _time_2 = time.time() 
       
        del_time = (_time_2 - _time_1)

        return  frequency_bin, size, del_time 




    """ """ """ """ """ """ """ """ """ """
    def hist1d_fast(self, data, preprocess = True, trop_lat = 10, variable_1 = 'pr',  num_of_bins = None, s_year = None, f_year = None, s_month = None, f_month = None, 
        first_edge = None,  width_of_bin  = None,   _add = None, *, start = []):
        """ 
            The function ***hist1d_fas*** takes few arguments, ***data***, *** _preprocess***, ***variable_1***, 
            ***num_of_bins***, ***first_edge*** and ***width_of_bin***, 
            calculates the frequency histogram 
                    with the use of fast_histogram.histogram1d (***fast_histogram*** package)
            and returns the dataset of frequency per bins, size of input Dataset or Dataarray, and 
            time consumed by function.  
            
            Args:
                data        (Dataset)   :   The Dataset.
                preprocess     (bool)   :   If ***_preprocess=True***, then function *** preprocessing *** converse the Dataset into DataArray.
                                            By definition, ***_preprocess=True***. 

                variable_1      (str)   :   Tropical latitudes borders. By definition,  *** trop_lat = self.trop_lat = 10***.

                num_of_bins     (int)   :   The number of bins in the histogram. By definition, ***num_of_bins = None***.
                first_edge      (int)   :   The first edge of the first bin of the histogram. By definition, ***first_edge = None***. 
                width_of_bin    (int)   :   Histogram bin width. By definition, ***width_of_bin = None***.
            
            Return:
                Dataset                 :   frequency per bins. 
            
        """

        #If the user has specified a function argument **trop_lat, s_year, f_year, s_month, f_month***, 
        # then the argument becomes a new class attributes.
        self.attributes_update(trop_lat = trop_lat, s_year = s_year, f_year = f_year, s_month = s_month, f_month = f_month, first_edge = first_edge, num_of_bins = num_of_bins, width_of_bin = width_of_bin)


        if preprocess == True:
            data = self.preprocessing(data, preprocess, variable_1, trop_lat, s_year, f_year, s_month, f_month,  sort = False, dask_array = False)


        bin_table = [self.first_edge + self.width_of_bin*j for j in range(0, self.num_of_bins)]        
        
        hist_fast = fast_histogram.histogram1d(data, 
            range=[self.first_edge, self.first_edge + (self.num_of_bins)*self.width_of_bin], bins = (self.num_of_bins))
        
        frequency_bin =  xr.DataArray(hist_fast, coords=[bin_table], dims=["bin"])
        

        return  frequency_bin 
    
    """ """ """ """ """ """ """ """ """ """
    def hist1d_np(self, data, preprocess = True, trop_lat = 10, variable_1 = 'pr',  num_of_bins = None,  s_year = None, f_year = None, s_month = None, f_month = None, 
        first_edge = None,  width_of_bin  = None,   _add = None, *, start = []):
        """ 
            The function ***hist1d_np*** takes few arguments, ***data***, *** _preprocess***, ***variable_1***, 
            ***num_of_bins***, ***first_edge*** and ***width_of_bin***, 
            calculates the frequency histogram 
                    with the use of numpy.histogram (***numpy*** package)
            and returns the dataset of frequency per bins, size of input Dataset or Dataarray, and 
            time consumed by function.  
            
            Args:
                data        (Dataset)   :   The Dataset.
                preprocess     (bool)   :   If ***_preprocess=True***, then function *** preprocessing *** converse the Dataset into DataArray.
                                            By definition, ***_preprocess=True***. 
                                            
                variable_1      (str)   :   Tropical latitudes borders. By definition,  *** trop_lat = self.trop_lat = 10***.
   
                num_of_bins     (int)   :   The number of bins in the histogram. By definition, ***num_of_bins = None***.
                first_edge      (int)   :   The first edge of the first bin of the histogram. By definition, ***first_edge = None***. 
                width_of_bin    (int)   :   Histogram bin width. By definition, ***width_of_bin = None***.
            
            Return:
                Dataset                 :   frequency per bins.

            
        """
        #If the user has specified a function argument **trop_lat, s_year, f_year, s_month, f_month***, 
        # then the argument becomes a new class attributes.
        self.attributes_update(trop_lat = trop_lat, s_year = s_year, f_year = f_year, s_month = s_month, f_month = f_month, first_edge = first_edge, num_of_bins = num_of_bins, width_of_bin = width_of_bin)
        
        last_edge = self.first_edge  + self.num_of_bins*self.width_of_bin

        if preprocess == True:
            data = self.preprocessing(data, preprocess, variable_1, trop_lat, s_year, f_year, s_month, f_month,  sort = False, dask_array = False)


        hist_np = np.histogram(data, range=[self.first_edge, self.first_edge + (self.num_of_bins )*self.width_of_bin], bins = (self.num_of_bins))

        frequency_bin =  xr.DataArray(hist_np[0], coords=[hist_np[1][0:-1]], dims=["bin"])

        return  frequency_bin
        
    """ """ """ """ """ """ """ """ """ """
    def hist1d_pyplot(self, data, preprocess = True, trop_lat = 10,  variable_1 = 'pr',  num_of_bins = None,  s_year = None, f_year = None, s_month = None, f_month = None, 
        first_edge = None,  width_of_bin  = None,   _add = None, *, start = []):
        """ 
            The function ***hist1d_pyplot*** takes few arguments, ***data***, *** _preprocess***, ***variable_1***, 
            ***num_of_bins***, ***first_edge*** and ***width_of_bin***, 
            calculates the frequency histogram 
                    with the use of plt.hist (***matplotlib.pyplot*** package)
            and returns the dataset of frequency per bins, size of input Dataset or Dataarray, and 
            time consumed by function.  
            
            Args:
                data        (Dataset)   :   The Dataset.
                preprocess     (bool)   :   If ***_preprocess=True***, then function *** preprocessing *** converse the Dataset into DataArray.
                                            By definition, ***_preprocess=True***. 
                variable_1      (str)   :   Tropical latitudes borders. By definition,  *** trop_lat = self.trop_lat = 10***.

                num_of_bins     (int)   :   The number of bins in the histogram. By definition, ***num_of_bins = None***.
                first_edge      (int)   :   The first edge of the first bin of the histogram. By definition, ***first_edge = None***. 
                width_of_bin    (int)   :   Histogram bin width. By definition, ***width_of_bin = None***.
            
            Return:
                Dataset                 :   frequency per bins.
            
        """
        #If the user has specified a function argument **trop_lat, s_year, f_year, s_month, f_month***, 
        # then the argument becomes a new class attributes.
        self.attributes_update(trop_lat = trop_lat, s_year = s_year, f_year = f_year, s_month = s_month, f_month = f_month, first_edge = first_edge, num_of_bins = num_of_bins, width_of_bin = width_of_bin)  

        last_edge = self.first_edge  + self.num_of_bins*self.width_of_bin

        if preprocess == True:
            data = self.preprocessing(data, preprocess, variable_1, trop_lat, s_year, f_year, s_month, f_month,  sort = False, dask_array = False)

        bins = [self.first_edge  + i*self.width_of_bin for i in range(0, self.num_of_bins+1)]
        hist_pyplt = plt.hist(x = data, bins = bins)

        plt.close()

        frequency_bin =  xr.DataArray(hist_pyplt[0], coords=[hist_pyplt[1][0:-1]], dims=["bin"])
        

        return  frequency_bin

        
         
    """ """ """ """ """ """ """ """ """ """
    def dask_factory(self, data, preprocess = True, trop_lat = 10,  variable_1 = 'pr',  num_of_bins = None, s_year = None, f_year = None, s_month = None, f_month = None, 
        first_edge = None,  width_of_bin  = None,  delay = False, _add = None, *, start = []):
        """ 
            The function ***hist1d_pyplot*** takes few arguments, ***data***, *** _preprocess***, ***variable_1***, 
            ***num_of_bins***, ***first_edge*** and ***width_of_bin***, 
            calculates the frequency histogram 
                    with the use of plt.hist (***matplotlib.pyplot*** package)
            and returns the dataset of frequency per bins, size of input Dataset or Dataarray, and 
            time consumed by function.  
            
            Args:
                data        (Dataset)   :   The Dataset.
                preprocess     (bool)   :   If ***_preprocess=True***, then function *** preprocessing *** converse the Dataset into DataArray.
                                            By definition, ***_preprocess=True***.  
                variable_1      (str)   :   Tropical latitudes borders. By definition,  *** trop_lat = self.trop_lat = 10***.
                                          
                num_of_bins     (int)   :   The number of bins in the histogram. By definition, ***num_of_bins = None***.
                first_edge      (int)   :   The first edge of the first bin of the histogram. By definition, ***first_edge = None***. 
                width_of_bin    (int)   :   Histogram bin width. By definition, ***width_of_bin = None***.
            
            Return:
                Dataset                 :   frequency per bins.
                int                     :   Size input Dataset ot Dataarray
                float                   :   Time consumed by ***hist1d_pyplot*** function. 
            
        """

        #If the user has specified a function argument **trop_lat, s_year, f_year, s_month, f_month***, 
        # then the argument becomes a new class attributes.
        self.attributes_update(trop_lat = trop_lat, s_year = s_year, f_year = f_year, s_month = s_month, f_month = f_month, first_edge = first_edge, num_of_bins = num_of_bins, width_of_bin = width_of_bin)

        last_edge = self.first_edge  + self.num_of_bins*self.width_of_bin
        

        if preprocess == True:
            data = self.preprocessing(data, preprocess, variable_1, trop_lat, s_year, f_year, s_month, f_month,  sort = False, dask_array = True)


        h = dh.factory(data, 
                axes=(bh.axis.Regular(self.num_of_bins, self.first_edge, last_edge),))     
        counts, edges = h.to_dask_array()
        if delay == False:
            counts = counts.compute()
            edges = edges.compute()
        frequency_bin =  xr.DataArray(counts, coords=[edges[0:-1]], dims=["bin"])
        
        return  frequency_bin


    """ """ """ """ """ """ """ """ """ """
    def dask_factory_weights(self, data, preprocess = True, trop_lat = 10,  variable_1 = 'pr',  num_of_bins = None,  s_year = None, f_year = None, s_month = None, f_month = None, 
        first_edge = None,  width_of_bin  = None,   delay = False,   _add = None, *, start = []):
        """ 
            The function ***hist1d_pyplot*** takes few arguments, ***data***, *** _preprocess***, ***variable_1***, 
            ***num_of_bins***, ***first_edge*** and ***width_of_bin***, 
            calculates the frequency histogram 
                    with the use of plt.hist (***matplotlib.pyplot*** package)
            and returns the dataset of frequency per bins, size of input Dataset or Dataarray, and 
            time consumed by function.  
            
            Args:
                data        (Dataset)   :   The Dataset.
                preprocess     (bool)   :   If ***_preprocess=True***, then function *** preprocessing *** converse the Dataset into DataArray.
                                            By definition, ***_preprocess=True***. 
                variable_1      (str)   :   Tropical latitudes borders. By definition,  *** trop_lat = self.trop_lat = 10***.

                num_of_bins     (int)   :   The number of bins in the histogram. By definition, ***num_of_bins = None***.
                first_edge      (int)   :   The first edge of the first bin of the histogram. By definition, ***first_edge = None***. 
                width_of_bin    (int)   :   Histogram bin width. By definition, ***width_of_bin = None***.
            
            Return:
                Dataset                 :   frequency per bins.
            
        """
        #If the user has specified a function argument **trop_lat, s_year, f_year, s_month, f_month***, 
        # then the argument becomes a new class attributes.
        self.attributes_update(trop_lat = trop_lat, s_year = s_year, f_year = f_year, s_month = s_month, f_month = f_month, first_edge = first_edge, num_of_bins = num_of_bins, width_of_bin = width_of_bin)    

        last_edge = self.first_edge  + self.num_of_bins*self.width_of_bin

        if preprocess == True:
            data = self.preprocessing(data, preprocess, variable_1, trop_lat, s_year, f_year, s_month, f_month,  sort = False, dask_array = True)


        ref = bh.Histogram(bh.axis.Regular(self.num_of_bins, self.first_edge, last_edge), storage=bh.storage.Weight())
        h = dh.factory(data, weights=data, histref=ref)
        counts, edges = h.to_dask_array()
        if delay == False:
            counts = counts.compute()
            edges = edges.compute()
        
        frequency_bin =  xr.DataArray(counts, coords=[edges[0:-1]], dims=["bin"])
        

        return  frequency_bin


    """ """ """ """ """ """ """ """ """ """
    def dask_boost(self, data, preprocess = True, trop_lat = 10,  variable_1 = 'pr',  num_of_bins = None, s_year = None, f_year = None, s_month = None, f_month = None, 
        first_edge = None,  width_of_bin  = None,  _add = None, *, start = []):
        """ 
            The function ***hist1d_pyplot*** takes few arguments, ***data***, *** _preprocess***, ***variable_1***, 
            ***num_of_bins***, ***first_edge*** and ***width_of_bin***, 
            calculates the frequency histogram 
                    with the use of plt.hist (***matplotlib.pyplot*** package)
            and returns the dataset of frequency per bins, size of input Dataset or Dataarray, and 
            time consumed by function.  
            
            Args:
                data        (Dataset)   :   The Dataset.
                preprocess     (bool)   :   If ***_preprocess=True***, then function *** preprocessing *** converse the Dataset into DataArray.
                                            By definition, ***_preprocess=True***. 
                variable_1      (str)   :   Tropical latitudes borders. By definition,  *** trop_lat = self.trop_lat = 10***.

                num_of_bins     (int)   :   The number of bins in the histogram. By definition, ***num_of_bins = None***.
                first_edge      (int)   :   The first edge of the first bin of the histogram. By definition, ***first_edge = None***. 
                width_of_bin    (int)   :   Histogram bin width. By definition, ***width_of_bin = None***.
            
            Return:
                Dataset                 :   frequency per bins.
            
        """
        #If the user has specified a function argument **trop_lat, s_year, f_year, s_month, f_month***, 
        # then the argument becomes a new class attributes.
        self.attributes_update(trop_lat = trop_lat, s_year = s_year, f_year = f_year, s_month = s_month, f_month = f_month, first_edge = first_edge, num_of_bins = num_of_bins, width_of_bin = width_of_bin) 

        last_edge = self.first_edge  + self.num_of_bins*self.width_of_bin

        if preprocess == True:
            data = self.preprocessing(data, preprocess, variable_1, trop_lat, s_year, f_year, s_month, f_month,  sort = False, dask_array = True)


        h = dhb.Histogram(dh.axis.Regular(self.num_of_bins, self.first_edge, last_edge),  storage=dh.storage.Double(), )
        h.fill(data)
        
        counts, edges = h.to_dask_array()
        print(counts, edges)
        counts =  dask.compute(counts)
        edges = dask.compute(edges[0]) 
        frequency_bin =  xr.DataArray(counts[0], coords=[edges[0][0:-1]], dims=["bin"])
        #else: 
        #    counts =  dask.compute(counts.to_delayed())
        #    edges = dask.compute(edges[0].to_delayed())
           

        return  frequency_bin



    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """
    def hist_plot(self, data, pdf = None, ls = '-', color = 'tab:blue', label = None):
        """ 
            The function ***hist_plot*** takes few arguments, ***data***, ***pdf***, 
            ***_ls***, ***_color*** and ***_label***, 
            and returns the frequency or pdf histogram. 
            
            Args:
                data            (Dataset)   :   The Dataset.
                pdf             (bool)      :   If ***_pdf=True***, then function returns the pdf histogram.
                                                If ***_pdf=False***, then function returns the frequency histogram.
                                                By defenition, ***_pdf=False***.
                _ls             (str)       :   The linestyle. By definition, _ls = '-' (solid line).
                _color          (str)       :   The color of the line. By definition, _color = 'tab:blue' (blue line). 
                _label          (str)       :   The label of the line for legend. By definition, label = None.  
            
            Return:
                plot                        :   Frequency or pdf histogram. 
            
        """
        print("hist_plot function in the process \n") 

        pdf =  True if None else pdf 
           
        if pdf == True:
            #print('pdf')
            _data_density = data[0:]/sum(data[:]) #*(data.bin[1]-data.bin[0]))
            plt.step(data.bin[0:], _data_density, #data.bin[1]-data.bin[0],
                linewidth=3.0, ls = ls, color = color, label = label )
            plt.ylabel('Probability', fontsize=14)
            plt.xlabel('Total Precipitation', fontsize=14)
            plt.yscale('log') 
        else:
            plt.step(data.bin[0:], data[0:],  #data.bin[1]-data.bin[0], 
                linewidth=3.0,  ls = ls, color = color, label = label )
            plt.ylabel('Frequency', fontsize=14)
            plt.xlabel('Total Precipitation', fontsize=14)
            plt.yscale('log') 



    def hist_figure(self, data, _plt = plt, pdf = None, ls = '-', color = 'tab:blue', label = None):
        """ 
            The function ***hist_figure*** takes few arguments, ***data***, ***_plt***, ***pdf***, 
            ***_ls***, ***_color*** and ***_label***, 
            and returns the frequency or pdf histogram. 
            
            Args:
                data            (Dataset)   :   The Dataset.
                _plt            (bool)      :   The name of the axe of the figure. By definition, _plt = plt.  
                pdf             (bool)      :   If ***_pdf=True***, then function returns the pdf histogram.
                                                If ***_pdf=False***, then function returns the frequency histogram.
                                                By defenition, ***_pdf=False***. 
                _ls             (str)       :   The linestyle. By definition, _ls = '-' (solid line).
                _color          (str)       :   The color of the line. By definition, _color = 'tab:blue' (blue line). 
                _label          (str)       :   The label of the line for legend. By definition, label = None. 
            
            Return:
                figure                      :   Frequency or pdf histogram. 
            
        """

        pdf =  True if None else pdf 
           
        if pdf == True:
            print('pdf')
            _data_density = data[0:]/sum(data[:]) #*(data.bin[1]-data.bin[0]))
            _plt.step(data.bin[0:], _data_density, #data.bin[1]-data.bin[0],
                linewidth=3.0, ls = ls, color = color, label = label )
            _plt.set_ylabel('Probability', fontsize=14)
            _plt.set_xlabel('Total Precipitation', fontsize=14)
            _plt.set_yscale('log') 
        else:
            _plt.step(data.bin[0:], data[0:],  #data.bin[1]-data.bin[0], 
                linewidth=3.0,  ls = ls, color = color, label = label )
            _plt.set_yscale('log')
            _plt.set_ylabel('Frequency', fontsize=14)
            _plt.set_xlabel('Total Precipitation', fontsize=14)
        # FIG SAVE 
            
            
        

    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """
    def hist_calculation_array_right(self, data, _preprocess = True, trop_lat=10, variable_1 = 'pr',  num_of_bins = None, 
        step = None, first_edge = None,  width_of_bin  = None,   _add = None, *, start = []):
        """ 
            The function ***hist_calculation_array_right*** takes few arguments, ***data***, *** _preprocess***, ***variable_1***, 
            ***num_of_bins***, ***first_edge*** and ***width_of_bin***, 
            calculates the frequency histogram starting from the RIGHT (reverse order)
                with the use of loop (package independent)
            and returns the dataset of frequency per bins, size of input Dataset or Dataarray, and 
            time consumed by function.  
            
            Args:
                data            (Dataset)   :   The Dataset.
                preprocess     (bool)       :   If ***_preprocess=True***, then function *** preprocessing *** converse the Dataset into DataArray.
                                                By definition, ***_preprocess=True***. 
                variable_1      (str)       :   Tropical latitudes borders. By definition,  *** trop_lat = self.trop_lat = 10***.
                num_of_bins     (int)       :   The number of bins in the histogram. By definition, ***num_of_bins = None***.
                step            (int)       :   The step in the loop. Helps to speed-up the calculation. 
                first_edge      (int)       :   The first edge of the first bin of the histogram. By definition, ***first_edge = None***. 
                width_of_bin    (int)       :   Histogram bin width. By definition, ***width_of_bin = None***.
            
            Return:
                Dataset                     :   frequency per bins.
                int                         :   Size input Dataset ot Dataarray
                float                       :   Time consumed by ***hist_calculation_array_right*** function. 
            
        """
        #print("hist_calculation function in the progress \n")

        _time_1 = time.time()

        if num_of_bins != None:
            self.num_of_bins = num_of_bins
        if step != None:
            self.step = step
        if first_edge != None:
            self.first_edge = first_edge
        if width_of_bin != None:
            self.width_of_bin = width_of_bin 
        if trop_lat != None:
            self.trop_lat = trop_lat

        # we not want to redefine the value  
        step = self.step

        bin_table = [self.first_edge + self.width_of_bin*j for j in range(0, self.num_of_bins)]
        frequency_table = np.zeros(self.num_of_bins) 
        frequency_bin =  xr.DataArray(frequency_table, coords=[bin_table], dims=["bin"])
        
        #if _add == True:
        #   frequency_bin += start
        if _preprocess == True:
            #print('preprocessing')
            if 'Dataset' in str(type(data)):
                for i in data._coord_names:
                    if 'lat' in i:
                        coord_lat = i
                    if 'lon' in i:
                        coord_lon = i 
            _temp = data.where(abs(data[coord_lat]) <= self.trop_lat, drop=True)     
            _temp = _temp[variable_1].stack(total=['time', coord_lat, coord_lon])
            data  = _temp.sortby(_temp)
            #data = self.preprocessing(data, _preprocess, variable_1) # ADD PARAMETERS

        # Its not necessary because this function only for DataArrays!
        if 'DataArray' in str(type(data)):
            _size = data.size
        elif 'Dataset' in str(type(data)): 
            #print("if you not want to pre-process data with the use of the class, \
            #    please, insert DataArray instead of Dataset")
            _names = list(data._coord_names)
            _size = 1
            for i in _names:
                _size *= data[i].size
        # Important 
        counter = self.num_of_bins -1 

        ind = 0
        iter = 0 
        _step =  self.step
        if data[ind] < self.first_edge:
            while  True:  
                if data[ind] < self.first_edge: 
                    ind = ind + _step  
                else:   
                    _ind = ind 
                    while True:
                        _step = 0.5* _step
                        if  data[_ind] < self.first_edge and iter < 10:
                            _ind = int(_ind + _step)
                            iter += 1
                        elif data[_ind] >= self.first_edge and iter < 10:
                            _ind = int(_ind - _step)
                            iter += 1
                        else: break
                    break 
            ind_negative =  _ind
        else: 
            ind_negative = 0 

        
        #print(ind_negative, 'negative precipitations')

        step = -1 
        for ind in range(_size-1, 0, step):
            if data[ind] <= bin_table[counter]:
                ind_max =  _size-1 - ind
                #print(ind_max, 'leftovers')
                break
        ind_f = _size-1 -  ind_max
        ind = ind_f
        
        while ind>=0:
            if data[ind] < bin_table[counter-1]: #<=
                _step =  step
                _ind = ind 
                iter = 0 
                while True:
                    _step = 0.5* _step
                    if  data[int(_ind-_step)] < bin_table[counter-1] and iter < 10 and abs(_step) >= 1:
                        _ind = int(_ind - _step)
                        iter += 1
                    elif data[int(_ind-_step)] >= bin_table[counter-1] and iter < 10 and abs(_step) >= 1:
                        iter += 1
                    else: 
                        ind = _ind
                        break  

                frequency_bin[counter] = _size -1 - ind  - ind_max -sum(frequency_bin) # _size -1 - ind 
                step = -1- int(10**(0)*frequency_bin[counter])
                ind = ind + step 
                
                counter += -1            
                if counter <=1: 
                    frequency_bin[counter] = _size -1 - sum(frequency_bin) -ind_negative - ind_max
                    break
            else:
                ind = ind + step
        _time_2     = time.time()
        _del_time   = (_time_2 - _time_1)
        return  frequency_bin, _size, _del_time


    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """
    def hist_calculation_array_left(self, data, _preprocess = True, trop_lat=10, variable_1 = 'pr',  num_of_bins = None, step = None, first_edge = None,  
        width_of_bin  = None,   _add = None, *, start = []):
        """ 
            The function ***hist_calculation_array_left*** takes few arguments, ***data***, *** _preprocess***, ***variable_1***, 
            ***num_of_bins***, ***first_edge*** and ***width_of_bin***, 
            calculates the frequency histogram starting from the LEFT bing (normal order, slower)
                with the use of loop (package independent)
            and returns the dataset of frequency per bins, size of input Dataset or Dataarray, and 
            time consumed by function.  
            
            Args:
                data            (Dataset)   :   The Dataset.
                preprocess     (bool)       :   If ***_preprocess=True***, then function *** preprocessing *** converse the Dataset into DataArray.
                                                By definition, ***_preprocess=True***. 
                variable_1      (str)       :   Tropical latitudes borders. By definition,  *** trop_lat = self.trop_lat = 10***.
                num_of_bins     (int)       :   The number of bins in the histogram. By definition, ***num_of_bins = None***.
                step            (int)       :   The step in the loop. Helps to speed-up the calculation. 
                first_edge      (int)       :   The first edge of the first bin of the histogram. By definition, ***first_edge = None***. 
                width_of_bin    (int)       :   Histogram bin width. By definition, ***width_of_bin = None***.
            
            Return:
                Dataset                     :   frequency per bins.
                int                         :   Size input Dataset ot Dataarray
                float                       :   Time consumed by ***hist_calculation_array_left*** function. 
            
        """
        #print("hist_calculation function in the progress \n")

        _time_1 = time.time()

        if num_of_bins != None:
            self.num_of_bins = num_of_bins
        if step != None:
            self.step = step
        if first_edge != None:
            self.first_edge = first_edge
        if width_of_bin != None:
            self.width_of_bin = width_of_bin 
        if trop_lat != None:
            self.trop_lat = trop_lat

        bin_table = [self.first_edge + self.width_of_bin*j for j in range(0, self.num_of_bins)]

        frequency_table = np.zeros(self.num_of_bins) 
        frequency_bin =  xr.DataArray(frequency_table, coords=[bin_table], dims=["bin"])
        
        #if _add == True:
        #    frequency_bin += start
        if _preprocess == True:
            #print('preprocessing')
            if 'Dataset' in str(type(data)):
                for i in data._coord_names:
                    if 'lat' in i:
                        coord_lat = i
                    if 'lon' in i:
                        coord_lon = i 
            _temp = data.where(abs(data[coord_lat]) <= self.trop_lat, drop=True)     
            _temp = _temp[variable_1].stack(total=['time', coord_lat, coord_lon])
            data  = _temp.sortby(_temp)
            #data = self.preprocessing(data, _preprocess, variable_1) # ADD PARAMETERS

        # Its not necessary because this function only for DataArrays!
        if 'DataArray' in str(type(data)):
            _size = data.size
        elif 'Dataset' in str(type(data)): 
            #print("if you not want to pre-process data with the use of the class, \
            #    please, insert DataArray instead of Dataset")
            _names = list(data._coord_names)
            _size = 1
            for i in _names:
                _size *= data[i].size

        for ind in range(_size-1, 0, -1):
            if data[ind] <= bin_table[self.num_of_bins -1]:
                ind_max =  _size-1 - ind
                break
        
        #print(ind_max)
        counter = 1

        ind = 0
        iter = 0 
        _step =  self.step
        if data[0] < self.first_edge:
            while  True:  
                if data[ind] < self.first_edge: 
                    ind = ind + _step  
                else:   
                    _ind = ind 
                    while True:
                        _step = 0.5* _step
                        if  data[_ind] < self.first_edge and iter < 10:
                            _ind = int(_ind + _step)
                            iter += 1
                        elif data[_ind] >= self.first_edge and iter < 10:
                            _ind = int(_ind - _step)
                            iter += 1
                        else: break
                    break 
            ind_negative =  _ind 
            ind = _ind
        else:
            ind_negative =  0 
            ind = 0       
        #print(ind_negative, 'negative precipitations')

        # we not want to redefine the value  
        step = self.step

        while ind <= _size -1:   
            if data[ind] >= bin_table[counter]:
                iter = 0
                _step =  step
                _ind = ind 
                while True:
                    _step = 0.5* _step
                    if  data[int(_ind - _step)] >= bin_table[counter] and iter < 10 and _step >= 1:
                        _ind = int(_ind - _step)
                        iter += 1
                    elif data[int(_ind - _step)] < bin_table[counter] and iter < 10 and _step >= 1:
                        iter += 1
                    else: 
                        ind = _ind
                        break 
          
                frequency_bin[counter] = ind-sum(frequency_bin) - ind_negative 
                step = 1 + int(10**(-4) * frequency_bin[counter]) # max(1, 0.1*self.step)#1 + int(10**(-2) * frequency_bin[counter]))
                counter += 1
                ind = ind + step
                if counter > self.num_of_bins-1: 
                    break
                    #frequency_bin[counter] =  _size  - sum(frequency_bin) - ind_max
                    
      
            else:
                ind = ind + step 
        _time_2     = time.time()
        _del_time   = (_time_2 - _time_1)
        return  frequency_bin, _size, _del_time

    

    def hist_tr_pr_viridis(self,  _empty = False,  _label = None, _density = True):
        """
            ADD AND MODIFY
            ...
        """
        PR_trop_1d = self.ds_into_mm() 
        #_line_title = self.label #i.ds.name.split("/")
        if _empty == True:
            n, bins, patches = PR_trop_1d.plot(linewidth=2.9,
                facecolor = "None",   histtype=u'step',
                density=True, label=_label)
        else:
            n, bins, patches = self.ds.plot(edgecolor='black', linewidth=1.9, density=_density)
        n=[abs(math.log10(n[i])) for i in range(0, len(n))]
        for i in range(len(patches)):
            patches[i].set_facecolor(plt.cm.viridis(n[i]/max(n)))
        plt.yscale('log')
        #plt.close()
        #return 

        # For i amount of step
        # Function which compute the histogramn before ploting! 
        # Save the data
        # Probability to reset the hist values (bool parameters) 
        # Only after plot





