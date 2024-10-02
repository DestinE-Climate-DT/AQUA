import os
import gc

import xarray as xr 
from aqua import Reader
from aqua.logger import log_configure
from aqua.exceptions import NoObservationError, NoDataError
from aqua.util import eval_formula,create_folder
from aqua.util import add_pdf_metadata, time_to_string
from aqua.graphics import plot_timeseries
import matplotlib.pyplot as plt

#from .reference_data import get_referrnce_timeseries

# 1- Compute the mean and std of the timeseries coming the aqua analysis
# 2- plot the computed statistics 
# 3- plots the individual timeseries
# 4- plots the reference ERA5 timeseries from the aqua analysis

xr.set_options(keep_attrs=True)


class Ensemble_timeseries():
    def __init__(self,var=None,models=None,exps=None,sources=None,startdate=None,enddate=None,plot_kw={'ylim':{}},longname=None,save=True,outdir='./',outfile=None,loglevel='WARNING'):
    
    #def __init__(self,var=None,catalogs=None,models=None,exps=None,sources=None,startdate=None,enddate=None,plot_kw={'ylim':{}},longname=None,save=True,outdir='./',outfile=None,loglevel='WARNING'):
    

        self.loglevel = loglevel
        #self.formula = formula
        self.logger = log_configure(log_level=self.loglevel, log_name='Timeseries')
        self.var = var
        self.models = models
        self.exps = exps
        self.sources= sources
        #self._catalogs(catalogs=catalogs)
        #self.catalogs = catalogs

        if self.models is None or self.exps is None or self.sources is None:
            raise NoDataError("No models, exps or sources provided")

        #if isinstance(self.catalogs, str):
        #    self.catalogs = [self.catalogs]
        if isinstance(self.models,str):
            self.models = [self.models]
        if isinstance(self.exps,str):
            self.exps = [self.exps]
        if isinstance(self.sources,str):
            self.sources = [self.sources]
        #self.regrid = False
        self.startdate = startdate
        self.enddate = enddate

        self.plot_kw = plot_kw
        self.longname = longname
        #self.units = units
       
        self.save = save

        if self.save is False:
            self.logger.info("Figure will not ne saved")

        self.outdir = outdir
        self.outfile = outfile
        self.dataset_mean = None
        self.dataset_std = None
        self.timeseries = None
        self.data_label = None
        
    def run(self):
        self.retrieve_data()
        self.plot()
            

    def retrieve_data(self):
        self.logger.debug("Retrieving data")
        self.dataset_mean = []
        self.dataset_std = []
        self.timeseries = []
        self.data_label = []
        # You don't want to concatenate inside the loop -- that would make your code run in quadratic time: https://stackoverflow.com/questions/33435953/is-it-possible-to-append-to-an-xarray-dataseti

        for i, model in enumerate(self.models):
            #self.logger.info(f'Retrieving data for {self.catalogs[i]} {model} {self.exps[i]} {self.sources[i]}')
            self.logger.info(f'Retrieving data for {model} {self.exps[i]} {self.sources[i]}')
            i_model = model;i_exp=self.exps[i];i_source=self.sources[i];istart=self.startdate;iend=self.enddate
            #print('Data Reader: ', i_model,' ',i_exp,' ',i_source,' ',istart,iend)
            try:
                reader = Reader(model=i_model,exp=i_exp,source=i_source,startdate=istart,enddate=iend,areas=False)
                #reader = Reader(model=model,exp=self.exps[i],source=self.sources[i],startdate=self.startdate,enddate=self.enddate,area=False)
                data = reader.retrieve(self.var)
                self.timeseries.append(data)
                self.data_label.append(str(i_exp))
            except Exception as e:
                self.logger.debug(f'Error while retrieving: {e}')
                #self.logger.warning(f'No data found for {self.catalog[i]} {model} {self.exps[i]} {self.sources[i]}')
                self.logger.warning(f'No data found for {model} {self.exps[i]} {self.sources[i]}')
        combined_dataset = xr.concat(self.timeseries,dim='Ensemble')
        self.dataset_mean = combined_dataset[self.var].mean(dim='Ensemble')
        self.dataset_std = combined_dataset[self.var].std(dim='Ensemble')

        # Clean up
        del reader
        del data
        #del dataset
        del combined_dataset
        gc.collect()
        
        #startdate = time_to_string(startdate)
        #self.startdate = startdate
        #self.logger.debug(f'Start date: {self.startdate}')
        #enddate = time_to_string(startdate)
        #self.enddate = enddate
        #self.logger.debug('End date: {self.enddate}')

        if self.dataset_mean.sizes == 0:
            raise NoDataError('No data found')

    def plot(self):
        fig, ax = plt.subplots(1, 1)
        self.logger.info('Plotting the timeseries')
        data_label = self.data_label
        ###### Only works with one var from the config file
        var = self.var[0]
        data_mean = self.dataset_mean
        data_mean = data_mean[var]
        data_std = self.dataset_std
        data_std = data_std[var]
        timeseries_data = self.timeseries
        #self.dataset_mean[self.var].plot(ax=ax)
        data_mean.plot(ax=ax,label='multimodel-mean')
        ax.fill_between(data_mean.time, data_mean -2.*data_std,data_mean +2.*data_std,facecolor='grey',alpha=0.25,label='+-std')
        for i in range(len(data_label)):
            timeseries_data[i][var].plot(ax=ax,label=data_label[i])
        #data_mean.plot(ax=ax)
        ax.legend(fontsize='small') 
        ax.figure.savefig('ens.png')
        #    return fig,ax

    def save_pdf(self):
        pass

    def save_netcdf(self):
        pass

    def check_range(self):
        pass

    def _catalogs(self):
        pass
      
    def reference_data(self):
        pass

    def cleanup(self):
        pass


