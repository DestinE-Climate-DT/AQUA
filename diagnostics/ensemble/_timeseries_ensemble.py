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
# 4- plots the reference ERA5 timeseries from the aqua analysis: mean and std (monthly and annually)
# 5- include time range check
# 6- edit the labels

xr.set_options(keep_attrs=True)


class Ensemble_timeseries():
    def __init__(self,var=None,mon_model=None,mon_exp=None,mon_source=None,ann_model=None,ann_exp=None,ann_source=None,ref_mon_dict=None,ref_ann_dict=None,mon_startdate=None,mon_enddate=None,ann_startdate=None,ann_enddate=None,plot_kw={'ylim':{}},outdir='./',outfile=None,loglevel='WARNING'):
    
        self.loglevel = loglevel
        self.logger = log_configure(log_level=self.loglevel, log_name='Timeseries')
        self.var = var
 
        self.mon_model = mon_model
        self.mon_exp = mon_exp
        self.mon_source = mon_source
        
        self.ann_model = ann_model
        self.ann_exp = ann_exp
        self.ann_source = ann_source
        
        self.ref_mon_dict = ref_mon_dict
        self.ref_ann_dict = ref_ann_dict
        
        #if self.models is None or self.exps is None or self.sources is None:
        #    raise NoDataError("No models, exps or sources provided")

        #if isinstance(self.catalogs, str):
        #    self.catalogs = [self.catalogs]
        if isinstance(self.mon_model,str):
            self.mon_model = [self.mon_model]
        if isinstance(self.ann_model,str):
            self.ann_model = [self.ann_model]

        if isinstance(self.mon_exp,str):
            self.mon_exp = [self.mon_exp]
        if isinstance(self.ann_exp,str):
            self.ann_exp = [self.ann_exp]

        if isinstance(self.mon_source,str):
            self.mon_source = [self.mon_source]
        if isinstance(self.ann_source,str):
            self.ann_source = [self.ann_source]

        self.plot_kw = plot_kw
        #self.longname = longname
        #self.units = units
       
        self.save = False

        if self.save is False:
            self.logger.info("Figure will not ne saved")

        self.outdir = outdir
        self.outfile = outfile

        self.mon_dataset_mean = None
        self.mon_dataset_std = None
        self.mon_timeseries = None
        self.mon_data_label = None

        self.ann_dataset_mean = None
        self.ann_dataset_std = None
        self.ann_timeseries = None
        self.ann_data_label = None

        self.mon_startdate = mon_startdate
        self.mon_enddate = mon_enddate
        self.ann_startdate = ann_startdate
        self.ann_enddate = ann_enddate
        
    def run(self):
        self.retrieve_data()
        self.plot()
            
    def retrive_ref_data(self):
        pass


    def retrieve_data(self):
        self.logger.debug("Retrieving data")
        self.mon_dataset_mean = []
        self.mon_dataset_std = []
        self.mon_timeseries = []
        self.mon_data_label = []
        mon_startdate_list = []
        mon_enddate_list = []

        self.ann_dataset_mean = []
        self.ann_dataset_std = []
        self.ann_timeseries = []
        self.ann_data_label = []
        ann_startdate_list = []
        ann_enddate_list = []

        # You don't want to concatenate inside the loop -- that would make your code run in quadratic time: https://stackoverflow.com/questions/33435953/is-it-possible-to-append-to-an-xarray-dataseti

        for i, model in enumerate(self.mon_model):
            #self.logger.info(f'Retrieving data for {self.catalogs[i]} {model} {self.exp[i]} {self.source[i]}')
            self.logger.info(f'Retrieving data for {model} {self.mon_exp[i]} {self.mon_source[i]}')
            i_model = model;i_exp=self.mon_exp[i];i_source=self.mon_source[i];istart=self.mon_startdate;iend=self.mon_enddate
            #print('Data Reader: ', i_model,i_exp,i_source,istart,iend)
            try:
                reader = Reader(model=i_model,exp=i_exp,source=i_source,startdate=istart,enddate=iend,areas=False)
                #reader = Reader(model=model,exp=self.exp[i],source=self.source[i],startdate=self.startdate,enddate=self.enddate,area=False)
                data = reader.retrieve(self.var)
                self.mon_timeseries.append(data)
                self.mon_data_label.append(str(i_exp))
                mon_startdate_list.append(data.time[0].values)
                mon_enddate_list.append(data.time[-1].values)
            except Exception as e:
                self.logger.debug(f'Error while retrieving: {e}')
                #self.logger.warning(f'No data found for {self.catalog[i]} {model} {self.exp[i]} {self.source[i]}')
                self.logger.warning(f'No data found for {model} {self.mon_exp[i]} {self.mon_source[i]}')
        self.mon_startdate = max(mon_startdate_list)
        self.mon_enddate = min(mon_enddate_list)
        del mon_startdate_list
        del mon_enddate_list
        mon_combined_dataset = xr.concat(self.mon_timeseries,dim='Monthly Ensemble')
        mon_combined_dataset = mon_combined_dataset.sel(time=slice(self.mon_startdate,self.mon_enddate))
        self.mon_timeseries[i] = self.mon_timeseries[i][self.var].sel(time=slice(self.mon_startdate,self.mon_enddate))
        self.mon_dataset_mean = mon_combined_dataset[self.var].mean(dim='Monthly Ensemble')
        self.mon_dataset_std = mon_combined_dataset[self.var].std(dim='Monthly Ensemble')
        
        # Clean up
        del reader
        del data
        del mon_combined_dataset
        gc.collect()

        for i, model in enumerate(self.ann_model):
            #self.logger.info(f'Retrieving data for {self.catalogs[i]} {model} {self.exp[i]} {self.sources[i]}')
            self.logger.info(f'Retrieving data for {model} {self.ann_exp[i]} {self.ann_source[i]}')
            i_model = model;i_exp=self.ann_exp[i];i_source=self.ann_source[i];istart=self.ann_startdate;iend=self.ann_enddate
            #print('Data Reader: ', i_model,i_exp,i_source,istart,iend)
            try:
                reader = Reader(model=i_model,exp=i_exp,source=i_source,startdate=istart,enddate=iend,areas=False)
                #reader = Reader(model=model,exp=self.exp[i],source=self.sources[i],startdate=self.startdate,enddate=self.enddate,area=False)
                data = reader.retrieve(self.var)
                self.ann_timeseries.append(data)
                self.ann_data_label.append(str(i_exp))
                ann_startdate_list.append(data.time[0].values)
                ann_enddate_list.append(data.time[-1].values)
            except Exception as e:
                self.logger.debug(f'Error while retrieving: {e}')
                #self.logger.warning(f'No data found for {self.catalog[i]} {model} {self.exp[i]} {self.source[i]}')
                self.logger.warning(f'No data found for {model} {self.ann_exp[i]} {self.ann_source[i]}')
        self.ann_startdate = max(ann_startdate_list)
        self.ann_enddate = min(ann_enddate_list)
        del ann_startdate_list
        del ann_enddate_list
        ann_combined_dataset = xr.concat(self.ann_timeseries,dim='Annual Ensemble')
        ann_combined_dataset = ann_combined_dataset.sel(time=slice(self.ann_startdate,self.ann_enddate))
        self.ann_timeseries[i] = self.ann_timeseries[i][self.var].sel(time=slice(self.ann_startdate,self.ann_enddate))
        self.ann_dataset_mean = ann_combined_dataset[self.var].mean(dim='Annual Ensemble')
        self.ann_dataset_std = ann_combined_dataset[self.var].std(dim='Annual Ensemble')

        # Clean up
        del reader
        del data
        del ann_combined_dataset
        gc.collect()
        
        #startdate = time_to_string(startdate)
        #self.startdate = startdate
        #self.logger.debug(f'Start date: {self.startdate}')
        #enddate = time_to_string(startdate)
        #self.enddate = enddate
        #self.logger.debug('End date: {self.enddate}')

        if self.mon_dataset_mean.sizes == 0:
            raise NoDataError('No data found')
        if self.ann_dataset_mean.sizes == 0:
            raise NoDataError('No data found')

    def plot(self):
        self.logger.info('Plotting the timeseries')
        var = self.var[0] # Only one variable defined in the config file
        plt.rcParams["figure.figsize"] =  (10, 5)
        fig, ax = plt.subplots(1, 1)
        mon_data_label = self.mon_data_label
        mon_data_mean = self.mon_dataset_mean
        mon_data_mean = mon_data_mean[var]
        mon_data_std = self.mon_dataset_std
        mon_data_std = mon_data_std[var]
        mon_timeseries_data = self.mon_timeseries
        #self.dataset_mean[self.var].plot(ax=ax)
        mon_data_mean.plot(ax=ax,label='Monthly multimodel-mean')
        ax.fill_between(mon_data_mean.time, mon_data_mean -2.*mon_data_std,mon_data_mean +2.*mon_data_std,facecolor='grey',alpha=0.25,label='+-2std')
        for i in range(len(mon_data_label)):
            mon_timeseries_data[i][var].plot(ax=ax,label=mon_data_label[i])

        ann_data_label = self.ann_data_label
        ann_data_mean = self.ann_dataset_mean
        ann_data_mean = ann_data_mean[var]
        ann_data_std = self.ann_dataset_std
        ann_data_std = ann_data_std[var]
        ann_timeseries_data = self.ann_timeseries
        #self.dataset_mean[self.var].plot(ax=ax)
        ann_data_mean.plot(ax=ax,label='Annual multimodel-mean')
        ax.fill_between(ann_data_mean.time, ann_data_mean -2.*ann_data_std,ann_data_mean +2.*ann_data_std,facecolor='grey',alpha=0.25,label='+-2std')
        for i in range(len(ann_data_label)):
            ann_timeseries_data[i][var].plot(ax=ax,label=ann_data_label[i])

        #data_mean.plot(ax=ax)
        ax.legend(fontsize='small') 
        ax.figure.savefig('ensemble_timeseries.png')
        #    return fig,ax

    def plot_ref(self):
        pass

    def save_pdf(self):
        pass

    def save_netcdf(self):
        pass

    def _catalogs(self):
        pass
      
    def reference_data(self):
        pass

    def cleanup(self):
        pass


