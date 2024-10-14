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
xr.set_options(keep_attrs=True)

class Ensemble_timeseries():
    def __init__(self,var=None,mon_model=None,mon_exp=None,mon_source=None,ann_model=None,ann_exp=None,ann_source=None,ref_mon_dict=None,ref_ann_dict=None,mon_startdate=None,mon_enddate=None,ann_startdate=None,ann_enddate=None,plot_kw={'ylim':{}},outdir=None,outfile=None,save=True,loglevel='WARNING'):
        """
        """
        self.loglevel = loglevel
        self.logger = log_configure(log_level=self.loglevel, log_name='Multi-model Timeseries')
        self.var = var
 
        self.mon_model = mon_model
        self.mon_exp = mon_exp
        self.mon_source = mon_source
        
        self.ann_model = ann_model
        self.ann_exp = ann_exp
        self.ann_source = ann_source
        
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

        self.ref_mon_dict = ref_mon_dict
        self.ref_ann_dict = ref_ann_dict
        self.ref_mon_data = None
        self.ref_ann_data = None
        if ref_mon_dict != {}:
            self.ref_mon_model = ref_mon_dict['model']
            self.ref_mon_exp = ref_mon_dict['exp']
            self.ref_mon_source = ref_mon_dict['source']
        if ref_ann_dict != {}:
            self.ref_ann_model = ref_ann_dict['model']
            self.ref_ann_exp = ref_ann_dict['exp']
            self.ref_ann_source = ref_ann_dict['source']
        
        self.plot_kw = plot_kw
        self.save = save
        if self.save is False:
            self.logger.info("Figure will not ne saved")
        self.outdir = outdir
        self.outfile = outfile

    def run(self):
        """
        """
        self.retrieve_data()
        self.retrieve_ref_data()
        self.plot()
            
    def retrieve_ref_data(self):
        """
        """
        if self.ref_mon_dict != {}:
            reader = Reader(model=self.ref_mon_model,exp=self.ref_mon_exp,source=self.ref_mon_source,startdate=self.mon_startdate,enddate=self.mon_enddate,areas=False)
            self.ref_mon_data = reader.retrieve(self.var)
            #self.ref_mon_data = self.ref_mon_data.to_dataarray(dim=self.var)
            self.ref_mon_data = self.ref_mon_data[self.var].sel(time=slice(self.mon_startdate,self.mon_enddate))
            del reader
            gc.collect()
        if self.ref_ann_dict != {}:
            reader = Reader(model=self.ref_ann_model,exp=self.ref_ann_exp,source=self.ref_ann_source,startdate=self.ann_startdate,enddate=self.ann_enddate,areas=False)
            self.ref_ann_data = reader.retrieve(self.var)
            #self.ref_ann_data = self.ref_ann_data.to_dataarray(dim=self.var)
            self.ref_ann_data = self.ref_ann_data[self.var].sel(time=slice(self.ann_startdate,self.ann_enddate))
            del reader
            gc.collect() 

    def retrieve_data(self):
        """
        """
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

        if self.mon_model != []:
            for i, model in enumerate(self.mon_model):
                #self.logger.info(f'Retrieving data for {self.catalogs[i]} {model} {self.exp[i]} {self.source[i]}')
                self.logger.info(f'Retrieving data for {model} {self.mon_exp[i]} {self.mon_source[i]}')
                i_model = model;i_exp=self.mon_exp[i];i_source=self.mon_source[i];istart=self.mon_startdate;iend=self.mon_enddate
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
        
        if self.ann_model != []:
            for i, model in enumerate(self.ann_model):
                #self.logger.info(f'Retrieving data for {self.catalogs[i]} {model} {self.exp[i]} {self.sources[i]}')
                self.logger.info(f'Retrieving data for {model} {self.ann_exp[i]} {self.ann_source[i]}')
                i_model = model;i_exp=self.ann_exp[i];i_source=self.ann_source[i];istart=self.ann_startdate;iend=self.ann_enddate
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

        if self.mon_model != [] and self.mon_dataset_mean.sizes == 0:
            raise NoDataError('No data found')
        if self.ann_model != [] and self.ann_dataset_mean.sizes == 0:
            raise NoDataError('No data found')

    def plot(self):
        """
        """
        self.logger.info('Plotting the timeseries')
        var = self.var[0] # Only one variable defined in the config file
        plt.rcParams["figure.figsize"] =  (10, 5)
        fig, ax = plt.subplots(1, 1)
        color_list = ["#1898e0", "#8bcd45", "#f89e13", "#d24493","#00b2ed", "#dbe622", "#fb4c27", "#8f57bf","#00bb62", "#f9c410", "#fb4865", "#645ccc"]
        if self.mon_model != []:
            mon_data_label = self.mon_data_label
            mon_data_mean = self.mon_dataset_mean
            mon_data_mean = mon_data_mean[var]
            mon_data_std = self.mon_dataset_std
            mon_data_std = mon_data_std[var]
            mon_timeseries_data = self.mon_timeseries
            #self.dataset_mean[self.var].plot(ax=ax)
            mon_data_mean.plot(ax=ax,label='Multimodel-mean-mon',color=color_list[0])
            ax.fill_between(mon_data_mean.time, mon_data_mean -2.*mon_data_std,mon_data_mean +2.*mon_data_std,facecolor=color_list[0],alpha=0.30,label='Multimodel-mean-mon'+r'$\pm2$std')
            for i in range(len(mon_data_label)):
                mon_timeseries_data[i][var].plot(ax=ax,label=mon_data_label[i]+'-mon',color=color_list[i+2],lw=0.5)
        
        if self.ann_model != []:
            ann_data_label = self.ann_data_label
            ann_data_mean = self.ann_dataset_mean
            ann_data_mean = ann_data_mean[var]
            ann_data_std = self.ann_dataset_std
            ann_data_std = ann_data_std[var]
            ann_timeseries_data = self.ann_timeseries
            #self.dataset_mean[self.var].plot(ax=ax)
            ann_data_mean.plot(ax=ax,label='Multimodel-mean-ann',color='grey',linestyle='--')
            ax.fill_between(ann_data_mean.time, ann_data_mean -2.*ann_data_std,ann_data_mean +2.*ann_data_std,facecolor='grey',alpha=0.20,label='Multimodel-mean-ann'+r'$\pm2$std')
            for i in range(len(ann_data_label)):
                ann_timeseries_data[i][var].plot(ax=ax,label=ann_data_label[i]+'-ann',color=color_list[i+2],lw=0.5,linestyle='--')
        
        if self.ref_mon_dict != {}:
            ref_mon_label = self.ref_mon_exp+' Monthly'
            self.ref_mon_data[var].plot(ax=ax,label=ref_mon_label,color='black',lw=0.6)
            ref_label = self.ref_mon_exp
        if self.ref_ann_dict != {}:
            ref_ann_label = self.ref_ann_exp+' Annual'
            self.ref_ann_data[var].plot(ax=ax,label=ref_ann_label,color='black',lw=0.6,linestyle='--')
            ref_label = self.ref_ann_exp
        if self.ref_mon_dict == {} and self.ref_ann_dict == {}:
            ref_label = 'no reference time series'
        ax.legend(ncol=4,fontsize=7.5,framealpha=0) 
        #ax.figure.savefig('ensemble_timeseries.png')
        if self.save:
            self.save_pdf(fig,ref_label)

    def save_pdf(self, fig,ref_label):
        """
        """
        self.logger.info("Saving figure to pdf")
        outfig = os.path.join(self.outdir, 'pdf')
        self.logger.debug(f"Saving figure to {outfig}")
        create_folder(outfig, self.loglevel)
        if self.outfile is None:
            self.outfile = f'multimodel_global_time_series_timeseries_{self.var}'
            self.outfile += '.pdf'
        self.logger.debug(f"Outfile: {self.outfile}")
        fig.savefig(os.path.join(outfig, self.outfile))

        description = "Time series of the global mean of"
        description += f" {self.var}"
        if self.mon_model != []: description += f" Monthly from {time_to_string(self.mon_startdate)} to {time_to_string(self.mon_enddate)}"
        if self.ann_model != []: description += f" Annual from {time_to_string(self.ann_startdate)} to {time_to_string(self.ann_enddate)}"
        if self.mon_model != []:
            for i, model in enumerate(self.mon_model):
                description += f" for Monthly {model} {self.mon_exp[i]} {self.mon_source[i]}"
        if self.ann_model != []:
            for i, model in enumerate(self.ann_model):
                description += f" for Annual {model} {self.ann_exp[i]} {self.ann_source[i]}"

        description += f" with {ref_label} as reference,"
        description += "."
        self.logger.debug(f"Description: {description}")
        add_pdf_metadata(filename=os.path.join(outfig, self.outfile),
                         metadata_value=description)


