import os
import pandas as pd
from aqua.util import get_arg
from aqua import Reader
from aqua.logger import log_configure
from dask.distributed import Client, LocalCluster
from tropical_rainfall import Tropical_Rainfall
from .tropical_rainfall_utils import adjust_year_range_based_on_dataset


class Tropical_Rainfall_CLI:
    def __init__(self, config, args):
        self.s_year = config['data']['s_year']
        self.f_year = config['data']['f_year']
        self.s_month = config['data']['s_month']
        self.f_month = config['data']['f_month']

        self.trop_lat = config['class_attributes']['trop_lat']
        self.num_of_bins = config['class_attributes']['num_of_bins']
        self.first_edge = config['class_attributes']['first_edge']
        self.width_of_bin = config['class_attributes']['width_of_bin']
        self.model_variable = config['class_attributes']['model_variable']
        self.new_unit = config['class_attributes']['new_unit']

        self.color = config['plot']['color']
        self.figsize = config['plot']['figsize']
        self.loc = config['plot']['loc']
        self.pdf_format = config['plot']['pdf_format']

        self.model = get_arg(args, 'model', config['data']['model'])
        self.exp = get_arg(args, 'exp', config['data']['exp'])
        self.source = get_arg(args, 'source', config['data']['source'])
        self.freq = get_arg(args, 'freq', config['data']['freq'])
        self.regrid = get_arg(args, 'regrid', config['data']['regrid'])
        self.loglevel = get_arg(args, 'loglevel', config['logger']['diag_loglevel'])
        reader_loglevel = get_arg(args, 'loglevel', config['logger']['reader_loglevel'])

        nproc = get_arg(args, 'nproc', config['compute_resources']['nproc'])
        machine = config['machine']
        path_to_output = get_arg(args, 'outputdir', config['output'][machine])
        self.xmax = get_arg(args, 'xmax', config['plot']['xmax'])

        self.mswep = config['mswep'][machine]
        self.mswep_s_year = config['mswep']['s_year']
        self.mswep_f_year = config['mswep']['f_year']
        self.mswep_auto = config['mswep']['auto']
        
        self.logger = log_configure(log_name="Trop. Rainfall CLI", log_level=self.loglevel)

        # Dask distributed cluster
        nworkers = get_arg(args, 'nworkers', None)
        if nworkers:
            cluster = LocalCluster(n_workers=nworkers, threads_per_worker=1)
            client = Client(cluster)
            self.logger.info(f"Running with {nworkers} dask distributed workers.")


        self.rebuild_output = config['rebuild_output']
        if path_to_output is not None:
            self.path_to_netcdf = os.path.join(path_to_output, f'netcdf/{self.model}_{self.exp}/')
            self.path_to_pdf = os.path.join(path_to_output, f'pdf/{self.model}_{self.exp}/')
        else:
            self.path_to_netcdf = self.path_to_pdf = None

        self.reader = Reader(model=self.model, exp=self.exp, source=self.source, loglevel=reader_loglevel, regrid=self.regrid,
                             nproc=nproc)
        self.diag = Tropical_Rainfall(trop_lat=self.trop_lat, num_of_bins=self.num_of_bins, first_edge=self.first_edge,
                                      width_of_bin=self.width_of_bin, loglevel=self.loglevel)

    def need_regrid_timmean(self, full_dataset):
        """Determines whether regridding or time averaging is needed for a dataset."""
        test_sample = full_dataset.isel(time=slice(1, 5))
        # Check for the need of regridding
        regrid_bool = False
        if isinstance(self.regrid, str):
            regrid_bool = self.diag.tools.check_need_for_regridding(test_sample, self.regrid)
        # Check for the need of time averaging
        freq_bool = False
        if isinstance(self.freq, str):
            freq_bool = self.diag.tools.check_need_for_time_averaging(test_sample, self.freq)
        return regrid_bool, freq_bool

    def calculate_histogram_by_months(self):
        """
        Calculates and saves histograms for each month within a specified year range. This function checks if histograms
        already exist in the specified output directory and decides whether to rebuild them based on the `rebuild_output` flag.
        It leverages the dataset to generate histograms by selecting data for each month, regridding and calculating the time mean
        if necessary, and then saves the histogram files in the designated path. This process is logged, and any years not present
        in the dataset are flagged with a warning.
        """
        full_dataset = self.reader.retrieve(var=self.model_variable)
        regrid_bool, freq_bool = self.need_regrid_timmean(full_dataset)

        self.s_year, self.f_year = adjust_year_range_based_on_dataset(full_dataset, start_year=self.s_year,
                                                                      final_year=self.f_year)
        s_month = 1 if self.s_month is None else self.s_month
        f_month = 12 if self.f_month is None else self.f_month

        for year in range(self.s_year, self.f_year+1):
            data_per_year = full_dataset.sel(time=str(year))
            if data_per_year.time.size != 0:
                for x in range(s_month, f_month+1):
                    path_to_output = self.path_to_netcdf+f"{self.regrid}/{self.freq}/histograms/"

                    bins_info = self.diag.get_bins_info()
                    keys = [f"{bins_info}_{year}-{x:02}", self.model, self.exp, self.regrid, self.freq]

                    # Check for file existence based on keys and decide on rebuilding
                    if self.rebuild_output and self.diag.tools.find_files_with_keys(folder_path=path_to_output, keys=keys):
                        self.logger.info("Rebuilding output...")
                        self.diag.tools.remove_file_if_exists_with_keys(folder_path=path_to_output, keys=keys)
                    elif not self.diag.tools.find_files_with_keys(folder_path=path_to_output, keys=keys):
                        self.logger.debug("No existing output. Proceeding with data processing...")
                        try:
                            data = data_per_year.sel(time=str(year)+'-'+str(x))
                            if freq_bool:
                                data = self.reader.timmean(data, freq=self.freq)
                            if regrid_bool:
                                data = self.reader.regrid(data)
                            self.diag.histogram(data, model_variable=self.model_variable,
                                                path_to_histogram=path_to_output,
                                                threshold = 30, name_of_file=f"{self.regrid}_{self.freq}")
                            self.logger.debug(f"The path to file is: {path_to_output}")
                        except KeyError:
                            pass
                        except Exception as e:
                                # Handle other exceptions
                                self.logger.error(f"An unexpected error occurred: {e}")
                    self.logger.debug(f"Current Status: {x}/{f_month} months processed in year {year}.")
            else:
                self.logger.warning("The specified year is not present in the dataset. " +
                            "Ensure the time range in your selection accurately reflects the available " +
                            "data. Check dataset time bounds and adjust your time selection parameters accordingly.")
        self.logger.info("The histograms are calculated and saved in storage.")
        return None

    def process_histograms(self, pdf_flag, pdfP_flag, model_merged, obs_merged, plot_color='tab:red', linestyle='-'):
        """
        Generates and saves histograms for model and observational data, with options for PDF and PDF*P plots.
        """
        plot_title = f"Grid: {self.regrid}, frequency: {self.freq}"
        legend = f"{self.model} {self.exp}"
        name_of_pdf = f"{self.model}_{self.exp}_{self.regrid}_{self.freq}"
        
        if 'm' in self.freq.lower() or 'y' in self.freq.lower():
            self.logger.debug("Contains 'M' or 'Y'. Setting xmax to 50 mm/day.")
            self.diag.xmax = 50

        add = self.diag.histogram_plot(model_merged, figsize=self.figsize, new_unit=self.new_unit, pdf=pdf_flag,
                                       pdfP=pdfP_flag, legend=legend, color=self.color, xmax=self.xmax,
                                       plot_title=plot_title, loc=self.loc, path_to_pdf=self.path_to_pdf,
                                       pdf_format=self.pdf_format, name_of_file=name_of_pdf)

        self.diag.histogram_plot(obs_merged, figsize=self.figsize, new_unit=self.new_unit, add=add, pdf=pdf_flag,
                                 pdfP=pdfP_flag, linewidth=1, linestyle=linestyle, color=plot_color,
                                 legend="MSWEP", xmax=self.xmax, loc=self.loc, plot_title=plot_title,
                                 path_to_pdf=self.path_to_pdf, pdf_format=self.pdf_format, name_of_file=name_of_pdf)

        self.logger.info("The histograms are plotted and saved in storage.")
    
    def plot_histograms(self):
        """
        Generates and saves histogram plots for the specified model, experiment, and source data over a defined period.
        The function constructs plot titles and legends based on model, experiment, and source details. It merges 
        histograms from specified paths and plots them, including comparative histograms with MSWEP data when available. 
        The absence of MSWEP data is handled gracefully with an error log.
        """
        self.logger.debug(f"The path to file is: {self.path_to_netcdf}{self.regrid}/{self.freq}/histograms/.")
        hist_path = f"{self.path_to_netcdf}{self.regrid}/{self.freq}/histograms/"
        model_merged = self.diag.merge_list_of_histograms(path_to_histograms=hist_path,
                                                        start_year=self.s_year, end_year=self.f_year,
                                                        start_month=self.s_month, end_month=self.f_month)

        if self.s_month is None and self.f_month is None:
            mswep_folder_path = os.path.join(self.mswep, self.regrid, self.freq, 'yearly_grouped')
        else:
            mswep_folder_path = os.path.join(self.mswep, self.regrid, self.freq, 'monthly_grouped')
        self.logger.info(f"The path to MSWEP data is  {mswep_folder_path}")
        if not os.path.exists(mswep_folder_path):
            self.logger.error(f"Error: The folder for MSWEP data with resolution '{self.regrid}' "
                            f"and frequency '{self.freq}' does not exist. Histograms for the "
                            "desired resolution and frequency have not been computed yet.")
            return
        if self.mswep_auto or (self.mswep_s_year is not None and self.mswep_f_year is not None):
            obs_interval = 10
            obs_merged = self.diag.merge_list_of_histograms(path_to_histograms=mswep_folder_path,
                                                            start_year=self.s_year-obs_interval, end_year=self.f_year+obs_interval,
                                                            start_month=self.s_month, end_month=self.f_month)
        else:
            obs_merged = self.diag.merge_list_of_histograms(path_to_histograms=mswep_folder_path,
                                                            start_year=self.mswep_s_year, end_year=self.mswep_f_year,
                                                            start_month=self.s_month, end_month=self.f_month)
        self.logger.info(f"The MSWEP data with resolution '{self.regrid}' and frequency '{self.freq}' are prepared for comparison.")
        
        pdf, pdfP = True, False # Set your PDF flag as needed
        self.process_histograms(pdf_flag=pdf, pdfP_flag=pdfP, model_merged=model_merged, obs_merged=obs_merged)
        pdf, pdfP = False, True  # Set your PDF flag as needed
        self.process_histograms(pdf_flag=pdfP, pdfP_flag=pdfP, model_merged=model_merged, obs_merged=obs_merged)
        
        self.logger.info("The Tropical Rainfall diagnostic is terminated.")