import os
import re
import glob
import pandas as pd
from aqua.util import get_arg
from aqua import Reader
from aqua.util import create_folder
from aqua.logger import log_configure
from aqua.util import add_pdf_metadata
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
        self.factor = config['plot']['factor']

        self.model = get_arg(args, 'model', config['data']['model'])
        self.exp = get_arg(args, 'exp', config['data']['exp'])
        self.source = get_arg(args, 'source', config['data']['source'])
        self.freq = get_arg(args, 'freq', config['data']['freq'])
        self.regrid = get_arg(args, 'regrid', config['data']['regrid'])
        self.loglevel = get_arg(args, 'loglevel', config['logger']['diag_loglevel'])
        self.reader_loglevel = get_arg(args, 'loglevel', config['logger']['reader_loglevel'])

        self.nproc = get_arg(args, 'nproc', config['compute_resources']['nproc'])
        machine = config['machine']
        path_to_output = get_arg(args, 'outputdir', config['output'][machine])
        path_to_buffer = get_arg(args, 'bufferdir', config['buffer'][machine])

        self.xmax = get_arg(args, 'xmax', config['plot']['xmax'])

        self.mswep = config['mswep'][machine]
        self.mswep_s_year = config['mswep']['s_year']
        self.mswep_f_year = config['mswep']['f_year']
        self.mswep_auto = config['mswep']['auto']
        self.mswep_factor = config['mswep']['factor']
        self.mswep_color = config['mswep']['color']
        
        
        self.era5 = config['era5'][machine]
        self.era5_s_year = config['era5']['s_year']
        self.era5_f_year = config['era5']['f_year']
        self.era5_auto = config['era5']['auto']
        self.era5_factor = config['era5']['factor']
        self.era5_color = config['era5']['color']
        
        self.imerg = config['imerg'][machine]
        self.imerg_s_year = config['imerg']['s_year']
        self.imerg_f_year = config['imerg']['f_year']
        self.imerg_auto = config['imerg']['auto']
        self.imerg_factor = config['imerg']['factor']
        self.imerg_color = config['imerg']['color']
  
        self.logger = log_configure(log_name="Trop. Rainfall CLI", log_level=self.loglevel)

        # Dask distributed cluster
        nworkers = get_arg(args, 'nworkers', None)
        if nworkers:
            cluster = LocalCluster(n_workers=nworkers, threads_per_worker=1)
            client = Client(cluster)
            self.logger.info(f"Running with {nworkers} dask distributed workers.")


        self.rebuild_output = config['rebuild_output']
        if path_to_output is not None:
            create_folder(path_to_output)
            self.path_to_netcdf = os.path.join(path_to_output, f'netcdf/{self.model}_{self.exp}/')
            self.path_to_pdf = os.path.join(path_to_output, f'pdf/{self.model}_{self.exp}/')
            self.path_to_buffer = os.path.join(path_to_buffer, f'netcdf/{self.model}_{self.exp}/')
        else:
            self.path_to_netcdf = self.path_to_pdf = None

        self.reader = Reader(model=self.model, exp=self.exp, source=self.source, loglevel=self.reader_loglevel, regrid=self.regrid,
                             nproc=self.nproc)
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
        path_to_output = self.path_to_buffer+f"{self.regrid}/{self.freq}/histograms/"
        create_folder(path_to_output)
        for year in range(self.s_year, self.f_year+1):
            data_per_year = full_dataset.sel(time=str(year))
            if data_per_year.time.size != 0:
                for x in range(s_month, f_month+1):
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

    def check_files(self, folder_path, start_year, end_year):
        if self.s_month is None and self.f_month is None:
            search_path = os.path.join(folder_path, '..', '*.nc')
            self.logger.error(f"search_path {search_path}")
            files = glob.glob(search_path)
            self.logger.error(f"Files {files}")
            # Regular expression to match the specific date format in your filenames
            # Adjusted to extract the start and end years directly
            date_pattern = re.compile(r'(\d{4})-\d{2}-\d{2}T\d{2}')


            for file in files:
                # Extract start and end years from filename
                matches = date_pattern.findall(os.path.basename(file))
                
                if matches:
                    file_start_year, file_end_year = matches[0], matches[-1]  # First and last match should be start and end years
                    if str(start_year) == file_start_year and str(end_year) == file_end_year:
                        self.logger.error(f"File {os.path.basename(file)} correctly spans from {start_year} to {end_year}.")
                        self.logger.error(f"File {file}")
                        return file
                    else:
                        self.logger.debug(f"File {os.path.basename(file)} does not span the specified period from {start_year} to {end_year}.")
                else:
                    self.logger.debug(f"No matching years found in {os.path.basename(file)}.")
            return False

    def get_merged_histogram_for_source(self, source_info, default_interval=10):
        """
        Merges histogram data for a given source based on specified parameters or defaults.
        """
        folder_name = 'yearly_grouped' if self.s_month is None and self.f_month is None else 'monthly_grouped'
        folder_path = os.path.join(source_info['path'], self.regrid, self.freq, folder_name)
        self.logger.info(f"The path to {source_info['name']} data is {folder_path}")

        if not os.path.exists(folder_path):
            self.logger.error(f"Error: The folder for {source_info['name']} data with resolution '{self.regrid}' "
                              f"and frequency '{self.freq}' does not exist.")
            return None

        start_year = self.s_year - default_interval if source_info.get('auto', False) else source_info.get('s_year', self.s_year)
        end_year = self.f_year + default_interval if source_info.get('auto', False) else source_info.get('f_year', self.f_year)

        file = self.check_files(folder_path=folder_path, start_year=start_year, end_year=end_year)
        if file:
            return self.diag.tools.open_dataset(file)
        else:
            return self.diag.merge_list_of_histograms(
                path_to_histograms=folder_path,
                start_year=start_year, end_year=end_year,
                start_month=self.s_month, end_month=self.f_month
            )

    def plot_histograms(self):
        """
        Optimized method to handle the merging and plotting of histograms from multiple sources.
        """
        hist_path = f"{self.path_to_netcdf}histograms/"
        hist_buffer_path = f"{self.path_to_buffer}{self.regrid}/{self.freq}/histograms/"
        bins_info = self.diag.get_bins_info()
        model_merged = self.diag.merge_list_of_histograms(
            path_to_histograms=hist_buffer_path,
            start_year=self.s_year, end_year=self.f_year,
            start_month=self.s_month, end_month=self.f_month,
            flag=bins_info
        )
        self.diag.dataset_to_netcdf(model_merged, path_to_netcdf=hist_path, name_of_file=f'histogram_{self.model}_{self.exp}_{self.regrid}_{self.freq}')
        
        # Define source information for MSWEP, IMERG, ERA5
        sources = [
            {'name': 'MSWEP', 'path': self.mswep, 's_year': self.mswep_s_year, 'f_year': self.mswep_f_year, 'auto': self.mswep_auto},
            {'name': 'IMERG', 'path': self.imerg, 's_year': self.imerg_s_year, 'f_year': self.imerg_f_year, 'auto': self.imerg_auto},
            {'name': 'ERA5', 'path': self.era5, 's_year': self.era5_s_year, 'f_year': self.era5_f_year, 'auto': self.era5_auto}
        ]

        # Loop through each source and process
        for source in sources:
            merged_data = self.get_merged_histogram_for_source(source)
            if merged_data is not None:
                self.diag.dataset_to_netcdf(
                    merged_data, path_to_netcdf=hist_path,
                    name_of_file=f"histogram_{source['name']}_{self.regrid}_{self.freq}"
                )

        # Example call to process_histograms, set flags as necessary
        pdf, pdfP = True, False
        self.process_histograms(pdf_flag=pdf, pdfP_flag=pdfP, model_merged=model_merged,
                                mswep_merged=self.get_merged_histogram_for_source(sources[0]),
                                imerg_merged=self.get_merged_histogram_for_source(sources[1]),
                                era5_merged=self.get_merged_histogram_for_source(sources[2]))

        self.logger.info("The Tropical Rainfall diagnostic is terminated.")
        
    def process_histograms(self, pdf_flag, pdfP_flag, model_merged=None, mswep_merged=None, 
                       imerg_merged=None, era5_merged=None, linestyle='-'):
        """
        Generates and saves histograms for model and observational data, with options for PDF and PDF*P plots.
        Allows for some datasets to be None, in which case those plots are skipped.
        """
        plot_title = f"Grid: {self.regrid}, frequency: {self.freq}"
        legend_model = f"{self.model} {self.exp}"
        name_of_pdf = f"{self.model}_{self.exp}_{self.regrid}_{self.freq}"
        if pdf_flag:
            description = (
                f"Comparison of the probability distribution function (PDF) for precipitation data "
                f"from {self.model} {self.exp}, measured in {self.new_unit}, over the time range "
                f"{model_merged.time_band}, against observations."
            )
        else:
            description = (
                f"Comparison of the probability distribution function (PDF) multiplied by probability "
                f"(PDF*P) for precipitation data from {self.model} {self.exp}, measured in "
                f"{self.new_unit}, across the time range {model_merged.time_band}, with observations."
            )
        self.logger.debug('Description: %s', description)
                    
        # Initial plot with model data if it exists
        if model_merged is not None:
            add, _path_to_pdf = self.diag.histogram_plot(model_merged, figsize=self.figsize, new_unit=self.new_unit, pdf=pdf_flag,
                                        pdfP=pdfP_flag, legend=legend_model, color=self.color, xmax=self.xmax,
                                        plot_title=plot_title, loc=self.loc, path_to_pdf=self.path_to_pdf,
                                        pdf_format=self.pdf_format, name_of_file=name_of_pdf, factor=self.factor)
            add_pdf_metadata(filename=_path_to_pdf, metadata_value=description, loglevel = self.loglevel)
        else:
            add = False  # Ensures that additional plots can be added to an existing plot if the model data is unavailable

        # Subsequent plots for each dataset
        if mswep_merged is not None:
            add, _path_to_pdf = self.diag.histogram_plot(mswep_merged, figsize=self.figsize, new_unit=self.new_unit, add=add, pdf=pdf_flag,
                                    pdfP=pdfP_flag, linewidth=1, linestyle=linestyle, color=self.mswep_color,
                                    legend="MSWEP", xmax=self.xmax, loc=self.loc, plot_title=plot_title,
                                    path_to_pdf=self.path_to_pdf, pdf_format=self.pdf_format, name_of_file=name_of_pdf,
                                    factor=self.mswep_factor)
            description = description+f" The time range of MSWEP is {mswep_merged.time_band}."
            add_pdf_metadata(filename=_path_to_pdf, metadata_value=description, loglevel = self.loglevel)
            self.logger.info("Plotting MSWEP data for comparison.")
        else:
            self.logger.warning("MSWEP data with the necessary resolution is missing for comparison. Check the data source or adjust the resolution settings.")

        if imerg_merged is not None:
            add, _path_to_pdf = self.diag.histogram_plot(imerg_merged, figsize=self.figsize, new_unit=self.new_unit, add=add, pdf=pdf_flag,
                                    pdfP=pdfP_flag, linewidth=1, linestyle=linestyle, color=self.imerg_color,
                                    legend="IMERG", xmax=self.xmax, loc=self.loc, plot_title=plot_title,
                                    path_to_pdf=self.path_to_pdf, pdf_format=self.pdf_format, name_of_file=name_of_pdf,
                                    factor=self.imerg_factor)
            description = description+f" The time range of IMERG is {imerg_merged.time_band}."
            add_pdf_metadata(filename=_path_to_pdf, metadata_value=description, loglevel = self.loglevel)
            self.logger.info("Plotting IMERG data for comparison.")
        else:
            self.logger.warning("IMERG data with the necessary resolution is missing for comparison. Check the data source or adjust the resolution settings.")

        if era5_merged is not None:
            add, _path_to_pdf = self.diag.histogram_plot(era5_merged, figsize=self.figsize, new_unit=self.new_unit, add=add, pdf=pdf_flag,
                                    pdfP=pdfP_flag, linewidth=1, linestyle=linestyle, color=self.era5_color,
                                    legend="ERA5", xmax=self.xmax, loc=self.loc, plot_title=plot_title,
                                    path_to_pdf=self.path_to_pdf, pdf_format=self.pdf_format, name_of_file=name_of_pdf,
                                    factor=self.era5_factor)
            description = description+f" The time range of ERA5 is {era5_merged.time_band}."
            add_pdf_metadata(filename=_path_to_pdf, metadata_value=description, loglevel = self.loglevel)
            self.logger.info("Plotting ERA5 data for comparison.")
        else:
            self.logger.warning("ERA5 data with the necessary resolution is missing for comparison. Check the data source or adjust the resolution settings.")
        self.logger.info("Histogram plots (as available) have been generated and saved.")
    
    def daily_variability(self):
        """
        Evaluates the daily variability of the dataset based on the specified model variable and frequency.
        This method specifically processes datasets with an hourly frequency ('h' or 'H') by slicing the first
        and last weeks of data within the defined start and final year and month range. It supports optional
        regridding of the data for these periods. The method adds localized time information to the sliced
        datasets and saves this information for further diagnostic analysis.
        """
        if 'h' in self.freq.lower():
            self.logger.debug("Contains 'h' or 'H'")
            self.reader = Reader(model=self.model, exp=self.exp, source=self.source, loglevel=self.reader_loglevel, regrid='r100',
                                nproc=self.nproc)
            full_dataset = self.reader.retrieve(var=self.model_variable)
            self.s_year, self.f_year = adjust_year_range_based_on_dataset(full_dataset, start_year=self.s_year,
                                                                            final_year=self.f_year)
            s_month = 1 if self.s_month is None else self.s_month
            f_month = 12 if self.f_month is None else self.f_month
            
            data_regrided = self.reader.regrid(full_dataset)
            
            path_to_output = self.path_to_buffer+f"{self.regrid}/{self.freq}/daily_variability/"
            create_folder(path_to_output)
            for year in range(self.s_year, self.f_year+1):
                data_per_year = data_regrided.sel(time=str(year))
                if data_per_year.time.size != 0:
                    for x in range(s_month, f_month+1):
                        keys = [f"{year}-{x:02}", self.model, self.exp, self.regrid, self.freq]

                        # Check for file existence based on keys and decide on rebuilding
                        if self.rebuild_output and self.diag.tools.find_files_with_keys(folder_path=path_to_output, keys=keys):
                            self.logger.info("Rebuilding output...")
                            self.diag.tools.remove_file_if_exists_with_keys(folder_path=path_to_output, keys=keys)
                        elif not self.diag.tools.find_files_with_keys(folder_path=path_to_output, keys=keys):
                            self.logger.debug("No existing output. Proceeding with data processing...")
                            try:
                                data = data_per_year.sel(time=str(year)+'-'+str(x))
                                self.diag.add_localtime(data, path_to_netcdf=path_to_output,
                                                        name_of_file=f"{self.regrid}_{self.freq}", 
                                                        new_unit="mm/hr")
                            except KeyError:
                                pass
                            except Exception as e:
                                    # Handle other exceptions
                                    self.logger.error(f"An unexpected error occurred: {e}")
                        self.logger.debug(f"Current Status: {x}/{f_month} months processed in year {year}.")
        else:
            self.logger.warning("Data appears to be not in hourly intervals. The CLI will not provide the plot of daily variability.")    
            
    def plot_daily_variability(self):
        if 'h' in self.freq.lower():
            self.logger.debug("Contains 'h' or 'H'")
            legend = f"{self.model} {self.exp}"
            name_of_pdf =f"{self.model}_{self.exp}"
            
            output_path = f"{self.path_to_netcdf}daily_variability/"
            output_buffer_path = f"{self.path_to_buffer}{self.regrid}/{self.freq}/daily_variability/"
            
            create_folder(output_path)
            create_folder(output_buffer_path)
            
            model_merged = self.diag.merge_list_of_daily_variability(
                path_to_output=output_buffer_path,
                start_year=self.s_year, end_year=self.f_year,
                start_month=self.s_month, end_month=self.f_month
            )
            filename = self.diag.dataset_to_netcdf(model_merged, path_to_netcdf=output_path, 
                                                name_of_file=f'daily_variability_{self.model}_{self.exp}_{self.regrid}_{self.freq}')
            
            add, _path_to_pdf = self.diag.daily_variability_plot(path_to_netcdf=filename, legend=legend, new_unit=self.new_unit,
                                                    trop_lat=90, relative=False, color=self.color,
                                                    linestyle='-', path_to_pdf=self.path_to_pdf, pdf_format=self.pdf_format,
                                                    name_of_file=name_of_pdf)
            description = (
                f"Comparison of the daily variability of the precipitation data "
                f"from {self.model} {self.exp}, measured in {self.new_unit}, over the time range "
                f"{model_merged.time_band}, against observations."
            )
            add_pdf_metadata(filename=_path_to_pdf, metadata_value=description, loglevel = self.loglevel)
            path_to_era5 = f"{self.era5}r100/H/daily_variability"
            era5_merged = self.diag.merge_list_of_daily_variability(
                path_to_output=path_to_era5,
                start_year=self.s_year, end_year=self.f_year,
                start_month=self.s_month, end_month=self.f_month
            )
            if not os.path.exists(path_to_era5):
                self.logger.error(f"The data is exist for compatison")
                return
            filename_era5 = self.diag.dataset_to_netcdf(era5_merged, path_to_netcdf=output_path, name_of_file=f'daily_variability_era5')
            self.diag.daily_variability_plot(path_to_netcdf=filename_era5, legend='ERA5', relative=False, new_unit=self.new_unit,
                                                   color=self.era5_color, add=add,
                                                   linestyle='-', path_to_pdf=self.path_to_pdf, pdf_format=self.pdf_format,
                                                   name_of_file=name_of_pdf)
            description = description+f" The time range of ERA5 is {era5_merged.time_band}."
            add_pdf_metadata(filename=_path_to_pdf, metadata_value=description, loglevel = self.loglevel)
        else:
            self.logger.warning("Data appears to be not in hourly intervals. The CLI will not provide the plot of daily variability.")


    def average_profiles(self):
        if 'm' in self.freq.lower() or 'y' in self.freq.lower():
            self.logger.debug("Contains 'M' or 'Y'")
            plot_title = f"Grid: {self.regrid}, frequency: {self.freq}"
            legend_model = f"{self.model} {self.exp}"
            name_of_pdf = f"{self.model}_{self.exp}_{self.regrid}_{self.freq}"
            output_path = f"{self.path_to_netcdf}mean/"
            
            full_dataset = self.reader.retrieve(var=self.model_variable)
            regrid_bool, freq_bool = self.need_regrid_timmean(full_dataset)
            self.s_year, self.f_year = adjust_year_range_based_on_dataset(full_dataset, start_year=self.s_year,
                                                                          final_year=self.f_year)
            if regrid_bool:
                full_dataset = self.reader.regrid(full_dataset)
            
            model_average_path_lat = self.diag.average_into_netcdf(full_dataset,
                                                                   path_to_netcdf=output_path,
                                                                   name_of_file=f"{self.regrid}_{self.freq}")
            model_average_path_lon = self.diag.average_into_netcdf(full_dataset, coord='lon', trop_lat=90,
                                                                   path_to_netcdf=output_path,
                                                                   name_of_file=f"{self.regrid}_{self.freq}")
            add = self.diag.plot_of_average(path_to_netcdf=model_average_path_lat, trop_lat=90,
                                      path_to_pdf=self.path_to_pdf, color=self.color,
                                      figsize=self.figsize, new_unit=self.new_unit, legend=legend_model,
                                      plot_title=plot_title, loc=self.loc,
                                      name_of_file=f"{self.regrid}_{self.freq}")
            _path_to_pdf = add[-1]
            description = (
                f"Comparison of the average precipitation profiles along latitude"
                f"from {self.model} {self.exp}, measured in {self.new_unit}, over the time range "
                f"{self.diag.tools.open_dataset(model_average_path_lat).time_band}, against observations."
            )
            add_pdf_metadata(filename=_path_to_pdf, metadata_value=description, loglevel = self.loglevel)
            
            path_to_mswep = f"{self.mswep}r100/M/mean/trop_rainfall_r100_M_lat_1979-02-01T00_2020-11-01T00_M.nc"
            add = self.diag.plot_of_average(path_to_netcdf=path_to_mswep, 
                                      trop_lat=90, color=self.mswep_color, fig=add,
                                      legend="MSWEP",
                                      path_to_pdf=self.path_to_pdf,
                                      name_of_file=f"{self.regrid}_{self.freq}")
            description = description+f" The time range of MSWEP is {self.diag.tools.open_dataset(path_to_mswep).time_band}."
            add_pdf_metadata(filename=_path_to_pdf, metadata_value=description, loglevel = self.loglevel)
            
            path_to_era5 = f"{self.era5}r100/M/mean/trop_rainfall_r100_M_lat_1940-01-01T00_2023-12-01T06_M.nc"
            add = self.diag.plot_of_average(path_to_netcdf=path_to_era5, 
                                      trop_lat=90, color=self.era5_color, fig=add,
                                      legend="ERA5",
                                      path_to_pdf=self.path_to_pdf,
                                      name_of_file=f"{self.regrid}_{self.freq}")
            description = description+f" The time range of ERA5 is {self.diag.tools.open_dataset(path_to_era5).time_band}."
            add_pdf_metadata(filename=_path_to_pdf, metadata_value=description, loglevel = self.loglevel)
                  
                                        
            add = self.diag.plot_of_average(path_to_netcdf=model_average_path_lon, trop_lat=90,
                                      path_to_pdf=self.path_to_pdf, color=self.color,
                                      figsize=self.figsize, new_unit=self.new_unit, legend=legend_model,
                                      plot_title=plot_title, loc=self.loc,
                                      name_of_file=f"{self.regrid}_{self.freq}")
            _path_to_pdf = add[-1]
            description = (
                f"Comparison of the average precipitation profiles along longitude"
                f"from {self.model} {self.exp}, measured in {self.new_unit}, over the time range "
                f"{self.diag.tools.open_dataset(model_average_path_lat).time_band}, against observations."
            )
            add_pdf_metadata(filename=_path_to_pdf, metadata_value=description, loglevel = self.loglevel)
            path_to_mswep = f"{self.mswep}r100/M/mean/trop_rainfall_r100_M_lon_1979-09-01T00_2020-11-01T00_M.nc"
            add = self.diag.plot_of_average(path_to_netcdf=path_to_mswep, 
                                      trop_lat=90, color=self.mswep_color, fig=add,
                                      legend="MSWEP",
                                      path_to_pdf=self.path_to_pdf,
                                      name_of_file=f"{self.regrid}_{self.freq}")
            description = description+f" The time range of MSWEP is {self.diag.tools.open_dataset(path_to_mswep).time_band}."
            add_pdf_metadata(filename=_path_to_pdf, metadata_value=description, loglevel = self.loglevel)
            
            path_to_imerg = f"{self.imerg}r100/M/mean/trop_rainfall_r100_M_lon_2000-09-01T00_2022-11-01T00_M.nc"
            add = self.diag.plot_of_average(path_to_netcdf=path_to_imerg, 
                                            trop_lat=90, color=self.imerg_color, fig=add,
                                            legend="IMERG",
                                            path_to_pdf=self.path_to_pdf,
                                            name_of_file=f"{self.regrid}_{self.freq}")
            description = description+f" The time range of IMERG is {self.diag.tools.open_dataset(path_to_imerg).time_band}."
            add_pdf_metadata(filename=_path_to_pdf, metadata_value=description, loglevel = self.loglevel)

            path_to_era5 = f"{self.era5}r100/M/mean/trop_rainfall_r100_M_lon_1940-09-01T00_2023-11-01T06_M.nc"
            add = self.diag.plot_of_average(path_to_netcdf=path_to_era5, 
                                      trop_lat=90, color=self.era5_color, fig=add,
                                      legend="ERA5",
                                      path_to_pdf=self.path_to_pdf,
                                      name_of_file=f"{self.regrid}_{self.freq}")
            description = description+f" The time range of ERA5 is {self.diag.tools.open_dataset(path_to_era5).time_band}."
            add_pdf_metadata(filename=_path_to_pdf, metadata_value=description, loglevel = self.loglevel)
        else:
            self.logger.warning("Data appears to be not in monthly or yearly intervals.") 
            self.logger.warning("The CLI will not provide the netcdf of average profiles.")

