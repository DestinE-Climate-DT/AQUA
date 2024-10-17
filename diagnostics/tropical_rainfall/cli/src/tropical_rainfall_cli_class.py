import os
import re
import glob
import pandas as pd
import xarray as xr
from aqua import Reader
from aqua.util import get_arg, ConfigPath
from aqua.util import create_folder, add_pdf_metadata
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
        self.factor = config['plot']['factor']

        self.model = get_arg(args, 'model', config['data']['model'])
        self.exp = get_arg(args, 'exp', config['data']['exp'])
        self.source = get_arg(args, 'source', config['data']['source'])
        self.freq = get_arg(args, 'freq', config['data']['freq'])
        self.regrid = get_arg(args, 'regrid', config['data']['regrid'])
        self.loglevel = get_arg(args, 'loglevel', config['logger']['diag_loglevel'])
        self.reader_loglevel = get_arg(args, 'loglevel', config['logger']['reader_loglevel'])

        self.nproc = get_arg(args, 'nproc', config['compute_resources']['nproc'])
        self.xmax = get_arg(args, 'xmax', config['plot']['xmax'])

        machine = ConfigPath().get_machine()
        path_to_output = get_arg(args, 'outputdir', config['output'][machine])
        path_to_buffer = get_arg(args, 'bufferdir', config['buffer'][machine])

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

    def need_regrid_timmean(self, dataset):
        """Determines whether regridding or time averaging is needed for a dataset."""
        test_sample = dataset.isel(time=slice(1, 5))
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
        Calculates and saves histograms for each month within a specified year range.

        This function checks if histograms already exist in the specified output directory and decides whether to rebuild
        them based on the `rebuild_output` flag. It leverages the dataset to generate histograms by selecting data for
        each month, regridding, and calculating the time mean if necessary, and then saves the histogram files in the
        designated path. This process is logged, and any years not present in the dataset are flagged with a warning.

        Returns:
            None
        """
        # Retrieve the full dataset
        full_dataset = self.reader.retrieve(var=self.model_variable)
        regrid_bool, freq_bool = self.need_regrid_timmean(full_dataset)

        # Adjust year range based on the dataset
        self.s_year, self.f_year = adjust_year_range_based_on_dataset(dataset=full_dataset, start_year=self.s_year,
                                                                      final_year=self.f_year)

        # Determine the start and end months
        s_month = 1 if self.s_month is None else self.s_month
        f_month = 12 if self.f_month is None else self.f_month

        # Prepare the output directory
        path_to_output = os.path.join(self.path_to_buffer, f"{self.regrid}/{self.freq}/histograms/")
        create_folder(path_to_output)

        # Process data year by year
        for year in range(self.s_year, self.f_year + 1):
            self.logger.info(f"Processing year {year}...")

            data_per_year = full_dataset.sel(time=str(year))
            if data_per_year.time.size == 0:
                self.logger.warning(f"Year {year} is not present in the dataset. Skipping year.")
                continue

            # Process data month by month
            for month in range(s_month, f_month + 1):
                self._process_month(year=year, month=month, f_month=f_month, data_per_year=data_per_year,
                                    path_to_output=path_to_output, regrid_bool=regrid_bool, freq_bool=freq_bool)

        self.logger.info("All histograms have been calculated and saved.")

    def _process_month(self, *, year, month, f_month, data_per_year, path_to_output, regrid_bool, freq_bool):
        """
        Process the data for a specific month and generate a histogram.

        Args:
            year (int): The year being processed.
            month (int): The month being processed.
            data_per_year (xarray.Dataset): The dataset for the specific year.
            path_to_output (str): The path where histogram files should be saved.
            regrid_bool (bool): Whether to regrid the dataset.
            freq_bool (bool): Whether to calculate time mean.

        Returns:
            None
        """
        # Generate keys for file identification
        bins_info = self.diag.get_bins_info()
        keys = [f"{bins_info}_{year}-{month:02}", self.model, self.exp, self.regrid, self.freq]

        # Check for existing output and handle accordingly
        if self.rebuild_output:
            if self.diag.tools.find_files_with_keys(folder_path=path_to_output, keys=keys):
                self.logger.info(f"Rebuilding output for {year}-{month:02}...")
                self.diag.tools.remove_file_if_exists_with_keys(folder_path=path_to_output, keys=keys)
        elif self.diag.tools.find_files_with_keys(folder_path=path_to_output, keys=keys):
            self.logger.debug(f"Histogram for {year}-{month:02} already exists. Skipping.")
            return

        self.logger.debug(f"No existing output for {year}-{month:02}. Proceeding with data processing...")

        try:
            # Select the data for the current month
            data = data_per_year.sel(time=f"{year}-{month:02}")

            # Apply frequency adjustment and regridding if necessary
            if freq_bool:
                data = self.reader.timmean(data, freq=self.freq)
            if regrid_bool:
                data = self.reader.regrid(data)

            # Generate and save the histogram
            self.diag.histogram(data, model_variable=self.model_variable, path_to_histogram=path_to_output, threshold=30,
                                name_of_file=f"{self.regrid}_{self.freq}")
            self.logger.debug(f"Histogram for {year}-{month:02} saved at {path_to_output}.")
        except KeyError as e:
            self.logger.warning(f"KeyError for {year}-{month:02}: {e}")
        except Exception as e:
            self.logger.error(f"An unexpected error occurred for {year}-{month:02}: {e}")

        self.logger.debug(f"Month {month}/{f_month} processed for year {year}.")

    def check_files(self, folder_path: str, start_year: int, end_year: int) -> bool:
        """
        Check if files in the specified folder span the given year range.

        Args:
            folder_path (str): The path to the folder containing the files.
            start_year (int): The start year of the required period.
            end_year (int): The end year of the required period.

        Returns:
            bool: True if files that span the specified year range exist, otherwise False.
        """
        # Validate the input parameters
        if not isinstance(start_year, int) or not isinstance(end_year, int):
            self.logger.error("Start year and end year must be integers.")
            return False

        if start_year > end_year:
            self.logger.error("Start year cannot be greater than end year.")
            return False

        search_path = os.path.join(folder_path, '*.nc')
        self.logger.debug(f"Searching for files in: {search_path}")
        files = glob.glob(search_path)

        # Regular expression to match the specific date format in your filenames
        date_pattern = re.compile(r'(\d{4})-\d{2}-\d{2}T\d{2}')

        for file in files:
            # Check if the file has read permissions
            if os.access(file, os.R_OK):
                self.logger.debug(f"File {file} is accessible.")

                # Extract start and end years from filename
                matches = date_pattern.findall(os.path.basename(file))

                if matches:
                    file_start_year, file_end_year = matches[0], matches[-1]

                    # Check if the file spans the desired year range
                    if str(start_year) <= file_start_year and str(end_year) >= file_end_year:
                        self.logger.info(f"File {os.path.basename(file)} matches the year range {start_year}-{end_year}.")
                        return True
                    else:
                        self.logger.debug(f"File {os.path.basename(file)} does not match the year range {start_year}-{end_year}.")
                else:
                    self.logger.debug(f"No matching years found in the filename {os.path.basename(file)}.")
            else:
                self.logger.error(f"File {file} is not accessible (read permissions missing).")

        self.logger.info(f"No matching files found in {folder_path} for the period {start_year}-{end_year}.")
        return False

    def get_merged_histogram_for_source(self, source_info, default_interval=10):
        """
        Merges histogram data for a given source based on specified parameters or defaults.
        """
        folder_name = 'yearly_grouped' if self.s_month is None and self.f_month is None else 'monthly_grouped'
        folder_path = os.path.join(source_info['path'], self.regrid, self.freq, folder_name)
        self.logger.info(f"The path to {source_info['name']} data is {folder_path}")

        if not os.path.exists(folder_path):
            self.logger.error(
                f"Error: The folder for {source_info['name']} data with resolution '{self.regrid}' "
                f"and frequency '{self.freq}' does not exist."
                )
            return None

        start_year = self.s_year - default_interval if source_info.get('auto', False) else source_info.get('s_year', self.s_year)
        end_year = self.f_year + default_interval if source_info.get('auto', False) else source_info.get('f_year', self.f_year)

        if self.check_files(folder_path=folder_path, start_year=start_year, end_year=end_year):
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
            start_month=self.s_month, end_month=self.f_month, flag=bins_info
        )
        self.diag.dataset_to_netcdf(
            model_merged, path_to_netcdf=hist_path,
            name_of_file=f'histogram_{self.model}_{self.exp}_{self.regrid}_{self.freq}'
        )

        # Define sources with a loop for flexibility
        sources = {
            'MSWEP': {'path': self.mswep, 's_year': self.mswep_s_year, 'f_year': self.mswep_f_year, 'auto': self.mswep_auto},
            'IMERG': {'path': self.imerg, 's_year': self.imerg_s_year, 'f_year': self.imerg_f_year, 'auto': self.imerg_auto},
            'ERA5': {'path': self.era5, 's_year': self.era5_s_year, 'f_year': self.era5_f_year, 'auto': self.era5_auto}
        }

        merged_data_sources = {}
        for name, source in sources.items():
            # Add 'name' key to the source dictionary
            source['name'] = name
            merged_data = self.get_merged_histogram_for_source(source)
            if merged_data is not None:
                merged_data_sources[name] = merged_data
                self.diag.dataset_to_netcdf(
                    merged_data, path_to_netcdf=hist_path,
                    name_of_file=f"histogram_{name}_{self.regrid}_{self.freq}"
                )

        # Process histograms for each combination of pdf and pdfP flags
        for pdf, pdfP in [(True, False), (False, True)]:
            self.process_histograms(
                pdf_flag=pdf, pdfP_flag=pdfP, model_merged=model_merged,
                mswep_merged=merged_data_sources.get('MSWEP'),
                imerg_merged=merged_data_sources.get('IMERG'),
                era5_merged=merged_data_sources.get('ERA5')
            )

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
                f"{self.diag.tools.format_time(model_merged.time_band)}, against observations. "
                f"{self.diag.tools.format_lat_band(model_merged)}. "
            )
        else:
            description = (
                f"Comparison of the probability distribution function (PDF) multiplied by probability "
                f"(PDF*P) for precipitation data from {self.model} {self.exp}, measured in "
                f"{self.new_unit}, across the time range {self.diag.tools.format_time(model_merged.time_band)}, with observations. "
                f"{self.diag.tools.format_lat_band(model_merged)}. "
            )
        self.logger.debug('Description: %s', description)

        # Check if latitude bands match
        if model_merged is not None:
            if not self.diag.tools.verify_lat_band(model_merged, mswep_merged, "MSWEP", self.logger):
                return  # or use 'break' if this is within a loop
            if not self.diag.tools.verify_lat_band(model_merged, imerg_merged, "IMERG", self.logger):
                return  # or use 'break' if this is within a loop
            if not self.diag.tools.verify_lat_band(model_merged, era5_merged, "ERA5", self.logger):
                return  # or use 'break' if this is within a loop

        if model_merged is not None:
            add, _path_to_pdf = self.diag.histogram_plot(model_merged, figsize=self.figsize, new_unit=self.new_unit, pdf=pdf_flag,
                                                         pdfP=pdfP_flag, legend=legend_model, color=self.color, xmax=self.xmax,
                                                         plot_title=plot_title, loc=self.loc, path_to_pdf=self.path_to_pdf,
                                                         pdf_format=self.pdf_format, name_of_file=name_of_pdf, factor=self.factor)
            add_pdf_metadata(filename=_path_to_pdf, metadata_value=description, loglevel=self.loglevel)
        else:
            add = False  # Ensures that additional plots can be added to an existing plot if the model data is unavailable

        # Subsequent plots for each dataset
        datasets = [
            (mswep_merged, "MSWEP", self.mswep_color, self.mswep_factor),
            (imerg_merged, "IMERG", self.imerg_color, self.imerg_factor),
            (era5_merged, "ERA5", self.era5_color, self.era5_factor)
        ]

        for dataset, name, color, factor in datasets:
            if dataset is not None:
                self.logger.info(f"Plotting {name} data for comparison.")
                add, _path_to_pdf = self.diag.histogram_plot(dataset, figsize=self.figsize, new_unit=self.new_unit, add=add,
                                                             pdf=pdf_flag, pdfP=pdfP_flag, linewidth=1, linestyle=linestyle,
                                                             color=color, legend=name, xmax=self.xmax, loc=self.loc,
                                                             plot_title=plot_title, path_to_pdf=self.path_to_pdf,
                                                             pdf_format=self.pdf_format, name_of_file=name_of_pdf, factor=factor)
                description += (
                    f"The time range of {name} is {self.diag.tools.format_time(dataset.time_band)}. "
                    f"{self.diag.tools.format_lat_band(dataset)}. "
                )
                add_pdf_metadata(filename=_path_to_pdf, metadata_value=description, loglevel=self.loglevel)
            else:
                self.logger.warning(
                    f"{name} data with the necessary resolution is missing for comparison. "
                    "Check the data source or adjust the resolution settings. "
                )
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
            self.s_year, self.f_year = adjust_year_range_based_on_dataset(dataset=full_dataset, start_year=self.s_year,
                                                                          final_year=self.f_year)
            s_month = 1 if self.s_month is None else self.s_month
            f_month = 12 if self.f_month is None else self.f_month

            data_regrided = self.reader.regrid(full_dataset)

            path_to_output = self.path_to_buffer + f"{self.regrid}/{self.freq}/daily_variability/"
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
                f"{self.diag.tools.format_time(model_merged.time_band)}, against observations. "
                f"{self.diag.tools.format_lat_band(self.diag.tools.open_dataset(model_merged))}. "
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
                                             color=self.era5_color, add=add, linestyle='-', path_to_pdf=self.path_to_pdf,
                                             pdf_format=self.pdf_format, name_of_file=name_of_pdf)
            description = description + (
                f" The time range of ERA5 is {self.diag.tools.format_time(era5_merged.time_band)}. "
                f"{self.diag.tools.format_lat_band(self.diag.tools.open_dataset(era5_merged))}. "
            )
            add_pdf_metadata(filename=_path_to_pdf, metadata_value=description, loglevel=self.loglevel)
        else:
            self.logger.warning("Data appears to be not in hourly intervals. The CLI will not provide the plot of daily variability.")

    def average_profiles(self):
        """
        Calculate and plot the average precipitation profiles along latitude and longitude for the given model and compare
        them with MSWEP and ERA5 datasets.

        This function handles monthly or yearly data, regrids it if necessary, calculates the averages, and generates plots
        with detailed metadata.
        """
        if 'm' in self.freq.lower() or 'y' in self.freq.lower():
            self.logger.debug("Contains 'M' or 'Y'")

            plot_title, legend_model, name_of_pdf, output_path = self._prepare_plot_metadata()
            full_dataset = self._retrieve_and_prepare_dataset()

            model_average_path_lat = self.diag.average_into_netcdf(dataset=full_dataset, coord='lat', trop_lat=15,
                                                                   path_to_netcdf=output_path, name_of_file=f"{self.regrid}_{self.freq}")
            self.logger.debug(f"Model average path (lat): {model_average_path_lat}")
            add, description = self._plot_and_add_metadata(model_average_path=model_average_path_lat, plot_title=plot_title,
                                                           legend_model=legend_model, coord='lat')
            add, description = self._plot_comparisons(add=add, model_average_path=model_average_path_lat, dataset_name='MSWEP',
                                                      dataset_color=self.mswep_color, coord='lat', description=description)
            add, description = self._plot_comparisons(add=add, model_average_path=model_average_path_lat, dataset_name='ERA5',
                                                      dataset_color=self.era5_color, coord='lat', description=description)

            model_average_path_lon = self.diag.average_into_netcdf(dataset=full_dataset, coord='lon', trop_lat=90,
                                                                   path_to_netcdf=output_path, name_of_file=f"{self.regrid}_{self.freq}")
            self.logger.debug(f"Model average path (lon): {model_average_path_lon}")
            add, description = self._plot_and_add_metadata(model_average_path=model_average_path_lon, plot_title=plot_title,
                                                           legend_model=legend_model, coord='lon')
            add, description = self._plot_comparisons(add=add, model_average_path=model_average_path_lon, dataset_name='MSWEP',
                                                      dataset_color=self.mswep_color, coord='lon', description=description)
            add, description = self._plot_comparisons(add=add, model_average_path=model_average_path_lon, dataset_name='ERA5',
                                                      dataset_color=self.era5_color, coord='lon', description=description)
        else:
            self.logger.warning("Data appears to be not in monthly or yearly intervals.")
            self.logger.warning("The CLI will not provide the netcdf of average profiles.")

    def _prepare_plot_metadata(self):
        """
        Prepare the metadata required for plot creation including plot title, legend model, PDF name, and output path.

        Returns:
            tuple: containing plot title, legend model, name of PDF file, and output path.
        """
        plot_title = f"Grid: {self.regrid}, frequency: {self.freq}"
        legend_model = f"{self.model} {self.exp}"
        name_of_pdf = f"{self.model}_{self.exp}_{self.regrid}_{self.freq}"
        output_path = f"{self.path_to_netcdf}mean/"
        self.logger.debug(f"Plot title: {plot_title}")
        self.logger.debug(f"Output path: {output_path}")
        return plot_title, legend_model, name_of_pdf, output_path

    def _retrieve_and_prepare_dataset(self):
        """
        Retrieve and prepare the dataset for further analysis. It checks if regridding and frequency adjustments are needed,
        and adjusts the year range if required.

        Returns:
            Dataset: The prepared dataset after applying necessary transformations.
        """
        full_dataset = self.reader.retrieve(var=self.model_variable)
        regrid_bool, freq_bool = self.need_regrid_timmean(dataset=full_dataset)
        self.logger.debug(f"Regrid needed: {regrid_bool}, Frequency adjustment needed: {freq_bool}")

        self.s_year, self.f_year = adjust_year_range_based_on_dataset(dataset=full_dataset, start_year=self.s_year, final_year=self.f_year)
        self.logger.debug(f"Adjusted year range: {self.s_year} to {self.f_year}")

        if regrid_bool:
            full_dataset = self.reader.regrid(dataset=full_dataset)
            self.logger.debug("Dataset regridded.")
        return full_dataset

    def _plot_and_add_metadata(self, model_average_path, plot_title, legend_model, coord):
        """
        Plot the average precipitation profile and add relevant metadata to the output PDF.

        Args:
            model_average_path (str): Path to the NetCDF file containing the average profiles.
            plot_title (str): The title to be used for the plot.
            legend_model (str): The legend label for the model.
            coord (str): The coordinate direction, either 'lat' or 'lon'.

        Returns:
            tuple: Updated plot object and metadata description.
        """
        add = self.diag.plot_of_average(
            path_to_netcdf=model_average_path, trop_lat=90, path_to_pdf=self.path_to_pdf,
            color=self.color, figsize=self.figsize, new_unit=self.new_unit,
            legend=legend_model, plot_title=plot_title, loc=self.loc,
            name_of_file=f"{self.regrid}_{self.freq}"
        )
        _path_to_pdf = add[-1]
        self.logger.debug(f"Plot of average ({coord}) created. Path: {_path_to_pdf}")

        description = (
            f"Comparison of the average precipitation profiles along {coord} "
            f"from {self.model} {self.exp}, measured in {self.new_unit}, over the time range "
            f"{self.diag.tools.format_time(self.diag.tools.open_dataset(model_average_path).time_band)}, "
            f"against observations. {self.diag.tools.format_lat_band(self.diag.tools.open_dataset(model_average_path))}. "
        )
        add_pdf_metadata(filename=_path_to_pdf, metadata_value=description, loglevel=self.loglevel)
        self.logger.debug(f"PDF metadata added for {coord} plot.")
        return add, description

    def _get_dataset_path(self, dataset_name: str, coord: str) -> str:
        """
        Get the path to the dataset based on the dataset name and coordinate direction.

        Args:
            dataset_name (str): The name of the dataset, e.g., 'MSWEP' or 'ERA5'.
            coord (str): The coordinate direction, either 'lat' or 'lon'.

        Returns:
            str: The path to the dataset.
        """
        if dataset_name == 'MSWEP':
            base_path = f"{self.mswep}r100/M/mean/"
            if coord == 'lat':
                return f"{base_path}trop_rainfall_r100_M_lat_1979-02-01T00_2020-11-01T00_M.nc"
            else:  # coord == 'lon'
                return f"{base_path}trop_rainfall_r100_M_lon_1979-09-01T00_2020-11-01T00_M.nc"
        elif dataset_name == 'ERA5':
            base_path = f"{self.era5}r100/M/mean/"
            if coord == 'lat':
                return f"{base_path}trop_rainfall_r100_M_lat_1940-01-01T00_2023-12-01T06_M.nc"
            else:  # coord == 'lon'
                return f"{base_path}trop_rainfall_r100_M_lon_1940-09-01T00_2023-11-01T06_M.nc"
        else:
            raise ValueError(f"Unknown dataset name: {dataset_name}")

    def _plot_comparisons(self, add, model_average_path, dataset_name, dataset_color, coord, description):
        """
        Plot comparisons against MSWEP and ERA5 datasets and update the metadata.

        Args:
            add (object): The current plot object to add the comparisons to.
            model_average_path (str): Path to the model's average profiles NetCDF file.
            dataset_name (str): The name of the dataset for comparison, e.g., 'MSWEP' or 'ERA5'.
            dataset_color (str): The color to be used for the dataset in the plot.
            coord (str): The coordinate direction, either 'lat' or 'lon'.
            description (str): The current metadata description to be updated.

        Returns:
            tuple: Updated plot object and metadata description.
        """
        self.logger.info(f"Plotting {dataset_name} data for comparison.")

        path_to_dataset = self._get_dataset_path(dataset_name=dataset_name, coord=coord)

        add = self.diag.plot_of_average(
            path_to_netcdf=path_to_dataset, trop_lat=90, color=dataset_color, fig=add,
            legend=dataset_name, path_to_pdf=self.path_to_pdf, name_of_file=f"{self.regrid}_{self.freq}"
        )
        _path_to_pdf = add[-1]
        description += (
            f"The time range of {dataset_name} is {self.diag.tools.format_time(self.diag.tools.open_dataset(path_to_dataset).time_band)}. "
            f"{self.diag.tools.format_lat_band(self.diag.tools.open_dataset(path_to_dataset))}. "
        )
        add_pdf_metadata(filename=_path_to_pdf, metadata_value=description, loglevel=self.loglevel)
        self.logger.debug(f"PDF metadata added for {dataset_name} ({coord}).")
        return add, description
