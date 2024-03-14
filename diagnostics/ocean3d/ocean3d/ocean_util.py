"""
Common Ocean modules
"""

import os
import xarray as xr
import numpy as np
from aqua import Reader
from aqua.exceptions import NoObservationError
from aqua.util import find_vert_coord, load_yaml, add_pdf_metadata
import matplotlib.pyplot as plt
from aqua.logger import log_configure


def kelvin_to_celsius(data, variable_name, loglevel= "WARNING"):
    """
    Convert temperature in Kelvin to degrees Celsius for a specific variable in an xarray dataset.

    Parameters:
    - data (xarray.Dataset): The input xarray dataset containing temperature data.
    - variable_name (str): The name of the variable to convert from Kelvin to degrees Celsius.
    - loglevel (str, optional): The log level for messages (default is "WARNING").

    Returns:
    - xarray.Dataset: the specified variable converted to degrees Celsius.
    """
    logger = log_configure(loglevel, 'Unit')
    # Check if the variable exists in the dataset
    if data[variable_name].attrs['units']== 'K' or 'kelvin':
        logger.warning("The unit of Pot. Temperature is Kelvin. Converting to degC")
        # Convert Kelvin to Celsius: Celsius = Kelvin - 273.15
        data[variable_name] -= 273.15
        data[variable_name].attrs['units']= 'degC'
    return data

def check_variable_name(data, loglevel= "WARNING"):
    """
    Check and select specific variables required for diagnostics from an xarray dataset.

    Parameters:
    - data (xarray.Dataset): The input xarray dataset.
    - loglevel (str, optional): The log level for messages (default is "WARNING").

    Returns:
    - xarray.Dataset: The modified xarray dataset with selected variables and renamed dimensions.
    """
    logger = log_configure(loglevel, 'Check Variables')
    vars = list(data.variables)
    required_vars= []
    var_list= ["SO","avg_so","thetao","THETAO","avg_SO","avg_so","avg_thetao","avg_THETAO",
               "toce_mean","soce_mean"]
    for var in vars:
        if var in var_list:
            required_vars.append(var)
    if required_vars != []:
        logger.debug("This are the variables %s available for the diags in the catalogue.", required_vars)
        # data = data[required_vars]
        logger.debug("Selected this variables")
        for var in required_vars:
            if 'avg_so' in var.lower() or 'soce' in var.lower():
                data = data.rename({var: "avg_so"})
                logger.debug("renaming %s as avg_so", var)
            if 'thetao' in var.lower() or 'toce' in var.lower():
                data = data.rename({var: "avg_thetao"})
                logger.debug("renaming %s as avg_thetao", var)
    else:
        raise ValueError("Required variable avg_so and avg_thetao is not available in the catalogue")
    vertical_coord = find_vert_coord(data)[0]
    data = data.rename({vertical_coord: "lev"})
    data = kelvin_to_celsius(data, "avg_thetao")
    # if "thetao_uncertainty" in data:
    #     data = kelvin_to_celsius(data, "thetao_uncertainty")
    return data

def time_slicing(data, start_year, end_year, loglevel= "WARNING"):
    """
    Slice the time dimension of an xarray dataset to select data within a specified time range.

    Parameters:
    - data (xarray.Dataset): The input xarray dataset.
    - start_year (int): The start year of the time range.
    - end_year (int): The end year of the time range.
    - loglevel (str, optional): The log level for messages (default is "WARNING").

    Returns:
    - xarray.Dataset: The xarray dataset with time dimension sliced to the specified range.
    """
    logger = log_configure(loglevel, 'time_slicing')
    data = data.sel(time=slice(str(start_year),str(end_year)))
    logger.debug("Selected the data for the range of %s and %s", start_year, end_year)
    return data

def process_region_config(regions_yaml):
    """
    Converts keys at the top level of the dictionary to lowercase and removes spaces, underscores, and dashes.
    """
    if isinstance(regions_yaml, dict):
        return {key.replace(" ", "").replace("_", "").replace("-", "").lower(): v for key, v in regions_yaml.items()}
    else:
        return regions_yaml

def predefined_regions(region, loglevel= "WARNING"):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    processed_region = region.replace(" ", "").replace("_","").replace("-","").lower()
    regions_yaml = f"{current_dir}/../config/regions.yaml"
    regions_dict = load_yaml(regions_yaml)
    try:
        regions_dict = process_region_config(regions_dict["regions"])
        region_boundary = regions_dict[processed_region]
        lat_n = region_boundary.get("LatN")
        lat_s = region_boundary.get("LatS")
        lon_e = region_boundary.get("LonE")
        lon_w = region_boundary.get("LonW")
    except KeyError:
        raise ValueError(
            f"Invalid region name: {region}. Check the region name in config file or update it: {regions_yaml}")

    return lat_s, lat_n, lon_w, lon_e


def convert_longitudes(data, loglevel= "WARNING"):
    """
    Convert longitudes in a given dataset to the range of -180 to 180 degrees.

    Args:
        data (DataArray): Input dataset with longitude coordinates.

    Returns:
        DataArray: Dataset with converted longitudes.

    """
    logger = log_configure(loglevel, 'convert_longitudes')
    # Adjust longitudes to the range of -180 to 180 degrees
    data = data.assign_coords(lon=((data["lon"] + 180) % 360) - 180)

    # Roll the dataset to reposition the prime meridian at the center
    data = data.roll(lon=int(len(data['lon']) / 2), roll_coords=True)

    return data


def area_selection(data, region=None, lat_s: float = None, lat_n: float = None,
                   lon_w: float = None, lon_e: float = None, loglevel= "WARNING"):
    """
    Compute the weighted area mean of data within the specified latitude and longitude bounds.

    Parameters:
        data (xarray.Dataset): Input data.

        region (str, optional): Predefined region name. If provided, latitude and longitude bounds will be fetched from predefined regions.

        lat_s (float, optional): Southern latitude bound. Required if region is not provided or None.

        lat_n (float, optional): Northern latitude bound. Required if region is not provided or None.

        lon_w (float, optional): Western longitude bound. Required if region is not provided or None.

        lon_e (float, optional): Eastern longitude bound. Required if region is not provided or None.

    Returns:
        xarray.Dataset: Weighted area mean of the input data.

    Raises:
        ValueError: If region is None but the latitude and longitude bounds are not specified.
    """
    logger = log_configure(loglevel, 'area_selection')
    if region is None:
        if lat_n is None or lat_s is None or lon_w is None or lon_e is None:
            raise ValueError(
                "When region is None, lat_n, lat_s, lon_w, lon_e values need to be specified.")

    else:
        # Obtain latitude and longitude boundaries for the predefined region
        lat_s, lat_n, lon_w, lon_e = predefined_regions(region)
    if lon_w < 0 or lon_e < 0:
        data = convert_longitudes(data)
    logger.debug(
        "Selected for this region (latitude %s to %s, longitude %s to %s)", lat_s, lat_n, lon_w, lon_e)
    # Perform data slicing based on the specified or predefined latitude and longitude boundaries
    data = data.sel(lat=slice(lat_s, lat_n), lon=slice(lon_w, lon_e))

    return data


def weighted_zonal_mean(data, region=None, lat_s: float = None, lat_n: float = None,
                        lon_w: float = None, lon_e: float = None, loglevel= "WARNING"):
    """
    Compute the weighted zonal mean of data within the specified latitude and longitude bounds.

    Parameters:
        data (xarray.Dataset): Input data.

        region (str, optional): Predefined region name. If provided, latitude and longitude bounds will be fetched from predefined regions.

        lat_s (float, optional): Southern latitude bound. Required if region is not provided or None.

        lat_n (float, optional): Northern latitude bound. Required if region is not provided or None.

        lon_w (float, optional): Western longitude bound. Required if region is not provided or None.

        lon_e (float, optional): Eastern longitude bound. Required if region is not provided or None.

    Returns:
        xarray.Dataset: Weighted zonal mean of the input data.
    """
    logger = log_configure(loglevel, 'predefined_regions')
    data = area_selection(data, region, lat_s,
                          lat_n, lon_w, lon_e)

    wgted_mean = data.mean(("lon"))

    return wgted_mean


def weighted_area_mean(data, region=None, lat_s: float = None, lat_n: float = None,
                       lon_w: float = None, lon_e: float = None, loglevel= "WARNING"):
    """
    Compute the weighted area mean of data within the specified latitude and longitude bounds.

    Parameters:
        data (xarray.Dataset): Input data.

        region (str, optional): Predefined region name. If provided, latitude and longitude bounds will be fetched from predefined regions.

        lat_s (float, optional): Southern latitude bound. Required if region is not provided or None.

        lat_n (float, optional): Northern latitude bound. Required if region is not provided or None.

        lon_w (float, optional): Western longitude bound. Required if region is not provided or None.

        lon_e (float, optional): Eastern longitude bound. Required if region is not provided or None.

    Returns:
        xarray.Dataset: Weighted area mean of the input data.
    """
    logger = log_configure(loglevel, 'weighted_area_mean')
    data = area_selection(data, region, lat_s,
                          lat_n, lon_w, lon_e)
    weighted_data = data.weighted(np.cos(np.deg2rad(data.lat)))
    wgted_mean = weighted_data.mean("lat").mean("lon")
    return wgted_mean


def custom_region(region=None, lat_s: float = None, lat_n: float = None,
                  lon_w: float = None, lon_e: float = None, loglevel= "WARNING"):
    logger = log_configure(loglevel, 'custom_region')
    if region in [None, "custom"]:
        region_name = f"Region ({lat_s}:{lat_n} Lat, {lon_w}:{lon_e} Lon)"
    else:
        region_name = region
    return region_name


def split_time_equally(data, loglevel= "WARNING"):
    """
    Splits the input data into two halves based on time dimension, or returns the original data if it has only one time step.

    Parameters:
        data (xarray.Dataset): Input data.

    Returns:
        list: A list containing the original data and the two halves of the data.
    """
    logger = log_configure(loglevel, 'split_time_equally')
    date_len = len(data.time)
    data_1 = None
    data_2 = None
    if date_len == 0:
        raise ValueError("Time lenth is 0 in the data")
    elif date_len > 1:
        # data = None
        if date_len % 2 == 0:
            data_1 = data.isel(time=slice(0, int(date_len/2)))
            data_2 = data.isel(time=slice(int(date_len/2), date_len))
        else:
            data_1 = data.isel(time=slice(0, int((date_len-1)/2)))
            data_2 = data.isel(time=slice(int((date_len-1)/2), date_len))
    return [data, data_1, data_2]


def load_obs_data(model='EN4', exp='en4', source='monthly', loglevel= "WARNING"):
    """
    Load observational data for ocean temperature and salinity.

    Parameters:
        model (str): Model name.
        exp (str): Experiment name.
        source (str): Data source.

    Returns:
        xarray.Dataset: Observational data containing ocean temperature and salinity.
    """
    logger = log_configure(loglevel, 'load_obs_data')
    try:
        reader = Reader(model, exp, source)
    except KeyError:
        raise NoObservationError(
            f"No observation of {model}, {exp}, {source} available")

    den4 = reader.retrieve()
    # We standardise the name for the vertical dimension
    den4 = den4.rename({find_vert_coord(den4)[0]: "lev"}).resample(time="MS").mean()
    # den4 = check_variable_name(den4).resample(time="MS").mean()
    # den4 = den4[["avg_thetao", "avg_so"]].resample(time="MS").mean()
    
    logger.debug("loaded %s data", model)
    return den4


def crop_obs_overlap_time(mod_data, obs_data, loglevel= "WARNING"):
    """
    Crop the observational data to the overlapping time period with the model data.

    Parameters:
        mod_data (xarray.Dataset): Model data.
        obs_data (xarray.Dataset): Observational data.

    Returns:
        xarray.Dataset: Observational data cropped to the overlapping time period with the model data.
    """
    logger = log_configure(loglevel, 'crop_obs_overlap_time')
    mod_data_time = mod_data.time
    obs_data_time = obs_data.time
    common_time = xr.DataArray(np.intersect1d(
        mod_data_time, obs_data_time), dims='time')
    if len(common_time) > 0:
        obs_data = obs_data.sel(time=common_time)
        logger.debug(
            "selected the overlaped time of the obs data compare to the model")
    return obs_data


def data_time_selection(data, time, loglevel= "WARNING"):
    """
    Selects the data based on the specified time period.

    Parameters:
        data (xarray.Dataset): Input data.
        time (str): Time period selection.

    Returns:
        xarray.Dataset: Data for the selected time period.
    """
    logger = log_configure(loglevel, 'data_time_selection')
    if not isinstance(time, int):
        time = time.lower()
    if time in ["jan", "january", "1", 1]:
        data = data.where(data.time.dt.month == 1, drop=True)
        time = "Jan"
    elif time in ["feb", "february", "2", 2]:
        data = data.where(data.time.dt.month == 2, drop=True)
        time = "Feb"
    elif time in ["mar", "march", "3", 3]:
        data = data.where(data.time.dt.month == 3, drop=True)
        time = "Mar"
    elif time in ["apr", "april", "4", 4]:
        data = data.where(data.time.dt.month == 4, drop=True)
        time = "Apr"
    elif time in ["may", "5", 5]:
        data = data.where(data.time.dt.month == 5, drop=True)
        time = "May"
    elif time in ["jun", "june", "6", 6]:
        data = data.where(data.time.dt.month == 6, drop=True)
        time = "Jun"
    elif time in ["jul", "july", "7", 7]:
        data = data.where(data.time.dt.month == 7, drop=True)
        time = "Jul"
    elif time in ["aug", "august", "8", 8]:
        data = data.where(data.time.dt.month == 8, drop=True)
        time = "Aug"
    elif time in ["sep", "sept", "september", "9", 9]:
        data = data.where(data.time.dt.month == 9, drop=True)
        time = "Sep"
    elif time in ["oct", "october", "10", 10]:
        data = data.where(data.time.dt.month == 10, drop=True)
        time = "Oct"
    elif time in ["nov", "november", "11", 11]:
        data = data.where(data.time.dt.month == 11, drop=True)
        time = "Nov"
    elif time in ["dec", "december", "12", 12]:
        data = data.where(data.time.dt.month == 12, drop=True)
        time = "Dec"
    elif time in ["yearly", "year", "y", "13", 13]:
        data = data.groupby('time.year').mean(dim='time')
        if "year" in list(data.dims):
            data = data.rename({"year": "time"})
            time = "Yearly"
    elif time in ["jja", "jun_jul_aug", "jun-jul-aug", "june-july-august", "june_july_august", "14", 14]:
        data = data.where((data['time.month'] >= 6) & (
            data['time.month'] <= 8), drop=True)
        time = "Jun-Jul-Aug"
    elif time in ["fma", "feb_mar_apr", "feb-mar-apr", "february-march-april", "february_march_april", "15", 15]:
        data = data.where((data['time.month'] >= 2) & (
            data['time.month'] <= 4), drop=True)
        time = "Feb-Mar-Apr"
    elif time in ["djf", "dec_jan_feb", "dec-jan-feb", "december-january-february", "december_january_february", "16", 16]:
        data = data.where((data['time.month'] == 12) | (
            data['time.month'] <= 2), drop=True)
        time = "Dec-Jan-Feb"
    elif time in ["son", "sep_oct_nov", "sep-oct-nov", "september-october-november", "september_october_november", "17", 17]:
        data = data.where((data['time.month'] >= 9) & (
            data['time.month'] <= 11), drop=True)
        time = "Sep-Oct-Nov"
    else:
        raise ValueError("""Invalid month input. Please provide a valid name. Among this:
                         Yearly, 3M, Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec, JJA, FMA, DJF, SON """)
    logger.debug("data selected for %s climatology", time)
    return data, time


def compare_arrays(mod_data, obs_data, loglevel= "WARNING"):
    """
    Compares the time scale of model data and observed data and selects the overlapping time periods.

    Parameters:
        mod_data (xarray.Dataset): Model data.
        obs_data (xarray.Dataset): Observed data.

    Returns:
        list: List of model data arrays with overlapping time periods.
        obs_data_selected (xarray.Dataset): Observed data for the overlapping time periods.
    """
    logger = log_configure(loglevel, 'compare_arrays')
    if (obs_data.time == mod_data.time).all() and (len(mod_data.time) == len(obs_data.time)):
        mod_data_list = [mod_data]
        obs_data_selected = obs_data
        logger.debug("obs data and model data time scale fully matched")
    elif (obs_data.time == mod_data.time).any():
        mod_data_ov = mod_data.sel(time=obs_data.time)
        mod_data_list = [mod_data_ov, mod_data]
        obs_data_selected = obs_data
        logger.debug("Model and Obs data time partly matched")
    else:
        mod_data_list = split_time_equally(mod_data)
        obs_data_selected = None
        logger.debug("Model data time is not avaiable for the obs data")

    return mod_data_list, obs_data_selected


def file_naming(region=None,  lat_s: float = None, lat_n: float = None, lon_w: float = None,
                 lon_e: float = None, plot_name=None, loglevel= "WARNING"):
    """
    Creates the directory structure for saving the output data and figures.

    Parameters:
        data (xarray.Dataset): Data used for the plot.
        region (str): Region name.
        lat_s (float): Southern latitude bound.
        lat_n (float): Northern latitude bound.
        lon_w (float): Western longitude bound.
        lon_e (float): Eastern longitude bound.
        plot_name (str): Name of the plot.

    Returns:
        tuple: Output path, figure directory path, data directory path, and filename.
    """
    logger = log_configure(loglevel, 'dir_creation')
    # current_time = f'{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}'

    if region in [None, "custom", "Custom"]:
        region = "custom"
        filename =  f"{plot_name}_lat_{lat_s}_{lat_n}_lon_{lon_w}_{lon_e}"
    else:
        filename =  f"{plot_name}_{region.replace(' ', '_').lower()}"

    return filename


def write_data(output_dir, filename, data, loglevel= "INFO"):
    """
    Write xarray data to a NetCDF file.

    Args:
        file_name (str): Name of the NetCDF file to write.
        data (xarray.Dataset): xarray data to be written to the file.
        loglevel (str): Logging level for configuring the logger. Defaults to 'INFO'.

    Returns:
        None
    """
    logger = log_configure(loglevel, 'write_data')
    output_dir = f"{output_dir}/netcdf"
    filename = f"{output_dir}/{filename}.nc"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    # Check if the file exists
    if os.path.exists(filename):
        # If it exists, delete it
        os.remove(filename)
        logger.debug("Deleted existing file: %s", filename)

    # Write the new xarray data to the NetCDF file
    data.to_netcdf(filename)
    logger.debug("Data written to: %s", filename)

def export_fig(output_dir, filename, type, metadata_value: str = None,
                metadata_name: str = '/Description', loglevel: str = "WARNING"):
    """
    Export a matplotlib figure to a specified output directory and file format.

    Args:
        output_dir (str): Directory where the figure will be saved.
        filename (str): Name of the file to be saved.
        type (str): File format/type of the saved figure (e.g., 'png', 'pdf').
        loglevel (str): Logging level for configuring the logger. Defaults to 'INFO'.

    Returns:
        None
    """
    logger = log_configure(loglevel, 'export_fig')
    # Check if the file exists
    output_dir = f"{output_dir}/{type}"
    filename = f"{output_dir}/{filename}.{type}"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    if os.path.exists(filename):
        os.remove(filename)
        logger.debug("Deleted existing file: %s", filename)
    plt.savefig(filename, bbox_inches='tight')

    if type == "pdf":
        add_pdf_metadata(filename, metadata_value, loglevel = loglevel)
    logger.info("Figure saved to: %s", output_dir)

def split_ocean3d_req(self, o3d_request, loglevel= "WARNING"):
    """
    Split the ocean3d request into individual attributes.

    Args:
        o3d_request (dict): Dictionary containing the ocean3d request details.
        loglevel (str): Logging level for configuring the logger.

    Returns:
        None
    """
    self.data = o3d_request.get('data')
    self.model = o3d_request.get('model')
    self.exp = o3d_request.get('exp')
    self.source = o3d_request.get('source')
    self.region = o3d_request.get('region', None)
    self.lat_s = o3d_request.get('lat_s', None)
    self.lat_n = o3d_request.get('lat_n', None)
    self.lon_w = o3d_request.get('lon_w', None)
    self.lon_e = o3d_request.get('lon_e', None)
    self.customise_level = o3d_request.get('customise_level', None)
    self.levels = o3d_request.get('levels', None)
    self.output = o3d_request.get('output')
    self.output_dir = o3d_request.get('output_dir', None)
    self.loglevel= o3d_request.get('loglevel',"WARNING")
    return self
