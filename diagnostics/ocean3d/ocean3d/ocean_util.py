"""
Common Ocean modules
"""

import os
import warnings
import logging
import xarray as xr
import numpy as np
from aqua import Reader
from aqua.exceptions import NoObservationError
from aqua.util import find_vert_coord


warnings.filterwarnings("ignore")

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)



def check_variable_name(data):
    vars= list(data.variables)
    required_vars= []
    for var in vars:
        if var in ["SO","so","thetao","THETAO","avg_SO","avg_so","avg_thetao","avg_THETAO"]:
            required_vars.append(var)
    if required_vars is not []:
        logger.info(f"This are the varibles {required_vars} available for the diags in the catalogue.")
        data = data[required_vars]
        logger.info("Selected this variables")
        for var in required_vars:
            if isinstance(var, str) and 'so' in var.lower():
                data = data.rename({var: "so"})
                logger.info(f"renaming {var} as so")
            if isinstance(var, str) and 'thetao' in var.lower():
                data = data.rename({var: "ocpt"})
                logger.info(f"renaming {var} as ocpt")
    
    else:
        logger.info("Required variable avg_so and avg_thetao is not available in the catalogue")
    

    vertical_coord = find_vert_coord(data)[0]
    data = data.rename({vertical_coord: "lev"})
    return data

def time_slicing(data, start_year, end_year):
    data = data.sel(time=slice(str(start_year),str(end_year)))
    logger.info(f"Selected the data for the range of {start_year} and {end_year}")
    return data

def predefined_regions(region):
    """
    Get the predefined latitude and longitude boundaries for a given region.

    Args:
        region (str): Name of the region.

    Returns:
        float, float, float, float: Latitude and longitude boundaries (latS, latN, lonW, lonE) for the region.

    Raises:
        ValueError: If an invalid region is provided.

    Available predefined regions:
    - 'Indian Ocean'
    - 'Labrador Sea'
    - 'Global Ocean'
    - 'Atlantic Ocean'
    - 'Pacific Ocean'
    - 'Arctic Ocean'
    - 'Southern Ocean'
    """
    region = region.lower().replace(" ", "").replace("_", "")
    if region in ["indianocean", "indian ocean"]:
        latN, latS, lonW, lonE = 30.0, -30.0, 30, 110.0
    elif region in ["labradorsea", "labrador sea"]:
        latN, latS, lonW, lonE = 65.0, 52.0, 300.0, 316.0
    elif region in ["labradorginseas", "labrador+gin seas"]:
        latN, latS, lonW, lonE = 80.0, 50.0, -70.0, 20.0
    elif region in ["irmingersea", "irminger sea"]:
        latN, latS, lonW, lonE = 60.0, 70.0, 316.0, 330.0
    elif region in ["globalocean", "global ocean"]:
        latN, latS, lonW, lonE = 90.0, -90.0, 0.0, 360.0
    elif region in ["atlanticocean", "atlantic ocean"]:
        latN, latS, lonW, lonE = 65.0, -35.0, -80.0, 30.0
    elif region in ["pacificocean", "pacific ocean"]:
        latN, latS, lonW, lonE = 65.0, -55.0, 120.0, 290.0
    elif region in ["arcticocean", "arctic ocean"]:
        latN, latS, lonW, lonE = 90.0, 65.0, 0.0, 360.0
    elif region in ["southernocean", "southern ocean"]:
        latN, latS, lonW, lonE = -50.0, -80.0, 0.0, 360.0
    elif region in ["weddellsea", "weddell sea"]:
        latN, latS, lonW, lonE = -65.0, -80.0, 295.0, 350.0
    elif region in ["beringsea", "bering sea"]:
        latN, latS, lonW, lonE = 66.0, 53.0, 168.0, -178.0
    elif region in ["gulfofmexico", "gulf of mexico"]:
        latN, latS, lonW, lonE = 30.0, 18.0, -97.0, -81.0
    elif region in ["hudsonbay", "hudson bay"]:
        latN, latS, lonW, lonE = 63.0, 51.0, -95.0, -75.0
    elif region in ["redsea", "red sea"]:
        latN, latS, lonW, lonE = 30.0, 12.0, 32.0, 44.0
    elif region in ["persiangulf", "persian gulf"]:
        latN, latS, lonW, lonE = 30.0, 24.0, 48.0, 56.0
    elif region in ["adriaticsea", "adriatic sea"]:
        latN, latS, lonW, lonE = 45.0, 40.0, 13.0, 19.0
    elif region in ["caribbeansea", "caribbean sea"]:
        latN, latS, lonW, lonE = 23.0, 9.0, -85.0, -60.0
    elif region in ["seaofjapan", "sea of japan"]:
        latN, latS, lonW, lonE = 43.0, 34.0, 129.0, 132.0
    elif region in ["mediterraneansea", "mediterranean sea"]:
        latN, latS, lonW, lonE = 46.0, 30.0, -6.0, 36.0
    elif region in ["blacksea", "black sea"]:
        latN, latS, lonW, lonE = 45.0, 41.0, 27.0, 41.0
    elif region in ["southchina_sea", "south china sea"]:
        latN, latS, lonW, lonE = 21.0, 3.0, 99.0, 121.0
    elif region in ["arabiansea", "arabian sea"]:
        latN, latS, lonW, lonE = 25.0, 12.0, 50.0, 70.0
    elif region in ["coralsea", "coral sea"]:
        latN, latS, lonW, lonE = -10.0, -24.0, 147.0, 157.0
    elif region in ["timorsea", "timor sea"]:
        latN, latS, lonW, lonE = -10.0, -13.0, 123.0, 129.0
    elif region in ["gulfofalaska", "gulf of alaska"]:
        latN, latS, lonW, lonE = 60.0, 48.0, -145.0, -136.0
    elif region in ["eastchinasea", "east china sea"]:
        latN, latS, lonW, lonE = 35.0, 30.0, 120.0, 128.0
    elif region in ["seaofokhotsk", "sea of okhotsk"]:
        latN, latS, lonW, lonE = 60.0, 45.0, 142.0, 163.0
    elif region in ["philippinesea", "philippine sea"]:
        latN, latS, lonW, lonE = 25.0, 5.0, 117.0, 135.0
    elif region in ["rosssea", "ross sea"]:
        latN, latS, lonW, lonE = -60.0, -78.0, 160.0, -150.0
    elif region in ["sargassosea", "sargasso sea"]:
        latN, latS, lonW, lonE = 35.0, 20.0, -70.0, -50.0
    elif region in ["andamansea", "andaman sea"]:
        latN, latS, lonW, lonE = 20.0, 6.0, 93.0, 98.0
    elif region in ["javasea", "java sea"]:
        latN, latS, lonW, lonE = -6.0, -8.0, 105.0, 117.0
    elif region in ["beaufortsea", "beaufort sea"]:
        latN, latS, lonW, lonE = 79.0, 68.0, -140.0, -148.0
    else:
        raise ValueError(
            "Invalid region. Available options: 'Indian Ocean', 'Labrador Sea', 'Global Ocean', 'Atlantic Ocean', 'Pacific Ocean', 'Arctic Ocean', 'Southern Ocean'")
    return latS, latN, lonW, lonE


def convert_longitudes(data):
    """
    Convert longitudes in a given dataset to the range of -180 to 180 degrees.

    Args:
        data (DataArray): Input dataset with longitude coordinates.

    Returns:
        DataArray: Dataset with converted longitudes.

    """
    # Adjust longitudes to the range of -180 to 180 degrees
    data = data.assign_coords(lon=(((data["lon"] + 180) % 360) - 180))

    # Roll the dataset to reposition the prime meridian at the center
    data = data.roll(lon=int(len(data['lon']) / 2), roll_coords=True)

    return data


def area_selection(data, region=None, latS: float = None, latN: float = None,
                   lonW: float = None, lonE: float = None):
    """
    Compute the weighted area mean of data within the specified latitude and longitude bounds.

    Parameters:
        data (xarray.Dataset): Input data.

        region (str, optional): Predefined region name. If provided, latitude and longitude bounds will be fetched from predefined regions.

        latS (float, optional): Southern latitude bound. Required if region is not provided or None.

        latN (float, optional): Northern latitude bound. Required if region is not provided or None.

        lonW (float, optional): Western longitude bound. Required if region is not provided or None.

        lonE (float, optional): Eastern longitude bound. Required if region is not provided or None.

    Returns:
        xarray.Dataset: Weighted area mean of the input data.

    Raises:
        ValueError: If region is None but the latitude and longitude bounds are not specified.
    """
    if region is None:
        if latN is None or latS is None or lonW is None or lonE is None:
            raise ValueError(
                "When region is None, latN, latS, lonW, lonE values need to be specified.")

    else:
        # Obtain latitude and longitude boundaries for the predefined region
        latS, latN, lonW, lonE = predefined_regions(region)
    if lonW < 0 or lonE < 0:
        data = convert_longitudes(data)
    logger.info(
        "Selected for this region (latitude %s to %s, longitude %s to %s)", latS, latN, lonW, lonE)
    # Perform data slicing based on the specified or predefined latitude and longitude boundaries
    data = data.sel(lat=slice(latS, latN), lon=slice(lonW, lonE))

    return data


def weighted_zonal_mean(data, region=None, latS: float = None, latN: float = None,
                        lonW: float = None, lonE: float = None):
    """
    Compute the weighted zonal mean of data within the specified latitude and longitude bounds.

    Parameters:
        data (xarray.Dataset): Input data.

        region (str, optional): Predefined region name. If provided, latitude and longitude bounds will be fetched from predefined regions.

        latS (float, optional): Southern latitude bound. Required if region is not provided or None.

        latN (float, optional): Northern latitude bound. Required if region is not provided or None.

        lonW (float, optional): Western longitude bound. Required if region is not provided or None.

        lonE (float, optional): Eastern longitude bound. Required if region is not provided or None.

    Returns:
        xarray.Dataset: Weighted zonal mean of the input data.
    """
    data = area_selection(data, region, latS,
                          latN, lonW, lonE)

    wgted_mean = data.mean(("lon"))

    return wgted_mean


def weighted_area_mean(data, region=None, latS: float = None, latN: float = None,
                       lonW: float = None, lonE: float = None):
    """
    Compute the weighted area mean of data within the specified latitude and longitude bounds.

    Parameters:
        data (xarray.Dataset): Input data.

        region (str, optional): Predefined region name. If provided, latitude and longitude bounds will be fetched from predefined regions.

        latS (float, optional): Southern latitude bound. Required if region is not provided or None.

        latN (float, optional): Northern latitude bound. Required if region is not provided or None.

        lonW (float, optional): Western longitude bound. Required if region is not provided or None.

        lonE (float, optional): Eastern longitude bound. Required if region is not provided or None.

    Returns:
        xarray.Dataset: Weighted area mean of the input data.
    """
    data = area_selection(data, region, latS,
                          latN, lonW, lonE)
    weighted_data = data.weighted(np.cos(np.deg2rad(data.lat)))
    wgted_mean = weighted_data.mean(("lat", "lon"))
    return wgted_mean


def custom_region(region=None, latS: float = None, latN: float = None,
                  lonW: float = None, lonE: float = None):
    if region in [None, "custom"]:
        region_name = f"Region ({latS}:{latN} Lat, {lonW}:{lonE} Lon)"
    else:
        region_name = region
    return region_name


def split_time_equally(data):
    """
    Splits the input data into two halves based on time dimension, or returns the original data if it has only one time step.

    Parameters:
        data (xarray.Dataset): Input data.

    Returns:
        list: A list containing the original data and the two halves of the data.
    """
    date_len = len(data.time)
    data_1 = None
    data_2 = None
    if date_len == 0:
        raise ValueError("Time lenth is 0 in the data")
    elif date_len > 1:
        data = None
        if date_len % 2 == 0:
            data_1 = data.isel(time=slice(0, int(date_len/2)))
            data_2 = data.isel(time=slice(int(date_len/2), date_len))
        else:
            data_1 = data.isel(time=slice(0, int((date_len-1)/2)))
            data_2 = data.isel(time=slice(int((date_len-1)/2), date_len))
    return [data, data_1, data_2]


def load_obs_data(model='EN4', exp='en4', source='monthly'):
    """
    Load observational data for ocean temperature and salinity.

    Parameters:
        model (str): Model name.
        exp (str): Experiment name.
        source (str): Data source.

    Returns:
        xarray.Dataset: Observational data containing ocean temperature and salinity.
    """
    try:
        reader = Reader(model, exp, source)
    except KeyError:
        raise NoObservationError(
            f"No observation of {model}, {exp}, {source} available")

    den4 = reader.retrieve()
    # We standardise the name for the vertical dimension
    den4 = den4.rename({"depth": "lev"})
    den4 = den4[["ocpt", "so"]].resample(time="MS").mean()
    logger.info("loaded %s data", model)
    return den4


def crop_obs_overlap_time(mod_data, obs_data):
    """
    Crop the observational data to the overlapping time period with the model data.

    Parameters:
        mod_data (xarray.Dataset): Model data.
        obs_data (xarray.Dataset): Observational data.

    Returns:
        xarray.Dataset: Observational data cropped to the overlapping time period with the model data.
    """
    mod_data_time = mod_data.time
    obs_data_time = obs_data.time
    common_time = xr.DataArray(np.intersect1d(
        mod_data_time, obs_data_time), dims='time')
    if len(common_time) > 0:
        obs_data = obs_data.sel(time=common_time)
        logger.info(
            "selected the overlaped time of the obs data compare to the model")
    return obs_data


def data_time_selection(data, time):
    """
    Selects the data based on the specified time period.

    Parameters:
        data (xarray.Dataset): Input data.
        time (str): Time period selection.

    Returns:
        xarray.Dataset: Data for the selected time period.
    """
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
    logger.info("data selected for %s climatology", time)
    return data, time


def compare_arrays(mod_data, obs_data):
    """
    Compares the time scale of model data and observed data and selects the overlapping time periods.

    Parameters:
        mod_data (xarray.Dataset): Model data.
        obs_data (xarray.Dataset): Observed data.

    Returns:
        list: List of model data arrays with overlapping time periods.
        obs_data_selected (xarray.Dataset): Observed data for the overlapping time periods.
    """
    if (obs_data.time == mod_data.time).all() and (len(mod_data.time) == len(obs_data.time)):
        mod_data_list = [mod_data]
        obs_data_selected = obs_data
        logger.info("obs data and model data time scale fully matched")
    elif (obs_data.time == mod_data.time).any():
        mod_data_ov = mod_data.sel(time=obs_data.time)
        mod_data_list = [mod_data_ov, mod_data]
        obs_data_selected = obs_data
        logger.info("Model and Obs data time partly matched")
    else:
        mod_data_list = split_time_equally(mod_data)
        obs_data_selected = None
        logger.info("Model data time is not avaiable for the obs data")

    return mod_data_list, obs_data_selected


def dir_creation(data, region=None,  latS: float = None, latN: float = None, lonW: float = None,
                 lonE: float = None, output_dir=None, plot_name=None):
    """
    Creates the directory structure for saving the output data and figures.

    Parameters:
        data (xarray.Dataset): Data used for the plot.
        region (str): Region name.
        latS (float): Southern latitude bound.
        latN (float): Northern latitude bound.
        lonW (float): Western longitude bound.
        lonE (float): Eastern longitude bound.
        output_dir (str): Directory path for saving the output.
        plot_name (str): Name of the plot.

    Returns:
        tuple: Output path, figure directory path, data directory path, and filename.
    """
    # current_time = f'{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}'

    if "model" in data.attrs and "exp" in data.attrs and "source" in data.attrs:
        model = data.attrs["model"]
        exp = data.attrs["exp"]
        source = data.attrs["source"]
        filename = f"ocean3d_{model}_{exp}_{source}_"
    else:
        filename = f"ocean3d_"
    if output_dir is None:
        raise ValueError("Please provide the outut_dir when output = True")
    if region in [None, "custom", "Custom"]:
        region = "custom"
        filename = filename + f"{plot_name}_{region.replace(' ', '_').lower()}_lat_{latS}_{latN}_lon_{lonW}_{lonE}"
    else:
        filename = filename + f"{plot_name}_{region.replace(' ', '_').lower()}"

    # output_path = f"{output_dir}/"
    fig_dir = f"{output_dir}/pdf"
    data_dir = f"{output_dir}/netcdf"
    os.makedirs(fig_dir, exist_ok=True)
    os.makedirs(data_dir, exist_ok=True)
    return output_dir, fig_dir, data_dir, filename
