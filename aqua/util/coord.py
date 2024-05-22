"""Utilities to flip latitude coordinates if needed."""

# List of latitude unit strings (the same used by cf2cdm)
latitude_units = ["degrees_north", "degree_north", "degree_N", "degrees_N", "degreeN", "degreesN"]
time_coords = ["time"]

def find_lat_dir(dataset):
    """
    Finds a latitude coordinate and returns its name and directions.

    Args:
        dataset (xarray.Dataset): Dataset to check

    Return:
        tuple: (latitude_coord, direction)
            latitude_coord (str): Name of the latitude coordinate
            direction (str): Direction of the latitude coordinate (increasing or decreasing)
    """

    # Iterate through coordinates to find the latitude coordinate
    latitude_coord = None
    direction = None

    for coord_name in dataset.coords:
        un = dataset.coords[coord_name].attrs.get('units')
        if un in latitude_units:
            latitude_coord = coord_name
            break

    # If a latitude coordinate is found, determine its direction
    if latitude_coord is not None:
        latitude_values = dataset.coords[latitude_coord].values
        if latitude_values[0] < latitude_values[1]:
            direction = "increasing"
        else:
            direction = "decreasing"

    return latitude_coord, direction


def check_direction(dataset, coord, old_dir):
    """
    Check if the direction of the coordinate is the same as the old direction
    If not, set attribute "flipped" to the name of the old direction.

    Args:
        dataset (xarray.Dataset): Dataset to check
        coord (str): Coordinate to check
        old_dir (str): Old direction of the coordinate

    Returns:
        None
    """

    if coord:
        if dataset.coords[coord].values[0] < dataset.coords[coord].values[1]:
            new_dir = "increasing"
        else:
            new_dir = "decreasing"

        if old_dir != new_dir:
            dataset.coords[coord].attrs["flipped"] = old_dir


def flip_lat_dir(dataset):
    """
    Finds a latitude coordinate and flips it back if it has a 'flipped' attribute.

    Args:
        dataset (xarray.Dataset): Dataset to check

    Return:
        None
    """

    # Iterate through coordinates to find the latitude coordinate
    latitude_coord = None
    for coord_name in dataset.coords:
        un = dataset.coords[coord_name].attrs.get('units')
        if un in latitude_units:
            latitude_coord = coord_name
            break

    # If a latitude coordinate is found, determine if it has been flipped
    # in that case, reverse it back
    new_dataset = dataset
    if latitude_coord is not None:
        if dataset.coords[latitude_coord].attrs.get("flipped"):
            new_dataset = dataset.copy()
            new_dataset = new_dataset.isel({latitude_coord: slice(None, None, -1)})
            new_dataset.coords[latitude_coord].attrs.pop("flipped")  # remove the flipped attribute

    return new_dataset

# NOT IN USE, LEAVING FOR FUTURE REFERENCE
# In case this is uncommented, a test is available in tests/test_util.py

# def flip_time(dataset):
#     """
#     Flips the time coordinate of a dataset.

#     Args:
#         dataset (xarray.Dataset or xarray.DataArray): The dataset to be flipped.

#     Return:
#         xarray.Dataset or xarray.DataArray: The flipped dataset.

#     Raises:
#         ValueError: If the dataset does not have a 'time' coordinate.
#     """

#     # Assume the time coordinate is called 'time'
#     new_dataset = dataset
#     time_coord = None
    
#     for coord in time_coords:
#         if coord in dataset.coords:
#             time_coord = coord
#             break

#     if time_coord:
#         new_dataset = dataset.copy()
#         new_dataset = new_dataset.isel({time_coord: slice(None, None, -1)})
#         new_dataset.coords["time"].attrs["flipped"] = True
#     else:
#         raise ValueError("The dataset does not have a 'time' coordinate.")

#     return new_dataset
