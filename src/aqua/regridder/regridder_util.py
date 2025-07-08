"""Regridding utilities."""

import os
import numpy as np


def check_existing_file(filename):
    """
    Checks if an area/weights file exists and is valid.
    Return true if the file has some records.
    """
    return os.path.exists(filename) and os.path.getsize(filename) > 0


def validate_reader_kwargs(reader_kwargs):
    """
    Validate the reader kwargs.
    """
    if not reader_kwargs:
        raise ValueError("reader_kwargs must be provided.")
    for key in ["model", "exp", "source"]:
        if key not in reader_kwargs:
            raise ValueError(f"reader_kwargs must contain key '{key}'.")
    return reader_kwargs

def detect_grid(data, lat='lat', lon='lon'):
    """
    This is copied from smmregrid.GridInspector.detect_grid
    Detect the grid type based on the structure of the data.
    The idea is to bring it back to the original smmregrid.GridInspector once we 
    clarified all the requirements for the grid detection."""

    if "healpix" in data.variables:
        return "Healpix"

    if not lat or not lon:
        return "Unknown"

    # 2D coord-dim dependency
    if data[lat].ndim == 2 and data[lon].ndim == 2:
        return "Curvilinear"

    # 1D coord-dim dependency
    if data[lat].ndim == 1 and data[lon].ndim == 1:

        # Regular: latitude and longitude depend on different coordinates
        if data[lat].dims != data[lon].dims:

            lat_diff = np.diff(data[lat].values)
            lon_diff = np.diff(data[lon].values)
            if np.allclose(lat_diff, lat_diff[0]) and np.allclose(lon_diff, lon_diff[0]):
                return "Regular"

            # Gaussian: second derivative of latitude is positive from -90 to 0
            lat_values = data[lat].where(data[lat]<0).values
            lat_values=lat_values[~np.isnan(lat_values)]
            gaussian = np.all(np.diff(lat_values, n=2) > 0)
            if gaussian:
                return "GaussianRegular"
            
            return "UndefinedRegular"

        # Healpix: number of pixels is a multiple of 12 and log2(pix / 12) is an integer
        pix = data[lat].size
        if pix % 12 == 0 and (pix // 12).bit_length() - 1 == np.log2(pix // 12):
            return "Healpix"
        
        # Guess gaussian reduced: increasing number of latitudes from -90 to 0
        lat_values = data[lat].where(data[lat]<0).values
        lat_values=lat_values[~np.isnan(lat_values)]
        _, counts = np.unique(lat_values, return_counts=True)
        gaussian_reduced = np.all(np.diff(counts)>0)
        if gaussian_reduced:
            return "GaussianReduced"

        # None of the above cases
        return "Unstructured"

    return "Unknown"
    