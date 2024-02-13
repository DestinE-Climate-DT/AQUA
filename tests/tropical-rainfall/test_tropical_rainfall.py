"""Test of tropical rainfall diagnostic"""
import pytest
# import numpy as np
import xarray

import re

from os import listdir
from os.path import isfile, join
from os import remove

from aqua import Reader
from aqua.util import create_folder

import os
import sys
path_to_diagnostic = './diagnostics/'
sys.path.insert(1, path_to_diagnostic)
try:
    from tropical_rainfall import Tropical_Rainfall
except ModuleNotFoundError:
    print("The module tropical_rainfall.py is not found.")

approx_rel = 1e-4


@pytest.mark.tropical_rainfall
@pytest.fixture
def reader():
    if os.getenv('INPUT_ARG') is None:
        data = Reader(model="IFS", exp="test-tco79", source="long")
        retrieved = data.retrieve()
        try:
            retrieved_array = retrieved['tprate']*86400
        except KeyError:
            retrieved_array = retrieved['2t']
        return retrieved_array.isel(time=slice(10, 11))

    elif str(os.getenv('INPUT_ARG')) == 'levante':
        """reader_levante """
        data = Reader(model="ICON", exp="ngc2009", source="lra-r100-monthly")
        retrieved = data.retrieve()
        try:
            retrieved_array = retrieved['tprate']*86400
        except KeyError:
            retrieved_array = retrieved['2t']
        return retrieved_array.isel(time=slice(10, 11))

    elif str(os.getenv('INPUT_ARG')) == 'lumi':
        """reader_levante """
        data = Reader(model="ERA5", exp="fdb", source="default")
        retrieved = data.retrieve()
        try:
            retrieved_array = retrieved['tprate']*86400
        except KeyError:
            retrieved_array = retrieved['2t']
        return retrieved_array.isel(time=slice(10, 11))


@pytest.mark.tropical_rainfall
def test_module_import():
    """Testing the import of tropical rainfall diagnostic
    """
    try:
        from tropical_rainfall import Tropical_Rainfall
    except ModuleNotFoundError:
        assert False, "Diagnostic could not be imported"


@pytest.fixture
def data_size(reader):
    """ Return total size of Dataset"""
    data = reader
    if 'DataArray' in str(type(data)):
        size = data.size
    elif 'Dataset' in str(type(data)):
        names = list(data.dims)
        size = 1
        for i in names:
            size *= data[i].size
    return size


@pytest.mark.tropical_rainfall
def test_update_default_attribute():
    """ Testing the update of default attributes
    """
    diag = Tropical_Rainfall()
    old_trop_lat_value = diag.trop_lat
    diag.class_attributes_update(trop_lat=20)
    new_trop_lat_value = diag.trop_lat
    assert old_trop_lat_value != new_trop_lat_value


@pytest.mark.tropical_rainfall
def test_attribute_type():
    """ Testing the type of attributes
    """
    try:
        diag = Tropical_Rainfall(trop_lat='str')
    except TypeError:
        print(diag.trop_lat)
        assert True,       "supposed to be the wrong type"
    try:
        Tropical_Rainfall(s_year=0.5)
    except TypeError:
        assert True,       "supposed to be the wrong type"
    try:
        Tropical_Rainfall(model_variable=0)
    except TypeError:
        assert True,       "supposed to be the wrong type"


@pytest.fixture
def histogram_output(reader):
    """ Histogram output fixture
    """
    data = reader
    if 'tprate' in data.shortName:
        diag = Tropical_Rainfall(
            num_of_bins=1000, first_edge=0, width_of_bin=1 - 1*10**(-6), loglevel='debug')
    elif '2t' in data.shortName:
        diag = Tropical_Rainfall(
            num_of_bins=1000, first_edge=0, width_of_bin=0.5, loglevel='debug')
    hist = diag.histogram(data, trop_lat=90)
    return hist


@pytest.mark.tropical_rainfall
def test_hisogram_counts(histogram_output, data_size):
    """ Testing the histogram counts
    """
    hist = histogram_output
    counts_sum = sum(hist.counts.values)
    assert counts_sum > 0
    assert counts_sum <= data_size


@pytest.mark.tropical_rainfall
def test_histogram_frequency(histogram_output):
    """ Testing the histogram frequency
    """
    hist = histogram_output
    frequency_sum = sum(hist.frequency.values)
    assert frequency_sum - 1 < 10**(-4)


@pytest.mark.tropical_rainfall
def test_histogram_pdf(histogram_output):
    """ Testing the histogram pdf
    """
    hist = histogram_output
    pdf_sum = sum(hist.pdf.values*hist.width.values)
    assert pdf_sum-1 < 10**(-4)


@pytest.mark.tropical_rainfall
def test_histogram_load_to_memory(histogram_output):
    """ Testing the histogram load to memory
    """
    path_to_histogram = str(path_to_diagnostic)+"/test_output/histograms/"
    create_folder(folder=str(path_to_diagnostic) +
                  "/test_output/",               loglevel='WARNING')
    create_folder(folder=str(path_to_diagnostic) +
                  "/test_output/histograms/",    loglevel='WARNING')

    # Cleaning the repository with histograms before new test
    histogram_list = [f for f in listdir(
        path_to_histogram) if isfile(join(path_to_histogram, f))]
    histograms_list_full_path = [str(
        path_to_histogram)+str(histogram_list[i]) for i in range(0, len(histogram_list))]
    for i in range(0, len(histograms_list_full_path)):
        remove(histograms_list_full_path[i])
    hist = histogram_output
    diag = Tropical_Rainfall()
    diag.main.dataset_to_netcdf(
        dataset=hist, path_to_netcdf=path_to_histogram, name_of_file='test_hist_saving')
    files = [f for f in listdir(path_to_histogram)
             if isfile(join(path_to_histogram, f))]
    assert 'test_hist_saving' in files[0]

    time_band = hist.counts.attrs['time_band']
    try:
        re_time_band = re.split(":", re.split(", ", time_band)[0])[
            0]+'_' + re.split(":", re.split(", ", time_band)[1])[0] in files[0]
    except IndexError:
        re_time_band = re.split(":", time_band)[0]
    assert re_time_band in time_band


@pytest.mark.tropical_rainfall
def test_hist_figure_load_to_memory(histogram_output):
    """ Testing the saving of the figure with histogram
    """
    create_folder(folder=str(path_to_diagnostic) +
                  "/test_output/plots/", loglevel='WARNING')
    path_to_pdf = str(path_to_diagnostic) + "/test_output/plots/"
    # Cleaning the repository with plots before new test
    figure_list = [f for f in listdir(
        path_to_pdf) if isfile(join(path_to_pdf, f))]
    figure_list_full_path = [str(path_to_pdf)+str(figure_list[i])
                             for i in range(0, len(figure_list))]
    for i in range(0, len(figure_list_full_path)):
        remove(figure_list_full_path[i])

    hist = histogram_output
    diag = Tropical_Rainfall()
    diag.histogram_plot(hist, path_to_pdf=path_to_pdf, name_of_file='test_hist_fig_saving')
    assert 'test_hist_fig_saving' in listdir(path_to_pdf)[0]


@pytest.mark.tropical_rainfall
def test_lazy_mode_calculation(reader):
    """ Testing the lazy mode of the calculation
    """
    data = reader
    diag = Tropical_Rainfall(
        num_of_bins=1000, first_edge=0, width_of_bin=1 - 1*10**(-6))
    hist_lazy = diag.histogram_lowres(data, lazy=True)
    assert 'frequency' not in hist_lazy.attrs
    assert 'pdf' not in hist_lazy.variables


@pytest.mark.tropical_rainfall
def test_local_attributes_of_histogram(histogram_output):
    """ Testing the local attributes of histogram
    """
    hist = histogram_output
    assert 'time_band' in hist.counts.attrs
    assert 'lat_band' in hist.counts.attrs
    assert 'lon_band' in hist.counts.attrs


@pytest.mark.tropical_rainfall
def test_global_attributes_of_histogram(histogram_output):
    """ Testing the global attributes of histogram
    """
    hist = histogram_output
    try:
        assert 'time_band' in hist.attrs['history']
        assert 'lat_band' in hist.attrs['history']
        assert 'lon_band' in hist.attrs['history']
    except KeyError:
        print("The obtained xarray.Dataset doesn't have global attributes.")


@pytest.mark.tropical_rainfall
def test_variables_of_histogram(histogram_output):
    """ Testing the variables of histogram
    """
    hist = histogram_output
    assert isinstance(hist, xarray.core.dataarray.Dataset)
    try:
        hist.counts
    except KeyError:
        assert False,       "counts not in variables"
    try:
        hist.frequency
    except KeyError:
        assert False,       "frequency not in variables"
    try:
        hist.pdf
    except KeyError:
        assert False,       "pdf not in variables"


@pytest.mark.tropical_rainfall
def test_coordinates_of_histogram(histogram_output):
    """ Testing the coordinates of histogram
    """
    hist = histogram_output
    'center_of_bin' in hist.coords
    'width' in hist.coords


@pytest.mark.tropical_rainfall
def test_latitude_band(reader):
    """ Testing the latitude band
    """
    data = reader
    max_lat_value = max(data.lat.values[0], data.lat.values[-1])
    diag = Tropical_Rainfall(trop_lat=10)
    data_trop = diag.main.latitude_band(data)
    assert max_lat_value > max(
        data_trop.lat.values[0], data_trop.lat.values[-1])
    assert 10 > data_trop.lat.values[-1]


@pytest.mark.tropical_rainfall
def test_histogram_merge(histogram_output):
    """ Testing the histogram merge
    """
    hist_1 = histogram_output
    counts_1 = sum(hist_1.counts.values)

    hist_2 = histogram_output
    counts_2 = sum(hist_2.counts.values)

    diag = Tropical_Rainfall()

    path_to_histogram = str(path_to_diagnostic)+"/test_output/histograms/"
    diag.main.dataset_to_netcdf(
        dataset=hist_2, path_to_netcdf=path_to_histogram, name_of_file='test_merge')

    hist_merged = diag.merge_two_datasets(
        dataset_1=hist_1, dataset_2=hist_2)
    counts_merged = sum(hist_merged.counts.values)
    assert counts_merged == (counts_1 + counts_2)


@pytest.mark.tropical_rainfall
def test_units_converter(reader):
    """ Testing convertation of units"""

    data = reader
    diag = Tropical_Rainfall()

    old_units = data.units

    if 'tprate' in data.shortName:
        old_mean_value = float(data.mean().values)
        data = diag.precipitation_rate_units_converter(
            data, new_unit=old_units)
        new_mean_value = float(data.mean().values)

        assert old_mean_value == new_mean_value

        data = diag.precipitation_rate_units_converter(
            data, new_unit='cm s**-1')
        mean_value_cmpersec = float(data.mean().values)
        data = diag.precipitation_rate_units_converter(
            data, new_unit='mm s**-1')
        mean_value_mmpersec = float(data.mean().values)

        assert abs(mean_value_mmpersec/mean_value_cmpersec - 10) < 1e-3
        assert data.units == 'mm s**-1'

        data = diag.precipitation_rate_units_converter(
            data, new_unit='m min**-1')
        mean_value_mpermin = float(data.mean().values)
        data = diag.precipitation_rate_units_converter(
            data, new_unit='m day**-1')
        mean_value_mperday = float(data.mean().values)

        assert abs(mean_value_mperday/mean_value_mpermin - 24*60) < 1e-3
        assert data.units == 'm day**-1'

        data = diag.precipitation_rate_units_converter(
            data, new_unit='m year**-1')
        mean_value_mperyear = float(data.mean().values)

        assert abs(mean_value_mperday/mean_value_mperyear - 0.00273973) < 1e-3
        assert data.units == 'm year**-1'
