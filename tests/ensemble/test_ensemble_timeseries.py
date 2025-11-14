"""Test ensemble Ensemble module"""
import os
import pytest
from aqua.diagnostics import EnsembleTimeseries
from aqua.diagnostics.ensemble.util import reader_retrieve_and_merge
from aqua.diagnostics import PlotEnsembleTimeseries
from conftest import DPI


@pytest.mark.ensemble
def test_ensemble_timeseries():
    """Initialize variables before the test."""
    var = '2t'
    tmp_path = './'

    # NOTE:
    # The variables filename1 and filename2 depend on 
    # the names in the following lists
    # if any of the values are to be changed
    # then please update variables filename1 and filename2

    catalog_list = ['ci', 'ci'] 
    model_list = ['FESOM', 'FESOM']
    exp_list = ['results', 'results']
    source_list = ['timeseries1D', 'timeseries1D']
    
    # loading and merging the data
    dataset = reader_retrieve_and_merge(
        variable=var,
        catalog_list=catalog_list,
        model_list=model_list,
        exp_list=exp_list,
        source_list=source_list,
        loglevel = "WARNING",
        ens_dim="ensemble",
    )
    assert dataset is not None
    
    # EnsembleTimeseries class
    ts = EnsembleTimeseries(
        var=var,
        monthly_data=dataset,
        annual_data=dataset,
        catalog_list=catalog_list,
        model_list=model_list,
        exp_list=exp_list,
        source_list=source_list,
        ensemble_dimension_name="ensemble",
        outputdir=tmp_path,
    )

    ts.run()

    filename1 = f'ensemble.ensembletimeseries.{catalog_list[0]}.{model_list[0]}.{exp_list[0]}.r1.{var}.mean.monthly.nc'
    file = os.path.join(tmp_path, 'netcdf', filename1)
    assert os.path.exists(file)

    filename2 = f'ensemble.ensembletimeseries.{catalog_list[0]}.{model_list[0]}.{exp_list[0]}.r1.{var}.mean.annual.nc'
    file = os.path.join(tmp_path, 'netcdf', filename2)
    assert os.path.exists(file)

    # test if mean is non-zero and variance is zero
    assert ts.monthly_data_mean is not None
    assert ts.annual_data_mean is not None
    assert ts.monthly_data_std.values.all() == 0
    assert ts.annual_data_std.values.all() == 0
    
    # PlotEnsembleTimeseries class
    plot_class_arguments = {
        "catalog_list": catalog_list,
        "model_list": model_list,
        "exp_list": exp_list,
        "source_list": source_list,
    }

    # STD values are zero. Therefore we are giving the mean value as std values to test the implementation
    ts_plot = PlotEnsembleTimeseries(
        **plot_class_arguments,
        outputdir=tmp_path,
    )
    plot_arguments = {
        "var": var,
        "save_pdf": True,
        "save_png": True,
        "plot_ensemble_members": True,
        "title": "test timeseries data",
        "monthly_data": ts.monthly_data,
        "monthly_data_mean": ts.monthly_data_mean,
        "monthly_data_std": ts.monthly_data_mean,
        "annual_data":ts.annual_data,
        "annual_data_mean": ts.annual_data_mean,
        "annual_data_std":ts.annual_data_mean,
        "ref_monthly_data":ts.monthly_data_mean,
        "ref_annual_data":ts.annual_data_mean,
        "dpi": DPI,
    }

    fig, ax = ts_plot.plot(**plot_arguments) 

    assert fig is not None
    assert ax is not None 
   
    filename1 = f'ensemble.ensembletimeseries.{catalog_list[0]}.{model_list[0]}.{exp_list[0]}.r1.{var}.mean.png'
    file = os.path.join(tmp_path, 'png', filename1)
    assert os.path.exists(file)
    
    filename2 = f'ensemble.ensembletimeseries.{catalog_list[0]}.{model_list[0]}.{exp_list[0]}.r1.{var}.mean.pdf'
    file = os.path.join(tmp_path, 'pdf', filename2)
    assert os.path.exists(file)
   


