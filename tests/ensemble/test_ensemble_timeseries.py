"""Test ensemble Ensemble module"""
import pytest
from aqua.diagnostics import EnsembleTimeseries
from aqua.diagnostics.ensemble.util import retrieve_merge_ensemble_data
from aqua.diagnostics import PlotEnsembleTimeseries


@pytest.mark.ensemble
def test_ensemble_timeseries():
    """Initialize variables before the test."""
    catalog_list = ['ci', 'ci']
    model_list = ['FESOM', 'FESOM']
    exp_list = ['results', 'results']
    source_list = ['timeseries1D', 'timeseries1D']
    variable = '2t'
    
    # loading and merging the data
    dataset = retrieve_merge_ensemble_data(
        variable=variable,
        catalog_list=catalog_list,
        model_list=model_list,
        exp_list=exp_list,
        source_list=source_list
        )
    assert dataset is not None
    ts = EnsembleTimeseries(
        var=variable,
        monthly_data=dataset,
        annual_data=dataset,
        catalog_list=catalog_list,
        model_list=model_list,
        exp_list=exp_list,
        source_list=source_list,
    )

    ts.run()
    assert ts.monthly_data_mean is not None
    assert ts.annual_data_mean is not None
    assert ts.monthly_data_std.values.all() == 0
    assert ts.annual_data_std.values.all() == 0
    
    # PlotEnsembleTimeseries class
    plot_arguments = {
        "var": variable,
        "catalog_list": catalog_list,
        "model_list": model_list,
        "exp_list": exp_list,
        "source_list": source_list,
        "save_pdf": True,
        "save_png": True,
        "plot_ensemble_members": True,
        "title": "test timeseries data",
    }


    ts_plot = PlotEnsembleTimeseries(
        **plot_arguments,
        monthly_data=ts.monthly_data,
        monthly_data_mean=ts.monthly_data_mean,
        monthly_data_std=ts.monthly_data_std,
        annual_data=ts.annual_data,
        annual_data_mean=ts.annual_data_mean,
        annual_data_std=ts.annual_data_std,
    )
    fig, ax = ts_plot.plot() 

    assert fig is not None
   


