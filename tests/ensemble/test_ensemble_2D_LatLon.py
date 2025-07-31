"""Test ensemble Ensemble module"""
import pytest
from aqua import Reader
from aqua.diagnostics import EnsembleLatLon
from aqua.diagnostics.ensemble.util import retrieve_merge_ensemble_data
from aqua.diagnostics import PlotEnsembleLatLon

@pytest.mark.ensemble
def test_ensemble_2D_LatLon():
    """Initialize variables before the test."""
    catalog_list = ['ci', 'ci']
    model_list = ['FESOM', 'FESOM']
    exp_list = ['results', 'results']
    source_list = ['atmglobalmean2D', 'atmglobalmean2D']
    variable = '2t'

    dataset = retrieve_merge_ensemble_data(
        variable=variable, 
        catalog_list=catalog_list, 
        model_list=model_list, 
        exp_list=exp_list, 
        source_list=source_list, 
        log_level = "WARNING",
        ens_dim="ensemble"
    )
    assert dataset is not None
   
    ens_latlon = EnsembleLatLon(
        var=variable,
        dataset=dataset,
        catalog_list=catalog_list,
        model_list=model_list,
        source_list=source_list,
        ensemble_dimension_name="ensemble",
    )

    ens_latlon.run()

    assert ens_latlon.dataset_mean is not None
    assert ens_latlon.dataset_std.all() == 0
 
    # PlotEnsembleLatLon class
    plot_arguments = {
        "var": variable,
        "catalog_list": catalog_list,
        "model_list": model_list,
        "exp_list": exp_list,
        "source_list": source_list,
        "save_pdf": True,
        "save_png": True,
        "title_mean": "Test data",
        "title_std": "Test data",
        "cbar_label": "Test Label",
    }

    ens_latlon_plot = PlotEnsembleLatLon(
        **plot_arguments,
        dataset_mean=ens_latlon.dataset_mean,
        dataset_std=ens_latlon.dataset_std,
    )
    plot_dict = ens_latlon_plot.plot()
    
    assert plot_dict['mean_plot'][0] is not None




    
