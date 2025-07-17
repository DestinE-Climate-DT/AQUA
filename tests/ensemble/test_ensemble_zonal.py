"""Test ensemble Ensemble module"""
import pytest
from aqua.diagnostics import EnsembleZonal
from aqua.diagnostics.ensemble.util import retrieve_merge_ensemble_data
from aqua.diagnostics import PlotEnsembleZonal

@pytest.mark.ensemble
def test_ensemble_zonal():
    """Initialize variables before the test."""
    catalog_list = ['ci', 'ci']
    model_list = ['NEMO', 'NEMO']
    exp_list = ['results', 'results']
    source_list = ['zonal_mean-latlev', 'zonal_mean-latlev']
    variable = 'avg_so'

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
    
    zonalmean_ens = EnsembleZonal(
        var=variable,
        dataset=dataset,
        catalog_list=catalog_list,
        model_list=model_list,
        source_list=source_list,
        ensemble_dimension_name="ensemble",
    )
 
    zonalmean_ens.run()
    
    assert zonalmean_ens.dataset_mean is not None
    assert zonalmean_ens.dataset_std.all() == 0
   
    # PlotEnsembleZonal class
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

    ens_zonal_plot = PlotEnsembleZonal(
        **plot_arguments,
        dataset_mean=zonalmean_ens.dataset_mean,
        dataset_std=zonalmean_ens.dataset_std,
    )
    plot_dict = ens_zonal_plot.plot()
    
    assert plot_dict['mean_plot'][0] is not None





