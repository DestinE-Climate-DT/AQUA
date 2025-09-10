"""Test ensemble Ensemble module"""
import os
import pytest
from aqua.diagnostics import EnsembleZonal
from aqua.diagnostics.ensemble.util import retrieve_merge_ensemble_data
from aqua.diagnostics import PlotEnsembleZonal

@pytest.mark.ensemble
def test_ensemble_zonal():
    """Initialize variables before the test."""
    var = 'avg_so'
    tmp_path = './'

    # NOTE:
    # The variables filename1 and filename2 depend on 
    # the names in the following lists
    # if any of the values are to be changed
    # then please update variables filename1 and filename2

    catalog_list = ['ci', 'ci']
    model_list = ['NEMO', 'NEMO']
    exp_list = ['results', 'results']
    source_list = ['zonal_mean-latlev', 'zonal_mean-latlev']

    # loading and merging the data
    dataset = retrieve_merge_ensemble_data(
        variable=var, 
        catalog_list=catalog_list, 
        model_list=model_list, 
        exp_list=exp_list, 
        source_list=source_list, 
        log_level = "WARNING",
        ens_dim="ensemble"
    )
    assert dataset is not None
    
    # EnsmebleZonal Class
    zonalmean_ens = EnsembleZonal(
        var=var,
        dataset=dataset,
        catalog_list=catalog_list,
        model_list=model_list,
        exp_list=exp_list,
        source_list=source_list,
        ensemble_dimension_name="ensemble",
        outputdir=tmp_path,
    )
 
    zonalmean_ens.run()
    
    filename1 = f'ensemble.EnsembleZonal.{catalog_list[0]}.{model_list[0]}.{exp_list[0]}.r1.{var}.mean.nc'
    file = os.path.join(tmp_path, 'netcdf', filename1)
    assert os.path.exists(file)

    filename2 = f'ensemble.EnsembleZonal.{catalog_list[0]}.{model_list[0]}.{exp_list[0]}.r1.{var}.std.nc'
    file = os.path.join(tmp_path, 'netcdf', filename2)
    assert os.path.exists(file)
 
    # test if mean is non-zero and variance is zero
    assert zonalmean_ens.dataset_mean is not None
    assert zonalmean_ens.dataset_std.all() == 0
   
    # PlotEnsembleZonal class
    plot_arguments = {
        "var": var,
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

    # STD values are zero. Therefore we are giving the mean value as std values to test the implementation
    ens_zonal_plot = PlotEnsembleZonal(
        **plot_arguments,
        dataset_mean=zonalmean_ens.dataset_mean,
        dataset_std=zonalmean_ens.dataset_std,
        outputdir=tmp_path,
    )
    plot_dict = ens_zonal_plot.plot()
    
    assert plot_dict['mean_plot'][0] is not None

    filename1 = f'ensemble.EnsembleZonal.{catalog_list[0]}.{model_list[0]}.{exp_list[0]}.r1.{var}.mean.png'
    file = os.path.join(tmp_path, 'png', filename1)
    assert os.path.exists(file)

    filename2 = f'ensemble.EnsembleZonal.{catalog_list[0]}.{model_list[0]}.{exp_list[0]}.r1.{var}.std.png'
    file = os.path.join(tmp_path, 'png', filename2)
    assert os.path.exists(file)

    filename1 = f'ensemble.EnsembleZonal.{catalog_list[0]}.{model_list[0]}.{exp_list[0]}.r1.{var}.mean.pdf'
    file = os.path.join(tmp_path, 'pdf', filename1)
    assert os.path.exists(file)

    filename2 = f'ensemble.EnsembleZonal.{catalog_list[0]}.{model_list[0]}.{exp_list[0]}.r1.{var}.std.pdf'
    file = os.path.join(tmp_path, 'pdf', filename2)
    assert os.path.exists(file)






