"""Test ensemble Ensemble module"""
import os
import pytest
from aqua import Reader
from aqua.diagnostics import EnsembleLatLon
from aqua.diagnostics.ensemble.util import reader_retrieve_and_merge
from aqua.diagnostics import PlotEnsembleLatLon

@pytest.mark.ensemble
def test_ensemble_2D_LatLon():
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
    source_list = ['atmglobalmean2D', 'atmglobalmean2D']

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
   
    # EnsembleLatLon class
    ens_latlon = EnsembleLatLon(
        var=var,
        dataset=dataset,
        catalog_list=catalog_list,
        model_list=model_list,
        exp_list=exp_list,
        source_list=source_list,
        ensemble_dimension_name="ensemble",
        outputdir=tmp_path, 
    )

    ens_latlon.run()

    filename1 = f'ensemble.ensemblelatlon.{catalog_list[0]}.{model_list[0]}.{exp_list[0]}.r1.{var}.mean.nc'
    file = os.path.join(tmp_path, 'netcdf', filename1)
    assert os.path.exists(file)

    filename2 = f'ensemble.ensemblelatlon.{catalog_list[0]}.{model_list[0]}.{exp_list[0]}.r1.{var}.std.nc'
    file = os.path.join(tmp_path, 'netcdf', filename2)
    assert os.path.exists(file)

    # test if mean is non-zero and variance is zero
    assert ens_latlon.dataset_mean is not None
    assert ens_latlon.dataset_std.all() == 0
 
    # PlotEnsembleLatLon class
    plot_class_arguments = {
        "catalog_list": catalog_list,
        "model_list": model_list,
        "exp_list": exp_list,
        "source_list": source_list,
    }

    # STD values are zero. Therefore we are giving the mean value as std values to test the implementation
    ens_latlon_plot = PlotEnsembleLatLon(
        **plot_class_arguments,
        outputdir=tmp_path,
    )

    plot_arguments = {
        "var": var,
        "dpi": 50,
        "save_pdf": True,
        "save_png": True,
        "title_mean": "Test data",
        "title_std": "Test data",
        "cbar_label": "Test Label",
        "dataset_mean": ens_latlon.dataset_mean,
        "dataset_std": ens_latlon.dataset_mean,
    }

    plot_dict = ens_latlon_plot.plot(**plot_arguments)
    
    assert plot_dict['mean_plot'][0] is not None

    filename1 = f'ensemble.ensemblelatlon.{catalog_list[0]}.{model_list[0]}.{exp_list[0]}.r1.{var}.mean.png'
    file = os.path.join(tmp_path, 'png', filename1)
    assert os.path.exists(file)

    filename2 = f'ensemble.ensemblelatlon.{catalog_list[0]}.{model_list[0]}.{exp_list[0]}.r1.{var}.std.png'
    file = os.path.join(tmp_path, 'png', filename2)
    assert os.path.exists(file)

    filename1 = f'ensemble.ensemblelatlon.{catalog_list[0]}.{model_list[0]}.{exp_list[0]}.r1.{var}.mean.pdf'
    file = os.path.join(tmp_path, 'pdf', filename1)
    assert os.path.exists(file)

    filename2 = f'ensemble.ensemblelatlon.{catalog_list[0]}.{model_list[0]}.{exp_list[0]}.r1.{var}.std.pdf'
    file = os.path.join(tmp_path, 'pdf', filename2)
    assert os.path.exists(file)




    
