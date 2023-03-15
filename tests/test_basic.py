import pytest
import numpy as np

# from aqua import aqua

from aqua import Reader, catalogue


def test_aqua_import():
    assert True


def test_aqua_catalogue():
    catalog = catalogue()
    assert len(catalog) > 0


def test_init():
    reader = Reader(model="FESOM", exp="test-pi", source="original_2d")
    assert reader.model == "FESOM"
    assert reader.exp == "test-pi"
    assert reader.source == "original_2d"


@pytest.fixture
def reader_instance():
    return Reader(model="FESOM", exp="test-pi", source="original_2d", regrid="r200")


def test_retrieve(reader_instance):
    data = reader_instance.retrieve(fix=False)
    assert len(data) > 0
    assert data.a_ice.shape == (2, 3140)


def test_regrid(reader_instance):
    data = reader_instance.retrieve(fix=False)
    sstr = reader_instance.regrid(data["sst"][0:2, :])
    assert sstr.shape == (2, 90, 180)
    assert np.nanmean(sstr[0, :, :].values) == pytest.approx(13.350324258783935)
    assert np.nanmean(sstr[1, :, :].values) == pytest.approx(13.319154700343551)


def test_fldmean(reader_instance):
    data = reader_instance.retrieve(fix=False)
    global_mean = reader_instance.fldmean(data.sst[:2, :])
    assert global_mean.shape == (2,)
    assert global_mean.values[0] == pytest.approx(17.99434183)
    assert global_mean.values[1] == pytest.approx(17.98060367)


# @pytest.fixture(
#     params=[
#         ("IFS", "test-tco2559", "ICMGG_atm2d", "r200", "2t"),
#         # ("IFS", "tco2559-ng5", "ICMU_atm3d", "r200"),
#         # ("FESOM", "tco2559-ng5", "interpolated_global2d", "r200"),
#         ("FESOM", "test-ng5", "original_2d", "r200", "ssh"),
#         # ("FESOM", "tco2559-ng5", "original_3d", "r200"),
#         # ("ICON", "ngc2009", "atm_2d_ml_R02B09"),
#         # ("ICON", "ngc2009", "oce_200m_atmgrid"),
#     ]
# )
# def reader_arguments(request):
#     return request.param


# def test_reader(reader_arguments):
#     model, exp, source, regrid, variable = reader_arguments
#     reader = Reader(model=model, exp=exp, source=source, regrid=regrid)
#     data = reader.retrieve(fix=False)
#     assert len(data) > 0
#     # tasr = reader.regrid(data[variable][0:2, :])
#     # assert tasr[0, :, :].values.mean() == pytest.approx(277.3602686711421)
#     # assert tasr[1, :, :].values.mean() == pytest.approx(277.1929510385794)
