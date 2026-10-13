import os

import pytest

from aqua.core.drop import drop_util
from aqua.core.drop.drop_util import estimate_time_chunk_size
from aqua.core.util import replace_intake_vars, replace_urlpath_jinja, replace_urlpath_wildcard


@pytest.fixture
def tmp_directory(tmpdir):
    return str(tmpdir)


@pytest.fixture
def output_directory(tmpdir):
    return str(tmpdir)


@pytest.fixture(params=[("IFS", "test-tco79", "long")])
def lra_arguments(request):
    return request.param


# @pytest.mark.aqua
# def test_opa_catalog_entry(tmp_directory, lra_arguments):
#     model, exp, source = lra_arguments
#     fixer_name = 'fixer'
#     frequency = 'monthly'
#     loglevel = 'WARNING'
#     entry_name = drop_util.opa_catalog_entry(datadir=tmp_directory, model=model, exp=exp,
#                                             source=source, fixer_name=fixer_name, frequency=frequency,
#                                             loglevel=loglevel, catalog='ci')

#     # Create temporary files
#     tmp_file1 = os.path.join(tmp_directory, f'test_{frequency}_mean.nc')
#     with open(tmp_file1, 'w') as f:
#         f.write('Temporary file 1')

#     reader = Reader(model='IFS', exp='test-tco79', source='opa_long', areas=False, loglevel='DEBUG')

#     assert entry_name == 'opa_long'
#     assert reader.esmcat.metadata['fixer_name'] == 'fixer'
#     assert reader.esmcat.describe()['args']['urlpath'] == os.path.join(tmp_directory, f'*{frequency}_mean.nc')


@pytest.mark.aqua
def test_move_tmp_files(tmp_directory, output_directory):
    tmp_file1 = os.path.join(tmp_directory, "file1_tmp.nc")
    tmp_file2 = os.path.join(tmp_directory, "file2.nc")
    output_file1 = os.path.join(output_directory, "file1.nc")
    output_file2 = os.path.join(output_directory, "file2.nc")

    # Create temporary files
    with open(tmp_file1, "w") as f:
        f.write("Temporary file 1")
    with open(tmp_file2, "w") as f:
        f.write("Temporary file 2")

    drop_util.move_tmp_files(tmp_directory, output_directory)

    # assert not os.path.exists(tmp_file1)
    # assert not os.path.exists(tmp_file2)
    assert os.path.exists(output_file1)
    assert os.path.exists(output_file2)


@pytest.mark.aqua
def test_move_tmp_files_zarr(tmpdir):
    """Test that move_tmp_files handles zarr stores correctly"""
    # Create separate tmp and output directories
    tmp_directory = str(tmpdir.mkdir("tmp"))
    output_directory = str(tmpdir.mkdir("output"))

    # Create a zarr store directory structure
    zarr_store1 = os.path.join(tmp_directory, "var1.zarr")
    zarr_store2 = os.path.join(tmp_directory, "var2.zarr")
    os.makedirs(zarr_store1)
    os.makedirs(zarr_store2)

    # Create some dummy zarr metadata files
    with open(os.path.join(zarr_store1, ".zarray"), "w") as f:
        f.write('{"shape": [10, 20]}')
    with open(os.path.join(zarr_store2, ".zarray"), "w") as f:
        f.write('{"shape": [5, 10]}')

    # Expected output locations
    output_zarr1 = os.path.join(output_directory, "var1.zarr")
    output_zarr2 = os.path.join(output_directory, "var2.zarr")

    drop_util.move_tmp_files(tmp_directory, output_directory)

    # Verify stores were moved
    assert os.path.exists(output_zarr1)
    assert os.path.exists(output_zarr2)
    assert os.path.isdir(output_zarr1)
    assert os.path.isdir(output_zarr2)
    assert os.path.exists(os.path.join(output_zarr1, ".zarray"))
    assert os.path.exists(os.path.join(output_zarr2, ".zarray"))


@pytest.mark.aqua
def test_estimate_time_chunk_size():
    """Cover all branches of estimate_time_chunk_size."""
    assert estimate_time_chunk_size("daily") == 37  # fixed-interval
    assert estimate_time_chunk_size("hourly") == 892  # fixed-interval
    assert estimate_time_chunk_size("monthly") == 14  # calendar default
    assert estimate_time_chunk_size("seasonal") == 3  # quarterly branch
    assert estimate_time_chunk_size("yearly") == 1  # yearly branch
    assert estimate_time_chunk_size("foobar") > 0  # fallback exception path


# Test replace_intake_vars
@pytest.mark.aqua
def test_replace_intake_vars():

    path = "./AQUA_tests/models/paperino/pluto"
    assert replace_intake_vars(path, catalog="ci") == "{{ TEST_PATH }}/paperino/pluto"


@pytest.mark.aqua
def test_replace_urlpath_wildcard():
    """Test wildcard replacement in URL paths."""

    # Test that replacement only happens when surrounded by same character
    block = {"args": {"urlpath": "data_r1_data.nc"}}
    result = replace_urlpath_wildcard(block, "r1")
    assert result["args"]["urlpath"] == "data_*_data.nc"

    # Test no replacement when not surrounded by same character
    block = {"args": {"urlpath": "/path/to/r1_data.nc"}}
    result = replace_urlpath_wildcard(block, "r1")
    assert result["args"]["urlpath"] == "/path/to/r1_data.nc"

    # Test edge cases
    assert replace_urlpath_wildcard(block, None) == block
    assert replace_urlpath_wildcard(block, "") == block


@pytest.mark.aqua
def test_replace_urlpath_jinja():
    """Test Jinja template replacement and parameter management."""

    # Test URL replacement when surrounded by same character
    block = {"args": {"urlpath": "data_global_data.nc"}}
    result = replace_urlpath_jinja(block, "global", "region")
    assert result["args"]["urlpath"] == "data_{{region}}_data.nc"

    # Test parameters block creation
    assert result["parameters"]["region"]["default"] == "global"
    assert result["parameters"]["region"]["allowed"] == ["global"]

    # Test adding second value
    result = replace_urlpath_jinja(result, "europe", "region")
    assert "europe" in result["parameters"]["region"]["allowed"]
