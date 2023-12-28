import pytest

from aqua import Reader

loglevel = "DEBUG"


@pytest.mark.aqua
def test_info_intake(capsys):
    """Test the info method of the Reader class with intake"""
    reader = Reader(model='NEMO', exp='test-eORCA1', source='long-2d',
                    regrid='r100', loglevel=loglevel)
    reader.info()
    captured = capsys.readouterr()

    assert "Reader for model NEMO, experiment test-eORCA1, source long-2d" in captured.out
    assert "Data fixing is active:" in captured.out
    assert "Fixes: None" in captured.out
    assert "Regridding is active:" in captured.out
    assert "Target grid is r100" in captured.out
    assert "Regridding method is ycon" in captured.out
    assert "Metadata:" in captured.out
    assert "source_grid_name: eORCA1-2d" in captured.out
    assert "dims: {'ncells': 120184, 'time': 6}" in captured.out
    assert "data_vars: {'sst': ['lon', 'lat', 'level', 'time']}" in captured.out
    assert "coords: ('lon', 'lat', 'level', 'time')" in captured.out
