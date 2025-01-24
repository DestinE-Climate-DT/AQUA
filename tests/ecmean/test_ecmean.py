"""Tests for ecmean diagnostics"""

import os
import pytest
from aqua import Reader
from aqua.util import load_yaml, ConfigPath
from aqua.diagnostics import PerformanceIndices, GlobalMean

@pytest.fixture
def common_setup(tmp_path):
    """Fixture to set up common configuration and data for tests."""
    Configurer = ConfigPath()
    loglevel = 'warning'
    ecmeandir = os.path.join(Configurer.configdir, 'diagnostics', 'ecmean')
    interface = os.path.join(ecmeandir, 'interface_AQUA_climatedt.yaml')
    config = os.path.join(ecmeandir, 'ecmean_config_climatedt.yaml')
    config = load_yaml(config)
    config['dirs']['exp'] = ecmeandir

    exp = 'era5-hpz3'
    reader = Reader(model='ERA5', exp=exp, source='monthly', catalog='ci', regrid='r100')
    data = reader.retrieve()
    data = reader.regrid(data)

    return {
        "config": config,
        "interface": interface,
        "loglevel": loglevel,
        "outputdir": tmp_path,
        "data": data,
        "exp": exp
    }

@pytest.mark.ecmean
def test_performance_indices(common_setup):
    """Test PerformanceIndices diagnostic."""
    setup = common_setup
    pi = PerformanceIndices(
        setup["exp"], 1990, 1994, numproc=2, config=setup["config"],
        interface=setup["interface"], loglevel=setup["loglevel"],
        outputdir=setup["outputdir"], xdataset=setup["data"]
    )
    pi.prepare()
    pi.run()
    pi.store()
    pi.plot()
    yamlfile = f'{setup["outputdir"]}/YAML/PI4_EC23_era5-hpz3_ClimateDT_r1i1p1f1_1990_1994.yml'
    pdffile = f'{setup["outputdir"]}/PDF/PI4_EC23_era5-hpz3_ClimateDT_r1i1p1f1_1990_1994.pdf'
    assert os.path.exists(yamlfile)
    assert os.path.exists(pdffile)

@pytest.mark.ecmean
def test_global_mean(common_setup):
    """Test GlobalMean diagnostic."""
    setup = common_setup
    gm = GlobalMean(
        setup["exp"], 1990, 1994, numproc=2, config=setup["config"],
        interface=setup["interface"], loglevel=setup["loglevel"],
        outputdir=setup["outputdir"], xdataset=setup["data"]
    )
    gm.prepare()
    gm.run()
    gm.store()
    gm.plot()
    yamlfile = f'{setup["outputdir"]}/YAML/global_mean_era5-hpz3_ClimateDT_r1i1p1f1_1990_1994.yml'
    pdffile = f'{setup["outputdir"]}/PDF/global_mean_era5-hpz3_ClimateDT_r1i1p1f1_1990_1994.pdf'
    assert os.path.exists(yamlfile)
    assert os.path.exists(pdffile)
