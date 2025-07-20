"""Tests for ecmean diagnostics"""

import os
import pytest
from aqua import Reader
from aqua.util import load_yaml, ConfigPath
from aqua.diagnostics import PerformanceIndices, GlobalMean
from aqua.diagnostics.core import OutputSaver

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
        "model": 'ERA5',
        "catalog": 'ci',
        "exp": exp
    }

@pytest.mark.diagnostics
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
    outputsaver = OutputSaver(diagnostic='ecmean',
                    catalog=setup['catalog'], model=setup['model'], exp=setup["exp"],
                    outdir=setup['outputdir'], loglevel=setup['loglevel'])
    yamlfile = outputsaver.generate_path(extension='yml', diagnostic_product='performance_indices')
    pdffile = outputsaver.generate_path(extension='pdf', diagnostic_product='performance_indices')
    pi.store(yamlfile=yamlfile)
    ecmean_fig = pi.plot(diagname='performance_indices', returnfig=True, storefig=False)
    outputsaver.save_pdf(fig=ecmean_fig, diagnostic_product='performance_indices')
    assert os.path.exists(yamlfile), f"{yamlfile} file not found"
    assert os.path.exists(pdffile), f"{pdffile} file not found"

@pytest.mark.diagnostics
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
    outputsaver = OutputSaver(diagnostic='ecmean',
                    catalog=setup['catalog'], model=setup['model'], exp=setup["exp"],
                    outdir=setup['outputdir'], loglevel=setup['loglevel'])
    yamlfile = outputsaver.generate_path(extension='yml', diagnostic_product='global_mean')
    pngfile = outputsaver.generate_path(extension='png', diagnostic_product='global_mean')
    gm.store(yamlfile=yamlfile)
    ecmean_fig = gm.plot(diagname='global_mean', returnfig=True, storefig=False)
    outputsaver.save_png(fig=ecmean_fig, diagnostic_product='global_mean')

    assert os.path.exists(yamlfile), f"{yamlfile} file not found"
    assert os.path.exists(pngfile), f"{pngfile} file not found"
