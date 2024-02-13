import pytest

# pytest approximation, to bear with different machines
approx_rel = 1e-4
loglevel = 'DEBUG'


@pytest.mark.parametrize("module_name", ['teleconnections'])
@pytest.mark.teleconnections
def test_import(module_name):
    """
    Test that the module can be imported
    """
    try:
        __import__(module_name)
    except ImportError:
        assert False, "Module {} could not be imported".format(module_name)


@pytest.mark.teleconnections
def test_lon_conversion():
    """
    Test that the lon conversion works
    """
    from teleconnections.tools import lon_180_to_360

    assert lon_180_to_360(-25) == pytest.approx(335, rel=approx_rel)
    assert lon_180_to_360(-75) == pytest.approx(285, rel=approx_rel)
    assert lon_180_to_360(25) == pytest.approx(25, rel=approx_rel)
    assert lon_180_to_360(75) == pytest.approx(75, rel=approx_rel)


@pytest.mark.teleconnections
def test_namelist():
    """
    Test that the namelist can be loaded
    """
    from teleconnections.tools import TeleconnectionsConfig

    configdir = "./diagnostics/teleconnections/config"
    interface = 'teleconnections-ci'
    config = TeleconnectionsConfig(configdir=configdir)
    namelist = config.load_namelist()
    assert len(namelist) > 0


@pytest.mark.teleconnections
@pytest.mark.parametrize("months_window", [1, 3])
def test_station_based(months_window, loglevel=loglevel):
    """
    Test that the station_based method works
    """
    from teleconnections.tools import TeleconnectionsConfig
    from teleconnections.cdo_testing import cdo_station_based_comparison

    filepath = "./AQUA_tests/models/IFS/teleconnections/nao_test.nc"
    configdir = "./diagnostics/teleconnections/config"
    telecname = 'NAO'
    interface = 'teleconnections-ci'
    rtol = approx_rel
    atol = approx_rel

    # 1. -- Opening yaml file
    config = TeleconnectionsConfig(configdir=configdir, interface=interface)
    namelist = config.load_namelist()

    # 2. -- Comparison cdo vs lib method
    cdo_station_based_comparison(infile=filepath, namelist=namelist,
                                 telecname=telecname, rtol=rtol, atol=atol,
                                 months_window=months_window,
                                 loglevel=loglevel)


@pytest.mark.teleconnections
@pytest.mark.parametrize("months_window", [1, 3])
def test_regional_mean(months_window):
    """
    Test that the regional_mean method works
    """
    from teleconnections.tools import TeleconnectionsConfig
    from teleconnections.cdo_testing import cdo_regional_mean_comparison

    filepath = "./AQUA_tests/models/IFS/teleconnections/enso_test.nc"
    configdir = "./diagnostics/teleconnections/config"
    telecname = 'ENSO'
    interface = 'teleconnections-ci'
    rtol = approx_rel
    atol = approx_rel

    # 1. -- Opening yaml file
    config = TeleconnectionsConfig(configdir=configdir, interface=interface)
    namelist = config.load_namelist()

    # 2. -- Comparison cdo vs lib method
    cdo_regional_mean_comparison(infile=filepath, namelist=namelist,
                                 telecname=telecname, rtol=rtol, atol=atol,
                                 months_window=months_window,
                                 loglevel=loglevel)
