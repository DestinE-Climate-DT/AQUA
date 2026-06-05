import os
import time

import pytest

from aqua import show_catalog_content
from aqua.core.configurer import ConfigPath


@pytest.mark.aqua
def test_config_plain():
    config = ConfigPath()
    assert config.filename == "config-aqua.yaml"
    assert "ci" in config.catalog_available


@pytest.mark.aqua
def test_config_paths():
    configfile = "tests/config/config-aqua-custom.yaml"

    configdir = ConfigPath().get_config_dir()

    # Copy the file to the config directory
    os.system(f"cp {configfile} {configdir}")

    config = ConfigPath(catalog="ci", filename="config-aqua-custom.yaml", configdir=configdir)
    paths, _ = config.get_machine_info()

    assert paths["paths"]["grids"] == "pluto"

    # Remove the copied file
    os.system(f"rm {configdir}/config-aqua-custom.yaml")


@pytest.mark.aqua
def test_show_catalog_content_basic():
    """Test show_catalog_content with no filters."""
    results = show_catalog_content()

    assert isinstance(results, dict)
    # Check structure: catalog -> model -> exp -> list of sources
    for _, catalog_data in results.items():
        assert isinstance(catalog_data, dict)
        for _, model_data in catalog_data.items():
            assert isinstance(model_data, dict)
            for _, sources in model_data.items():
                assert isinstance(sources, list)


@pytest.mark.aqua
def test_configpath_caching_efficiency():
    """Test that ConfigPath caching saves time on repeated initializations."""
    # Clear cache to ensure a clean test
    ConfigPath.clear_cache()

    # First initialization (cache miss) — measure I/O time
    t_start = time.perf_counter()
    config1 = ConfigPath()
    t_first = time.perf_counter() - t_start

    # Subsequent initializations (cache hits) — should be much faster
    times = []
    for _ in range(10):
        t_start = time.perf_counter()
        config_cached = ConfigPath()
        times.append(time.perf_counter() - t_start)

    # Verify that all cached instances are identical
    assert config_cached is config1, "Cached instances should be the same object"

    t_cached_avg = sum(times) / len(times)

    # Cache hits should be significantly faster (at least 2x, typically >100x)
    speedup_ratio = t_first / t_cached_avg if t_cached_avg > 0 else float("inf")
    print("\nConfigPath caching performance:")
    print(f"  First init (I/O):        {t_first * 1000:.2f} ms")
    print(f"  Cached init (avg):       {t_cached_avg * 1000:.6f} ms")
    print(f"  Speedup ratio:           {speedup_ratio:.1f}x")

    assert speedup_ratio > 2, f"Cached init should be >2x faster, got {speedup_ratio:.1f}x"

    # Clean up for other tests
    ConfigPath.clear_cache()


@pytest.mark.aqua
def test_configpath_cache_independence():
    """Test that different argument combinations use separate cache entries."""
    ConfigPath.clear_cache()

    # Create instances with different arguments
    config_default = ConfigPath()
    config_ci = ConfigPath(catalog="ci")

    # Verify they are different objects
    assert config_default is not config_ci, "Different arguments should create different instances"

    # Both should be cached now
    assert config_default is ConfigPath()
    assert config_ci is ConfigPath(catalog="ci")

    # Clean up
    ConfigPath.clear_cache()


@pytest.mark.aqua
def test_configpath_cache_bypass_with_locator():
    """Test that custom locator objects bypass the cache."""
    from aqua.core.configurer import ConfigLocator

    ConfigPath.clear_cache()

    config1 = ConfigPath()

    # Create a custom locator (should bypass cache)
    custom_locator = ConfigLocator()
    config2 = ConfigPath(locator=custom_locator)

    # These should be different instances despite having the same default arguments
    assert config1 is not config2, "Custom locator should bypass the cache"

    # But re-creating without locator should still use cache
    config3 = ConfigPath()
    assert config1 is config3

    # Clean up
    ConfigPath.clear_cache()
