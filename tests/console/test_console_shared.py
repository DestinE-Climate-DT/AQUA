"""Console catalog, grid, fixes, and analysis operations."""

import os
import tempfile

import pytest

from aqua.core.console import catalog
from aqua.core.gridbuilder.griddeploy import GridDeployer
from aqua.core.util import dump_yaml, load_yaml, to_list

pytestmark = [pytest.mark.aqua, pytest.mark.console]


@pytest.mark.xdist_group(name="console_catalog_operations")
class TestCatalogOperations:
    """Catalog add, update, and remove operations."""

    def test_catalog_operations(self, aqua_install, run_aqua):
        """Test catalog add, set, update, remove operations."""
        mydir = aqua_install

        # add two catalogs
        for catalog_name in ["ci", "obs"]:
            run_aqua(["add", catalog_name])
            assert os.path.isdir(os.path.join(mydir, ".aqua/catalogs", catalog_name))
            config_file = load_yaml(os.path.join(mydir, ".aqua", "config-aqua.yaml"))
            assert catalog_name in config_file["catalog"]

        # set catalog
        run_aqua(["set", "ci"])
        assert os.path.isdir(os.path.join(mydir, ".aqua/catalogs/ci"))
        config_file = load_yaml(os.path.join(mydir, ".aqua", "config-aqua.yaml"))
        assert config_file["catalog"][0] == "ci"

        # update the installation files
        run_aqua(["-v", "update"])
        assert os.path.isdir(os.path.join(mydir, ".aqua/fixes"))

        # update a catalog
        run_aqua(["-v", "update", "-c", "ci"])
        assert os.path.isdir(os.path.join(mydir, ".aqua/catalogs/ci"))

        # remove catalog
        run_aqua(["remove", "ci"])
        assert not os.path.exists(os.path.join(mydir, ".aqua/catalogs/ci"))

    def test_editable_catalog_operations(self, aqua_install, run_aqua):
        """Test editable catalog operations."""
        mydir = aqua_install

        # add catalog with editable option
        run_aqua(["-v", "add", "ci", "-e", "AQUA_tests/catalog_copy"])
        assert os.path.isdir(os.path.join(mydir, ".aqua/catalogs/ci"))

        # update a catalog installed in editable mode (should fail)
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["-v", "update", "-c", "ci"])
            assert excinfo.value.code == 1

        # error for update an editable catalog
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["-v", "update", "ci"])
            assert excinfo.value.code == 1

        # remove existing catalog from link
        run_aqua(["remove", "ci"])
        assert not os.path.exists(os.path.join(mydir, ".aqua/catalogs/ci"))


@pytest.mark.xdist_group(name="console_grid_fix_operations")
class TestGridAndFixOperations:
    """Grid and fixes operations."""

    def test_grids_and_fixes_operations(self, aqua_install, run_aqua):
        """Test grids and fixes operations."""
        mydir = aqua_install

        # add mock grid file
        gridtest = os.path.join(mydir, "supercazzola.yaml")
        dump_yaml(gridtest, {"grids": {"sindaco": {"path": "{{ grids }}/comesefosseantani.nc"}}})
        run_aqua(["-v", "grids", "add", gridtest])
        assert os.path.isfile(os.path.join(mydir, ".aqua/grids/supercazzola.yaml"))

        # add mock grid file but editable
        gridtest = os.path.join(mydir, "garelli.yaml")
        dump_yaml(gridtest, {"grids": {"sindaco": {"path": "{{ grids }}/comesefosseantani.nc"}}})
        run_aqua(["-v", "grids", "add", gridtest, "-e"])
        assert os.path.islink(os.path.join(mydir, ".aqua/grids/garelli.yaml"))

        # remove grid file
        run_aqua(["-v", "grids", "remove", "garelli.yaml"])
        assert not os.path.exists(os.path.join(mydir, ".aqua/grids/garelli.yaml"))

        # set the grids path in the config-aqua.yaml
        run_aqua(["-v", "grids", "set", os.path.join(mydir, "pippo")])
        assert os.path.exists(os.path.join(mydir, "pippo", "grids"))
        assert os.path.exists(os.path.join(mydir, "pippo", "areas"))
        assert os.path.exists(os.path.join(mydir, "pippo", "weights"))
        config_file = load_yaml(os.path.join(mydir, ".aqua", "config-aqua.yaml"))
        assert config_file["paths"] == {
            "grids": os.path.join(mydir, "pippo", "grids"),
            "areas": os.path.join(mydir, "pippo", "areas"),
            "weights": os.path.join(mydir, "pippo", "weights"),
        }

        # add wrong fix file
        fixtest = os.path.join(mydir, "antani.yaml")
        dump_yaml(fixtest, {"fixer_name": "antani"})
        run_aqua(["fixes", "add", fixtest])
        assert not os.path.exists(os.path.join(mydir, ".aqua/fixes/antani.yaml"))

        # error for already existing file
        gridtest = os.path.join(mydir, "garelli.yaml")
        dump_yaml(gridtest, {"grids": {"sindaco": {"path": "{{ grids }}/comesefosseantani.nc"}}})
        run_aqua(["-v", "grids", "add", gridtest, "-e"])
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["-v", "grids", "add", gridtest, "-e"])
            assert excinfo.value.code == 1

        # remove non existing grid file
        run_aqua(["-v", "grids", "remove", "garelli.yaml"])
        assert not os.path.exists(os.path.join(mydir, ".aqua/grids/garelli.yaml"))

        # error for already non existing file
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["-v", "fixes", "remove", "ciccio.yaml"])
            assert excinfo.value.code == 1

        # set the grids path in the config-aqua.yaml with the block already existing
        run_aqua(["-v", "grids", "set", os.path.join(mydir, "pippo")])
        run_aqua(["-v", "grids", "set", os.path.join(mydir, "pluto")])
        assert os.path.exists(os.path.join(mydir, "pluto", "grids"))
        config_file = load_yaml(os.path.join(mydir, ".aqua", "config-aqua.yaml"))
        assert config_file["paths"] == {
            "grids": os.path.join(mydir, "pluto", "grids"),
            "areas": os.path.join(mydir, "pluto", "areas"),
            "weights": os.path.join(mydir, "pluto", "weights"),
        }

    def test_grids_deploy_entrypoint(self, aqua_install, run_aqua, monkeypatch):
        """Test that `aqua grids deploy` reaches GridDeployer with a realistic grid name."""
        mydir = aqua_install
        called = []

        def fake_deploy(self, source_grid_name):
            called.append(source_grid_name)

        monkeypatch.setattr(GridDeployer, "deploy", fake_deploy)

        # Keep the call realistic: configure default paths before deploy.
        run_aqua(["-v", "grids", "set", os.path.join(mydir, "deploy-target")])
        run_aqua(["-v", "grids", "deploy", "hpz1-nested"])

        assert called == ["hpz1-nested"]


@pytest.mark.xdist_group(name="console_catalog_listing_errors")
class TestCatalogListingAndErrors:
    """Catalog listing and error handling operations."""

    def test_console_list(self, aqua_install, run_aqua, capfd):
        """Basic tests for list command"""

        # getting fixture

        run_aqua(["add", "ci"])
        run_aqua(["add", "ciccio", "-e", "AQUA_tests/catalog_copy"])
        run_aqua(["list", "-a"])

        out, _ = capfd.readouterr()
        assert "AQUA current installed catalogs in" in out
        assert "ci" in out
        assert "ciccio (editable" in out
        assert "ifs.yaml" in out
        assert "HealPix.yaml" in out

        run_aqua(["avail", "--repository", "DestinE-Climate-DT/Climate-DT-catalog"])
        out, _ = capfd.readouterr()

        assert "climatedt-gen2" in out
        assert "nextgems4" in out

        run_aqua(["-v", "update", "-c", "all"])

        out, _ = capfd.readouterr()
        assert ".aqua/catalogs/ci .." in out

    def test_console_nonexistent_catalog_from_existing_repo(self, aqua_install, run_aqua):
        """Test adding a non-existing catalog from an existing GitHub repository"""

        # Try to add a catalog that doesn't exist in the repository
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["add", "nonexistent-catalog-test-xyz", "--repository", "DestinE-Climate-DT/Climate-DT-catalog"])
        assert excinfo.value.code == 1

    @pytest.mark.parametrize("is_editable", [False, True])
    def test_add_catalog_cleanup_on_failure(self, aqua_install, run_aqua, monkeypatch, is_editable):
        """Test that failed catalog additions are properly cleaned up without remote access."""
        mydir = aqua_install
        catalog_name = f"cleanup_test_{'editable' if is_editable else 'standard'}"
        catalog_path = os.path.join(mydir, ".aqua/catalogs", catalog_name)

        if is_editable:
            # For editable, mock a write failure
            with tempfile.TemporaryDirectory() as src_dir:
                # Create minimal valid catalog
                with open(os.path.join(src_dir, "catalog.yaml"), "w", encoding="utf-8") as f:
                    f.write("sources: {}")

                # define a mock failing dump_yaml function to fail during _set_catalog
                def failing_dump_yaml(filepath, data):
                    if "config-aqua.yaml" in filepath:
                        raise PermissionError("Simulated write failure")
                    return dump_yaml(filepath, data)

                # replace the dump_yaml function with the mock failing function
                monkeypatch.setattr(catalog, "dump_yaml", failing_dump_yaml)

                with pytest.raises(SystemExit) as excinfo:
                    run_aqua(["-v", "add", catalog_name, "-e", src_dir])
        else:
            # Simulate a failed download after the catalog directory was created.
            def fail_after_partial_download(self, catalog, repository=None):
                os.makedirs(os.path.join(self.configpath, "catalogs", catalog))
                raise SystemExit(1)

            monkeypatch.setattr(catalog.CatalogMixin, "_add_catalog_github", fail_after_partial_download)

            with pytest.raises(SystemExit) as excinfo:
                run_aqua(["-v", "add", catalog_name])

        # both must fail
        assert excinfo.value.code == 1

        # Verify cleanup
        assert not os.path.exists(catalog_path), "Catalog path should be cleaned up"
        if is_editable:
            assert not os.path.islink(catalog_path), "Symlink should be removed"

        # Verify config
        config = load_yaml(os.path.join(mydir, ".aqua", "config-aqua.yaml"))
        assert catalog_name not in to_list(config.get("catalog"))

    def test_update_nonexistent_catalog(self, aqua_install, run_aqua):
        """Test updating a catalog that doesn't exist"""
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["update", "-c", "non_existent_catalog"])
        assert excinfo.value.code == 1
