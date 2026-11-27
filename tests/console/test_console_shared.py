"""Console catalog, grid, and fixes operations."""

import logging
import os
import shutil
import tempfile

import pytest

from aqua.core.console import install
from aqua.core.gridbuilder.griddeploy import GridDeployer
from aqua.core.util import dump_yaml, load_yaml, to_list

pytestmark = pytest.mark.console


def _add_listed_catalog(mydir, name):
    """List again, as the last one, a catalog whose folder is in the installation."""
    configfile = os.path.join(mydir, ".aqua", "config-aqua.yaml")
    cfg = load_yaml(configfile)
    dump_yaml(configfile, {**cfg, "catalog": [*to_list(cfg["catalog"]), name]})


@pytest.mark.xdist_group(name="console_catalog_operations")
class TestCatalogOperations:
    """Catalog add, update, and remove operations."""

    def test_catalog_operations(self, aqua_install, run_aqua, fake_github, fake_github_repo, capfd):
        """Test catalog add, set, update, remove operations."""
        mydir = aqua_install

        # add two catalogs
        for catalog_name in ["ci", "obs"]:
            run_aqua(["catalog", "add", catalog_name])
            assert os.path.isdir(os.path.join(mydir, ".aqua/catalogs", catalog_name))
            config_file = load_yaml(os.path.join(mydir, ".aqua", "config-aqua.yaml"))
            assert catalog_name in config_file["catalog"]

        # set catalog
        run_aqua(["catalog", "set", "ci"])
        assert os.path.isdir(os.path.join(mydir, ".aqua/catalogs/ci"))
        config_file = load_yaml(os.path.join(mydir, ".aqua", "config-aqua.yaml"))
        assert config_file["catalog"][0] == "ci"

        # a folder that the catalog key does not list is not an installed catalog: no verb acts on it
        stray = os.path.join(mydir, ".aqua/catalogs/stray")
        os.makedirs(stray)
        for verb in ("set", "update", "remove"):
            with pytest.raises(SystemExit) as excinfo:
                run_aqua(["catalog", verb, "stray"])
            assert excinfo.value.code == 1
            assert load_yaml(os.path.join(mydir, ".aqua", "config-aqua.yaml"))["catalog"] == ["ci", "obs"]
            assert os.path.isdir(stray)
        # add refuses it too, without deleting it: the folder is not a catalog that AQUA installed
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["catalog", "add", "stray"])
        assert excinfo.value.code == 1
        assert os.path.isdir(stray)
        capfd.readouterr()
        run_aqua(["catalog", "list"])
        listed = capfd.readouterr().out
        assert "- ci (standard)" in listed and "- obs (standard)" in listed and "stray" not in listed
        os.rmdir(stray)

        # a catalog of the key whose folder is gone stops every verb but remove, with the way to restore it
        obs = os.path.join(mydir, ".aqua/catalogs/obs")
        os.rename(obs, obs + ".moved")
        for command in (["set", "obs"], ["update", "obs"], ["update", "all"], ["list"], ["add", "obs"]):
            with pytest.raises(SystemExit) as excinfo:
                run_aqua(["catalog", *command])
            assert excinfo.value.code == 1
            assert load_yaml(os.path.join(mydir, ".aqua", "config-aqua.yaml"))["catalog"] == ["ci", "obs"]
            assert not os.path.exists(obs)
        # remove is the way out, as for a project without its folders: the catalog is then added again
        run_aqua(["catalog", "remove", "obs"])
        assert load_yaml(os.path.join(mydir, ".aqua", "config-aqua.yaml"))["catalog"] == "ci"
        os.rename(obs + ".moved", obs)
        _add_listed_catalog(mydir, "obs")

        # update the installation files: a modified file of the installation is restored
        fixes = os.path.join(mydir, ".aqua/fixes/ifs.yaml")
        with open(fixes, encoding="utf-8") as f:
            original = f.read()
        with open(fixes, "w", encoding="utf-8") as f:
            f.write("modified\n")
        run_aqua(["-v", "update"])
        with open(fixes, encoding="utf-8") as f:
            assert f.read() == original

        # update a catalog: it is downloaded again, a file added to the installed copy is gone,
        # and the active catalog does not change
        leftover = os.path.join(mydir, ".aqua/catalogs/obs/leftover.txt")
        with open(leftover, "w", encoding="utf-8") as f:
            f.write("not in the repository\n")
        run_aqua(["-v", "catalog", "update", "obs"])
        assert os.path.isfile(os.path.join(mydir, ".aqua/catalogs/obs/catalog.yaml"))
        assert not os.path.exists(leftover)
        assert load_yaml(os.path.join(mydir, ".aqua", "config-aqua.yaml"))["catalog"] == ["ci", "obs"]

        # update from another repository: -r reaches GitHub, also for all, and a wrong format stops before any access
        obs = os.path.join(mydir, ".aqua/catalogs/obs")
        run_aqua(["catalog", "update", "obs", "-r", "someone/some-catalogs"])
        assert fake_github.repositories[-1] == "someone/some-catalogs"
        run_aqua(["catalog", "update", "all", "-r", "someone/some-catalogs"])
        assert fake_github.repositories[-2:] == ["someone/some-catalogs"] * 2
        accesses = len(fake_github.repositories)
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["catalog", "update", "obs", "-r", "notarepository"])
        assert excinfo.value.code == 1
        assert len(fake_github.repositories) == accesses and os.path.isfile(os.path.join(obs, "catalog.yaml"))

        # a failed or interrupted download does not lose the installed catalog: it is put back, without leftovers
        with open(leftover, "w", encoding="utf-8") as f:
            f.write("still here\n")
        for error, raised in ((OSError("connection lost"), SystemExit), (KeyboardInterrupt(), KeyboardInterrupt)):
            fake_github.fail = error
            with pytest.raises(raised):
                run_aqua(["catalog", "update", "obs"])
            assert os.path.isfile(leftover) and os.path.isfile(os.path.join(obs, "catalog.yaml"))
            assert sorted(os.listdir(os.path.join(mydir, ".aqua/catalogs"))) == ["ci", "obs"]

        # a backup left by an update which was killed is the good copy: the next update starts from it
        os.rename(obs, obs + ".backup")
        os.makedirs(obs)
        with pytest.raises(KeyboardInterrupt):
            run_aqua(["catalog", "update", "obs"])
        assert os.path.isfile(leftover)
        assert sorted(os.listdir(os.path.join(mydir, ".aqua/catalogs"))) == ["ci", "obs"]
        fake_github.fail = None

        # as for add, the downloaded catalog must load: if it does not, the installed one is kept
        broken = os.path.join(str(fake_github_repo), "catalogs", "obs", "catalog.yaml")
        os.rename(broken, broken + ".moved")
        try:
            with pytest.raises(SystemExit) as excinfo:
                run_aqua(["catalog", "update", "obs"])
        finally:
            os.rename(broken + ".moved", broken)
        assert excinfo.value.code == 1
        assert os.path.isfile(leftover)
        os.remove(leftover)

        # remove catalog
        run_aqua(["catalog", "remove", "ci"])
        assert not os.path.exists(os.path.join(mydir, ".aqua/catalogs/ci"))

    def test_editable_catalog_operations(self, aqua_install, run_aqua, catalog_copy_path, aqua_caplog, tmp_path):
        """Test editable catalog operations."""
        mydir = aqua_install

        # add catalog with editable option
        run_aqua(["-v", "catalog", "add", "ci", "-e", catalog_copy_path])
        assert os.path.isdir(os.path.join(mydir, ".aqua/catalogs/ci"))

        # update a catalog installed in editable mode: nothing to do, the link is kept
        caplog = aqua_caplog(logging.getLogger("AQUA"))
        run_aqua(["-v", "catalog", "update", "ci"])
        assert os.path.islink(os.path.join(mydir, ".aqua/catalogs/ci"))
        assert "ci is installed in editable mode: it follows its repository, nothing to update" in [
            r.getMessage() for r in caplog.records
        ]

        # a positional catalog name is invalid CLI syntax
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["-v", "update", "ci"])
        assert excinfo.value.code == 2

        # remove existing catalog from link
        run_aqua(["catalog", "remove", "ci"])
        assert not os.path.exists(os.path.join(mydir, ".aqua/catalogs/ci"))

        # an editable catalog whose source was deleted is still removed, also from the catalog key
        configfile = os.path.join(mydir, ".aqua", "config-aqua.yaml")
        source = tmp_path / "gone"
        shutil.copytree(catalog_copy_path, source)
        run_aqua(["catalog", "add", "gone", "-e", str(source)])
        shutil.rmtree(source)
        run_aqua(["catalog", "remove", "gone"])
        assert not os.path.lexists(os.path.join(mydir, ".aqua/catalogs/gone"))
        assert "gone" not in to_list(load_yaml(configfile)["catalog"])


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
        run_aqua(["-v", "grids", "path", os.path.join(mydir, "pippo")])
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
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["fixes", "add", fixtest])
        assert excinfo.value.code == 1
        assert not os.path.exists(os.path.join(mydir, ".aqua/fixes/antani.yaml"))

        # add a file which does not exist
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["grids", "add", os.path.join(mydir, "missing.yaml")])
        assert excinfo.value.code == 1
        assert not os.path.lexists(os.path.join(mydir, ".aqua/grids/missing.yaml"))

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

        # a link whose source was deleted is still removed
        run_aqua(["-v", "grids", "add", gridtest, "-e"])
        os.remove(gridtest)
        run_aqua(["-v", "grids", "remove", "garelli.yaml"])
        assert not os.path.lexists(os.path.join(mydir, ".aqua/grids/garelli.yaml"))

        # error for already non existing file
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["-v", "fixes", "remove", "ciccio.yaml"])
        assert excinfo.value.code == 1

        # set the grids path in the config-aqua.yaml with the block already existing
        run_aqua(["-v", "grids", "path", os.path.join(mydir, "pippo")])
        run_aqua(["-v", "grids", "path", os.path.join(mydir, "pluto")])
        assert os.path.exists(os.path.join(mydir, "pluto", "grids"))
        config_file = load_yaml(os.path.join(mydir, ".aqua", "config-aqua.yaml"))
        assert config_file["paths"] == {
            "grids": os.path.join(mydir, "pluto", "grids"),
            "areas": os.path.join(mydir, "pluto", "areas"),
            "weights": os.path.join(mydir, "pluto", "weights"),
        }

    @pytest.mark.parametrize("noun, shipped", [("grids", "healpix.yaml"), ("fixes", "ifs.yaml"), ("styles", "aqua.mplstyle")])
    def test_file_nouns_list(self, aqua_install, run_aqua, capfd, noun, shipped):
        """`aqua <noun> list` lists the files of that folder only: a wrong folder misses the shipped file."""
        mydir = aqua_install
        capfd.readouterr()
        run_aqua([noun, "list"])
        out, _ = capfd.readouterr()
        assert f"AQUA current installed {noun} in {os.path.join(mydir, '.aqua', noun)}:" in out
        assert os.path.join(mydir, ".aqua", noun, shipped) in out

    def test_styles_operations(self, aqua_install, run_aqua):
        """`aqua styles add/remove` installs and removes a matplotlib style file; a YAML file is not a style."""
        mydir = aqua_install
        style = os.path.join(mydir, "mystyle.mplstyle")
        with open(style, "w", encoding="utf-8") as f:
            f.write("lines.linewidth: 3\n")

        run_aqua(["styles", "add", style, "-e"])
        assert os.path.islink(os.path.join(mydir, ".aqua/styles/mystyle.mplstyle"))
        run_aqua(["styles", "remove", "mystyle.mplstyle"])
        assert not os.path.lexists(os.path.join(mydir, ".aqua/styles/mystyle.mplstyle"))

        notastyle = os.path.join(mydir, "notastyle.yaml")
        dump_yaml(notastyle, {"grids": {}})
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["styles", "add", notastyle])
        assert excinfo.value.code == 1
        assert not os.path.exists(os.path.join(mydir, ".aqua/styles/notastyle.yaml"))

    def test_grids_deploy_entrypoint(self, aqua_install, run_aqua, monkeypatch):
        """Test that `aqua grids deploy` reaches GridDeployer with a realistic grid name."""
        mydir = aqua_install
        called = []

        def fake_deploy(self, source_grid_name):
            called.append(source_grid_name)

        monkeypatch.setattr(GridDeployer, "deploy", fake_deploy)

        # Keep the call realistic: configure default paths before deploy.
        run_aqua(["-v", "grids", "path", os.path.join(mydir, "deploy-target")])
        run_aqua(["-v", "grids", "deploy", "hpz1-nested"])

        assert called == ["hpz1-nested"]


@pytest.mark.xdist_group(name="console_catalog_listing_errors")
class TestCatalogListingAndErrors:
    """Catalog listing and error handling operations."""

    def test_console_list(self, aqua_install, run_aqua, capfd, catalog_copy_path, fake_github):
        """Basic tests for list command"""

        # getting fixture

        run_aqua(["catalog", "add", "ci"])
        run_aqua(["catalog", "add", "ciccio", "-e", catalog_copy_path])
        run_aqua(["catalog", "list"])
        out, _ = capfd.readouterr()
        assert "AQUA catalogs installed in" in out
        assert "- ciccio (editable, " in out
        assert "ifs.yaml" not in out

        run_aqua(["list", "-a"])

        out, _ = capfd.readouterr()
        assert "AQUA catalogs installed in" in out
        assert "ci" in out
        assert "- ciccio (editable, " in out
        assert "ifs.yaml" in out
        assert "healpix.yaml" in out

        run_aqua(["catalog", "avail", "--repository", "DestinE-Climate-DT/Climate-DT-catalog"])
        out, _ = capfd.readouterr()

        assert all(f"'{name}'" in out for name in ("ci", "climatedt-gen2", "obs"))

        run_aqua(["-v", "catalog", "update", "all"])

        out, _ = capfd.readouterr()
        assert ".aqua/catalogs/ci .." in out

    def test_console_nonexistent_catalog_from_existing_repo(self, aqua_install, run_aqua, fake_github):
        """Test adding a non-existing catalog from an existing GitHub repository"""

        # Try to add a catalog that doesn't exist in the repository
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(
                ["catalog", "add", "nonexistent-catalog-test-xyz", "--repository", "DestinE-Climate-DT/Climate-DT-catalog"]
            )
        assert excinfo.value.code == 1

    @pytest.mark.parametrize("is_editable", [False, True])
    def test_add_catalog_cleanup_on_failure(self, aqua_install, run_aqua, monkeypatch, fake_github, is_editable):
        """Test that failed catalog additions are properly cleaned up without remote access."""
        mydir = aqua_install
        # the standard one must be in the repository, or the download would not start
        catalog_name = "cleanup_test_editable" if is_editable else "climatedt-gen2"
        catalog_path = os.path.join(mydir, ".aqua/catalogs", catalog_name)

        if is_editable:
            # For editable, mock a write failure
            with tempfile.TemporaryDirectory() as src_dir:
                # Create minimal valid catalog
                with open(os.path.join(src_dir, "catalog.yaml"), "w", encoding="utf-8") as f:
                    f.write("sources: {}")
                with open(os.path.join(src_dir, "machine.yaml"), "w", encoding="utf-8") as f:
                    f.write("{}")

                # define a mock failing dump_yaml function to fail during _catalog_key_set
                def failing_dump_yaml(filepath, data):
                    if "config-aqua.yaml" in filepath:
                        raise PermissionError("Simulated write failure")
                    return dump_yaml(filepath, data)

                # replace the dump_yaml function with the mock failing function
                monkeypatch.setattr(install, "dump_yaml", failing_dump_yaml)

                with pytest.raises(SystemExit) as excinfo:
                    run_aqua(["-v", "catalog", "add", catalog_name, "-e", src_dir])
        else:
            # the download fails after the catalog directory was created
            fake_github.fail = OSError("connection lost")
            with pytest.raises(SystemExit) as excinfo:
                run_aqua(["-v", "catalog", "add", catalog_name])

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
            run_aqua(["catalog", "update", "non_existent_catalog"])
        assert excinfo.value.code == 1


# verb-first commands of AQUA < 1.3, replaced by `aqua catalog <verb>` and `aqua grids path`
REMOVED_COMMANDS = [
    ["add", "ci"],
    ["remove", "ci"],
    ["set", "ci"],
    ["avail"],
    ["update", "-c", "ci"],
    ["grids", "set", "path"],
]


@pytest.mark.parametrize("command", REMOVED_COMMANDS, ids=[" ".join(c[:2]) for c in REMOVED_COMMANDS])
def test_removed_commands_are_rejected(run_aqua, command):
    """The removed commands are a usage error (exit code 2): a leftover alias would run its handler instead."""
    with pytest.raises(SystemExit) as excinfo:
        run_aqua(command)
    assert excinfo.value.code == 2
