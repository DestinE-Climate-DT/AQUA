"""Module for tests for AQUA cli"""

import os
import shutil
import subprocess

import pytest

from aqua import __path__ as pypath
from aqua import __version__ as version

MACHINE = "github"

pytestmark = [pytest.mark.aqua, pytest.mark.console]


class TestAquaConsole:
    """Class for AQUA console tests"""

    def test_console_install(self):
        """Test for CLI call"""
        # test version
        result = subprocess.run(["aqua", "--version"], check=False, capture_output=True, text=True)
        assert result.stdout.strip() == f"aqua v{version}"

        # test path
        result = subprocess.run(["aqua", "--path"], check=False, capture_output=True, text=True)
        assert pypath[0] == result.stdout.strip()

    # base set of tests
    def test_console_base(self, tmp_path, set_home, run_aqua, run_aqua_console_with_input):
        """Basic tests

        Args:
            tmp_path (Path): temporary directory
            set_home (fixture): fixture to modify the HOME environment variable
            run_aqua (fixture): fixture to run AQUA console with some interactive command
            run_aqua_console_with_input (fixture): fixture to run AQUA console with some interactive command
        """

        # getting fixture
        mydir = str(tmp_path)
        set_home(mydir)

        # aqua install
        run_aqua(["install", MACHINE])
        assert os.path.isdir(os.path.join(mydir, ".aqua"))
        assert os.path.isfile(os.path.join(mydir, ".aqua", "config-aqua.yaml"))

        # do it twice!
        run_aqua_console_with_input(["-vv", "install", MACHINE], "yes")
        assert os.path.exists(os.path.join(mydir, ".aqua"))
        for folder in ["fixes", "data_model", "grids"]:
            assert os.path.isdir(os.path.join(mydir, ".aqua", folder))

        # add unexesting catalog from path
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["add", "config/ueeeeee/ci"])
            assert excinfo.value.code == 1

        # add non existing catalog from default
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["-v", "add", "antani"])
            assert excinfo.value.code == 1

        # add from wrongly formatted repository
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["-v", "add", "pippo", "--repository", "thisisnotauserandrepo"])
            assert excinfo.value.code == 1

        # add existing folder which is not a catalog
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["add", "config/fixes"])
            assert excinfo.value.code == 1

        # create a test for DROP
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["drop"])

        # create a test for catgen
        with pytest.raises(FileNotFoundError, match="ERROR: config.yaml not found: you need to have this configuration file!"):
            run_aqua(["catgen", "--config", "config.yaml"])

        # uninstall and say no
        with pytest.raises(SystemExit) as excinfo:
            run_aqua_console_with_input(["uninstall"], "no")
            assert excinfo.value.code == 0
            assert os.path.exists(os.path.join(mydir, ".aqua"))

        # uninstall and say yes
        run_aqua_console_with_input(["uninstall"], "yes")
        assert not os.path.exists(os.path.join(mydir, ".aqua"))

    @pytest.mark.parametrize(
        "install_args,should_fail",
        [
            (["install", MACHINE, "--core"], False),
            (["install", MACHINE, "--diagnostics"], True),
        ],
    )
    def test_console_selective_install(
        self, tmp_path, set_home, run_aqua, run_aqua_console_with_input, install_args, should_fail
    ):
        """Test for running selective install via the console (parametrized)"""

        mydir = str(tmp_path)
        set_home(mydir)

        if should_fail:
            # aqua install diagnostics only (should fail)
            with pytest.raises(SystemExit) as excinfo:
                run_aqua(install_args)
                assert excinfo.value.code == 1
        else:
            # aqua install core only
            run_aqua(install_args)
            assert os.path.isdir(os.path.join(mydir, ".aqua"))
            assert os.path.isfile(os.path.join(mydir, ".aqua", "config-aqua.yaml"))

            # uninstall aqua
            run_aqua_console_with_input(["uninstall"], "yes")
            assert not os.path.exists(os.path.join(mydir, ".aqua"))

    def test_console_advanced(self, tmp_path, run_aqua, set_home, run_aqua_console_with_input):
        """Advanced tests for editable installation, editable catalog, catalog update,
        add a wrong catalog, uninstall

        Args:
            tmp_path (pathlib.Path): temporary directory
            run_aqua (fixture): fixture to run AQUA console with some interactive command
            set_home (fixture): fixture to modify the HOME environment variable
            run_aqua_console_with_input (fixture): fixture to run AQUA console with some interactive command
        """

        # getting fixture
        mydir = str(tmp_path)
        set_home(mydir)

        # check unexesting installation
        with pytest.raises(SystemExit) as excinfo:
            run_aqua_console_with_input(["uninstall"], "yes")
            assert excinfo.value.code == 1

        # a new install
        run_aqua(["install", MACHINE])
        assert os.path.exists(os.path.join(mydir, ".aqua"))

        # add catalog again and error
        run_aqua(["-v", "add", "ci", "-e", "AQUA_tests/catalog_copy"])
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["-v", "add", "ci", "-e", "config/catalogs/ci"])
            assert excinfo.value.code == 1
        assert os.path.exists(os.path.join(mydir, ".aqua/catalogs/ci"))

        # error for update an missing catalog
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["-v", "update", "antani"])
            assert excinfo.value.code == 1

        # add non existing catalog editable
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["-v", "add", "ci", "-e", "config/catalogs/baciugo"])
            assert excinfo.value.code == 1
        assert not os.path.exists(os.path.join(mydir, ".aqua/catalogs/baciugo"))

        # remove existing catalog from link
        run_aqua(["remove", "ci"])
        assert not os.path.exists(os.path.join(mydir, ".aqua/catalogs/ci"))

    def test_console_with_links(self, tmp_path, set_home, run_aqua_console_with_input):
        """Advanced tests for installation from path with symlinks"""

        # getting fixture
        mydir = str(tmp_path)
        set_home(mydir)

        # check unexesting installation
        with pytest.raises(SystemExit) as excinfo:
            run_aqua_console_with_input(["-v", "install", MACHINE, "-p", "environment.yml"], "yes")
            assert excinfo.value.code == 1

        # install from path
        run_aqua_console_with_input(["-v", "install", MACHINE, "-p", os.path.join(mydir, "vicesindaco")], "yes")
        assert os.path.exists(os.path.join(mydir, "vicesindaco"))

        # uninstall everything again
        run_aqua_console_with_input(["uninstall"], "yes")
        assert not os.path.exists(os.path.join(mydir, ".aqua"))

    def test_console_editable(self, tmp_path, run_aqua, set_home, run_aqua_console_with_input):
        """Advanced tests for editable installation from path with editable mode"""

        # getting fixture
        mydir = str(tmp_path)
        set_home(mydir)

        # find the correct AQUA root and config paths
        test_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # /path/to/AQUA/tests
        aqua_root = os.path.abspath(os.path.join(test_dir, ".."))  # /path/to/AQUA
        # config_dir = os.path.join(aqua_root, 'config')  # /path/to/AQUA/config

        # install from path with grids
        run_aqua(["-vv", "install", MACHINE, "--core", aqua_root])
        assert os.path.exists(os.path.join(mydir, ".aqua"))
        for folder in ["fixes", "data_model", "grids"]:
            assert os.path.islink(os.path.join(mydir, ".aqua", folder))
        assert os.path.isdir(os.path.join(mydir, ".aqua", "catalogs"))

        # try to install diagnostics only on top of existing installation (should fail)
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["install", MACHINE, "--diagnostics", test_dir])
            assert excinfo.value.code == 1

        # install from path in editable mode
        run_aqua_console_with_input(
            ["-vv", "install", MACHINE, "--core", aqua_root, "--path", os.path.join(mydir, "vicesindaco2")], "yes"
        )
        assert os.path.islink(os.path.join(mydir, ".aqua"))
        run_aqua_console_with_input(["uninstall"], "yes")

        # install from path in editable mode but without aqua link
        run_aqua_console_with_input(
            ["-vv", "install", MACHINE, "--core", aqua_root, "--path", os.path.join(mydir, "vicesindaco1")], "no"
        )
        assert not os.path.exists(os.path.join(mydir, ".aqua"))
        assert os.path.isdir(os.path.join(mydir, "vicesindaco1", "catalogs"))

        # uninstall everything again, using AQUA_CONFIG env variable
        original_aqua_config = os.environ.get("AQUA_CONFIG")
        os.environ["AQUA_CONFIG"] = os.path.join(mydir, "vicesindaco1")
        try:
            run_aqua_console_with_input(["uninstall"], "yes")
            assert not os.path.exists(os.path.join(mydir, "vicesindaco1"))
        finally:
            if original_aqua_config is None:
                os.environ.pop("AQUA_CONFIG", None)
            else:
                os.environ["AQUA_CONFIG"] = original_aqua_config

        assert not os.path.exists(os.path.join(mydir, ".aqua"))

    def test_console_without_home(self, delete_home, run_aqua, tmp_path, run_aqua_console_with_input):
        """Basic tests without HOME environment variable"""

        # getting fixture
        delete_home()
        mydir = str(tmp_path)

        print(f"HOME is set to: {os.environ.get('HOME')}")

        # check unexesting installation
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["install", MACHINE])
            assert excinfo.value.code == 1

        # install from path without home
        if os.path.exists(os.path.join(mydir, "vicesindaco")):
            shutil.rmtree(os.path.join(mydir, "vicesindaco"))
        run_aqua_console_with_input(["-v", "install", MACHINE, "-p", os.path.join(mydir, "vicesindaco")], "yes")
        assert os.path.isdir(os.path.join(mydir, "vicesindaco"))
        assert os.path.isfile(os.path.join(mydir, "vicesindaco", "config-aqua.yaml"))
        assert not os.path.exists(os.path.join(mydir, ".aqua"))
