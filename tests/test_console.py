"""Module for tests for AQUA cli"""

import os
import shutil
import sys
import subprocess
import pytest
from aqua.core.console.main import AquaConsole
from aqua.core.console.util import query_yes_no
from aqua.core.util import dump_yaml, load_yaml
from aqua import __version__ as version
from aqua import __path__ as pypath

TESTFILE = 'testfile.txt'
MACHINE = 'github'

pytestmark = [
    pytest.mark.aqua,
    pytest.mark.console
]

def set_args(args):
    """Helper function to simulate command line arguments"""
    sys.argv = ['aqua'] + args


@pytest.fixture(scope="session")
def tmpdir(tmp_path_factory):
    """Fixture to create a temporary directory"""
    mydir = tmp_path_factory.mktemp('tmp')
    yield mydir
    shutil.rmtree(str(mydir))


@pytest.fixture(scope="class")
def set_home():
    """Fixture to modify the HOME environment variable"""
    original_value = os.environ.get('HOME')

    def _modify_home(new_value):
        os.environ['HOME'] = new_value
    yield _modify_home
    os.environ['HOME'] = original_value


@pytest.fixture
def delete_home():
    """Fixture to delete the temporary HOME environment variable"""
    original_value = os.environ.get('HOME')

    def _modify_home():
        del os.environ['HOME']
    yield _modify_home
    os.environ['HOME'] = original_value


@pytest.fixture(scope="class")
def run_aqua_console_with_input(tmpdir):
    """Fixture to run AQUA console with some interactive command

    Args:
        tmpdir (str): temporary directory
    """
    def _run_aqua_console(args, input_text):
        """Run AQUA console with some interactive command

        Args:
            args (list): list of arguments
            input_text (str): input text
        """
        set_args(args)
        myfile = os.path.join(str(tmpdir), TESTFILE)
        with open(myfile, 'w', encoding='utf-8') as f:
            f.write(input_text)
        sys.stdin = open(myfile, 'r', encoding='utf-8')
        aquacli = AquaConsole()
        aquacli.execute()
        sys.stdin.close()
        os.remove(myfile)
    return _run_aqua_console


@pytest.fixture(scope="class")
def run_aqua():
    """Fixture to run AQUA console with some interactive command"""
    def _run_aqua_console(args):
        set_args(args)
        aquacli = AquaConsole()
        aquacli.execute()
    return _run_aqua_console


@pytest.fixture(scope="class")
def shared_aqua_install(tmpdir, set_home, run_aqua, run_aqua_console_with_input):
    """Shared AQUA installation for multiple tests in a class
    
    This fixture installs AQUA once and provides cleanup after all tests in the class.
    """
    mydir = str(tmpdir)
    set_home(mydir)
    
    # Install AQUA once for all tests in class
    run_aqua(['install', MACHINE])
    
    yield mydir
    
    # Cleanup after all tests in class
    if os.path.exists(os.path.join(mydir, '.aqua')):
        run_aqua_console_with_input(['uninstall'], 'yes')


@pytest.mark.aqua
class TestAquaConsole():
    """Class for AQUA console tests"""

    def test_console_install(self):
        """Test for CLI call"""
        # test version
        result = subprocess.run(['aqua', '--version'], check=False, capture_output=True, text=True)
        assert result.stdout.strip() == f'aqua v{version}'

        # test path
        result = subprocess.run(['aqua', '--path'], check=False, capture_output=True, text=True)
        assert pypath[0] == result.stdout.strip()

    # base set of tests
    def test_console_base(self, tmpdir, set_home, run_aqua, run_aqua_console_with_input):
        """Basic tests

        Args:
            tmpdir (str): temporary directory
            set_home (fixture): fixture to modify the HOME environment variable
            run_aqua (fixture): fixture to run AQUA console with some interactive command
            run_aqua_console_with_input (fixture): fixture to run AQUA console with some interactive command
        """

        # getting fixture
        mydir = str(tmpdir)
        set_home(mydir)

        # aqua install
        run_aqua(['install', MACHINE])
        assert os.path.isdir(os.path.join(mydir, '.aqua'))
        assert os.path.isfile(os.path.join(mydir, '.aqua', 'config-aqua.yaml'))

        # do it twice!
        run_aqua_console_with_input(['-vv', 'install', MACHINE], 'yes')
        assert os.path.exists(os.path.join(mydir, '.aqua'))
        for folder in ['fixes', 'data_model', 'grids']:
            assert os.path.isdir(os.path.join(mydir, '.aqua', folder))

        # add unexesting catalog from path
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['add', 'config/ueeeeee/ci'])
            assert excinfo.value.code == 1

        # add non existing catalog from default
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['-v', 'add', 'antani'])
            assert excinfo.value.code == 1

        # add from wrongly formatted repository
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['-v', 'add', 'pippo', '--repository', 'thisisnotauserandrepo'])
            assert excinfo.value.code == 1

        # add existing folder which is not a catalog
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['add', 'config/fixes'])
            assert excinfo.value.code == 1

        # create a test for DROP
        with pytest.raises(FileNotFoundError, match="ERROR: drop_config.yaml not found: you need to have this configuration file!"):
            run_aqua(['drop'])

        # create a test for catgen
        with pytest.raises(FileNotFoundError, match="ERROR: config.yaml not found: you need to have this configuration file!"):
            run_aqua(['catgen', '--config', 'config.yaml'])

        # uninstall and say no
        with pytest.raises(SystemExit) as excinfo:
            run_aqua_console_with_input(['uninstall'], 'no')
            assert excinfo.value.code == 0
            assert os.path.exists(os.path.join(mydir, '.aqua'))

        # uninstall and say yes
        run_aqua_console_with_input(['uninstall'], 'yes')
        assert not os.path.exists(os.path.join(mydir, '.aqua'))

    def test_console_drop(self, tmpdir, set_home, run_aqua, run_aqua_console_with_input): 
        """Test for running DROP via the console"""

        mydir = str(tmpdir)
        set_home(mydir)

        # aqua install
        run_aqua(['install', MACHINE])
        run_aqua(['add', 'ci', '--repository', 'DestinE-Climate-DT/Climate-DT-catalog'])

        # create fake config file
        drop_test = os.path.join(mydir, 'faketrip.yaml')
        dump_yaml(drop_test,{
                'target': {
                    'resolution': 'r200',
                    'frequency': 'monthly',
                    'catalog': 'ci'
                },
                'paths': {
                    'outdir': os.path.join(mydir, 'drop_test'),
                    'tmpdir': os.path.join(mydir, 'tmp')
                },
                'options': {
                    'loglevel': 'INFO'
                },
                'data': {
                    'IFS': {
                        'test-tco79': {
                            'long': {'vars': '2t'} 
                        }
                    }
                }
            }
        )

        # run DROP and verify that at least one file exist
        run_aqua(['drop', '--config', drop_test, '-w', '1', '-d', '--rebuild', '--startdate', '2020-01-01', '--enddate', '2020-03-31'])
        path = os.path.join(os.path.join(mydir, 'drop_test'),
                            "ci/IFS/test-tco79/r1/r200/monthly/mean/global/2t_ci_IFS_test-tco79_r1_r200_monthly_mean_global_202002.nc")
        assert os.path.isfile(path), f"File not found: {path}"

        # run DROP with a different stat and verify that at least one file exist
        run_aqua(['drop', '--config', drop_test, '-w', '1', '-d', '--rebuild', '--stat', 'min'])
        path = os.path.join(os.path.join(mydir, 'drop_test'),
                            "ci/IFS/test-tco79/r1/r200/monthly/min/global/2t_ci_IFS_test-tco79_r1_r200_monthly_min_global_202002.nc")
        assert os.path.isfile(path), f"File not found: {path}"
        
        # remove aqua
        run_aqua_console_with_input(['uninstall'], 'yes')

    @pytest.mark.parametrize("install_args,should_fail", [
        (['install', MACHINE, '--core'], False),
        (['install', MACHINE, '--diagnostics'], True),
    ])
    def test_console_selective_install(self, tmpdir, set_home, run_aqua, run_aqua_console_with_input, 
                                       install_args, should_fail):
        """Test for running selective install via the console (parametrized)"""

        mydir = str(tmpdir)
        set_home(mydir)

        if should_fail:
            # aqua install diagnostics only (should fail)
            with pytest.raises(SystemExit) as excinfo:
                run_aqua(install_args)
                assert excinfo.value.code == 1
        else:
            # aqua install core only
            run_aqua(install_args)
            assert os.path.isdir(os.path.join(mydir, '.aqua'))
            assert os.path.isfile(os.path.join(mydir, '.aqua', 'config-aqua.yaml'))

            # uninstall aqua
            run_aqua_console_with_input(['uninstall'], 'yes')
            assert not os.path.exists(os.path.join(mydir, '.aqua'))

    # def test_console_analysis(self, tmpdir, set_home, run_aqua, run_aqua_console_with_input):
    #     """Test for running the analysis via the console"""

    #     mydir = str(tmpdir)
    #     set_home(mydir)

    #     # aqua install
    #     run_aqua(['install', MACHINE])
    #     run_aqua(['add', 'ci', '--repository', 'DestinE-Climate-DT/Climate-DT-catalog'])

    #     test_dir = os.path.dirname(os.path.abspath(__file__))
    #     config_path = os.path.join(test_dir, 'analysis', 'config.aqua-analysis-test.yaml')

    #     # Run details
    #     catalog = 'ci'
    #     model = 'IFS'
    #     experiment = 'test-tco79'
    #     source = 'teleconnections'
    #     output_dir = os.path.join(mydir, 'output')
    #     regrid = False

    #     # run the analysis and verify that at least one file exist
    #     run_aqua(['analysis', '--config', config_path, '-m', model, '-e', experiment,
    #             '-s', source, '-d', output_dir, '-l', 'debug', '--regrid', regrid])
        
    #     output_path = os.path.join(output_dir, catalog, model, experiment, 'r1')
        
    #     assert os.path.exists(os.path.join(output_path, 'experiment.yaml')), \
    #         "experiment.yaml not found"
        
    #     log_file = os.path.join(output_path, 'dummy-dummy_tool.log')
    #     assert os.path.exists(log_file), \
    #         f"dummy-dummy_tool.log not found. Files in {output_path}: {os.listdir(output_path) if os.path.exists(output_path) else 'directory does not exist'}"
        
    #     # Check if "This is a dummy CLI script that does nothing." is in the log
    #     with open(log_file, 'r', encoding='utf-8') as f:
    #         content = f.read()
    #     assert "This is a dummy CLI script that does nothing." in content, \
    #         "Expected content not found in dummy-dummy_tool.log"
        
    #     assert os.path.exists(os.path.join(output_path, 'setup_checker.log')), \
    #         "setup_checker.log not found"

    #     # remove aqua
    #     run_aqua_console_with_input(['uninstall'], 'yes')


    def test_console_advanced(self, tmpdir, run_aqua, set_home, run_aqua_console_with_input):
        """Advanced tests for editable installation, editable catalog, catalog update,
        add a wrong catalog, uninstall

        Args:
            tmpdir (str): temporary directory
            run_aqua (fixture): fixture to run AQUA console with some interactive command
            set_home (fixture): fixture to modify the HOME environment variable
            run_aqua_console_with_input (fixture): fixture to run AQUA console with some interactive command
        """

        # getting fixture
        mydir = str(tmpdir)
        set_home(mydir)

        # check unexesting installation
        with pytest.raises(SystemExit) as excinfo:
            run_aqua_console_with_input(['uninstall'], 'yes')
            assert excinfo.value.code == 1

        # a new install
        run_aqua(['install', MACHINE])
        assert os.path.exists(os.path.join(mydir, '.aqua'))

        # add catalog again and error
        run_aqua(['-v', 'add', 'ci', '-e', 'AQUA_tests/catalog_copy'])
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['-v', 'add', 'ci', '-e', 'config/catalogs/ci'])
            assert excinfo.value.code == 1
        assert os.path.exists(os.path.join(mydir, '.aqua/catalogs/ci'))

        # error for update an missing catalog
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['-v', 'update', 'antani'])
            assert excinfo.value.code == 1

        # add non existing catalog editable
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['-v', 'add', 'ci', '-e', 'config/catalogs/baciugo'])
            assert excinfo.value.code == 1
        assert not os.path.exists(os.path.join(mydir, '.aqua/catalogs/baciugo'))

        # remove existing catalog from link
        run_aqua(['remove', 'ci'])
        assert not os.path.exists(os.path.join(mydir, '.aqua/catalogs/ci'))


    def test_console_with_links(self, tmpdir, set_home, run_aqua_console_with_input):
        """Advanced tests for installation from path with symlinks"""

        # getting fixture
        mydir = str(tmpdir)
        set_home(mydir)

        # check unexesting installation
        with pytest.raises(SystemExit) as excinfo:
            run_aqua_console_with_input(['-v', 'install', MACHINE, '-p', 'environment.yml'], 'yes')
            assert excinfo.value.code == 1

        # install from path with grids
        # run_aqua_console_with_input(['-v', 'install', '-g', os.path.join(mydir, 'supercazzola')], 'yes')
        # assert os.path.exists(os.path.join(mydir, '.aqua'))

        # uninstall everything
        # run_aqua_console_with_input(['uninstall'], 'yes')
        # assert not os.path.exists(os.path.join(mydir,'.aqua'))

        # install from path
        run_aqua_console_with_input(['-v', 'install', MACHINE, '-p', os.path.join(mydir, 'vicesindaco')], 'yes')
        assert os.path.exists(os.path.join(mydir, 'vicesindaco'))

        # uninstall everything again
        run_aqua_console_with_input(['uninstall'], 'yes')
        assert not os.path.exists(os.path.join(mydir, '.aqua'))

    def test_console_editable(self, tmpdir, run_aqua, set_home, run_aqua_console_with_input):
        """Advanced tests for editable installation from path with editable mode"""

        # getting fixture
        mydir = str(tmpdir)
        set_home(mydir)

        # find the correct AQUA root and config paths
        test_dir = os.path.dirname(os.path.abspath(__file__))  # /path/to/AQUA/tests
        aqua_root = os.path.abspath(os.path.join(test_dir, '..'))  # /path/to/AQUA
        #config_dir = os.path.join(aqua_root, 'config')  # /path/to/AQUA/config

        # check unexesting installation
        #with pytest.raises(SystemExit) as excinfo:
        #    run_aqua(['-vv', 'install', MACHINE, '--core', test_dir])
        #    assert excinfo.value.code == 1

        # install from path with grids
        run_aqua(['-vv', 'install', MACHINE, '--core', aqua_root])
        assert os.path.exists(os.path.join(mydir, '.aqua'))
        for folder in ['fixes', 'data_model', 'grids']:
            assert os.path.islink(os.path.join(mydir, '.aqua', folder))
        assert os.path.isdir(os.path.join(mydir, '.aqua', 'catalogs'))

        # try to install diagnostics only on top of existing installation (should fail)
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['install', MACHINE, '--diagnostics', test_dir])
            assert excinfo.value.code == 1

        # install from path in editable mode
        run_aqua_console_with_input(['-vv', 'install', MACHINE, '--core', aqua_root,
                                     '--path', os.path.join(mydir, 'vicesindaco2')], 'yes')
        assert os.path.islink(os.path.join(mydir, '.aqua'))
        run_aqua_console_with_input(['uninstall'], 'yes')

        # install from path in editable mode but without aqua link
        run_aqua_console_with_input(['-vv', 'install', MACHINE, '--core', aqua_root,
                                     '--path', os.path.join(mydir, 'vicesindaco1')], 'no')
        assert not os.path.exists(os.path.join(mydir, '.aqua'))
        assert os.path.isdir(os.path.join(mydir, 'vicesindaco1', 'catalogs'))

        # uninstall everything again, using AQUA_CONFIG env variable
        os.environ['AQUA_CONFIG'] = os.path.join(mydir, 'vicesindaco1')
        run_aqua_console_with_input(['uninstall'], 'yes')
        assert not os.path.exists(os.path.join(mydir, 'vicesindaco1'))
        del os.environ['AQUA_CONFIG']

        assert not os.path.exists(os.path.join(mydir, '.aqua'))


    def test_console_without_home(self, delete_home, run_aqua, tmpdir, run_aqua_console_with_input):
        """Basic tests without HOME environment variable"""

        # getting fixture
        delete_home()
        mydir = str(tmpdir)

        print(f"HOME is set to: {os.environ.get('HOME')}")

        # check unexesting installation
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['install', MACHINE])
            assert excinfo.value.code == 1

        # install from path without home
        if os.path.exists(os.path.join(mydir, 'vicesindaco')):
            shutil.rmtree(os.path.join(mydir, 'vicesindaco'))
        run_aqua_console_with_input(['-v', 'install', MACHINE, '-p', os.path.join(mydir, 'vicesindaco')], 'yes')
        assert os.path.isdir(os.path.join(mydir, 'vicesindaco'))
        assert os.path.isfile(os.path.join(mydir, 'vicesindaco', 'config-aqua.yaml'))
        assert not os.path.exists(os.path.join(mydir, '.aqua'))


@pytest.mark.aqua
class TestAquaConsoleShared():
    """Tests that share a common AQUA installation to reduce I/O overhead"""

    def test_catalog_operations(self, shared_aqua_install, run_aqua):
        """Test catalog add, set, update, remove operations with shared installation"""
        mydir = shared_aqua_install

        # add two catalogs
        for catalog in ['ci', 'levante']:
            run_aqua(['add', catalog])
            assert os.path.isdir(os.path.join(mydir, '.aqua/catalogs', catalog))
            config_file = load_yaml(os.path.join(mydir, '.aqua', 'config-aqua.yaml'))
            assert catalog in config_file['catalog']

        # set catalog
        run_aqua(['set', 'ci'])
        assert os.path.isdir(os.path.join(mydir, '.aqua/catalogs/ci'))
        config_file = load_yaml(os.path.join(mydir, '.aqua', 'config-aqua.yaml'))
        assert config_file['catalog'][0] == 'ci'

        # update the installation files
        run_aqua(['-v', 'update'])
        assert os.path.isdir(os.path.join(mydir, '.aqua/fixes'))

        # update a catalog
        run_aqua(['-v', 'update', '-c', 'ci'])
        assert os.path.isdir(os.path.join(mydir, '.aqua/catalogs/ci'))

        # remove catalog
        run_aqua(['remove', 'ci'])
        assert not os.path.exists(os.path.join(mydir, '.aqua/catalogs/ci'))

    def test_editable_catalog_operations(self, shared_aqua_install, run_aqua):
        """Test editable catalog operations with shared installation"""
        mydir = shared_aqua_install

        # add catalog with editable option
        run_aqua(['-v', 'add', 'ci', '-e', 'AQUA_tests/catalog_copy'])
        assert os.path.isdir(os.path.join(mydir, '.aqua/catalogs/ci'))

        # update a catalog installed in editable mode (should fail)
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['-v', 'update', '-c', 'ci'])
            assert excinfo.value.code == 1

        # error for update an editable catalog
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['-v', 'update', 'ci'])
            assert excinfo.value.code == 1

        # remove existing catalog from link
        run_aqua(['remove', 'ci'])
        assert not os.path.exists(os.path.join(mydir, '.aqua/catalogs/ci'))

    def test_grids_and_fixes_operations(self, shared_aqua_install, run_aqua):
        """Test grids and fixes operations with shared installation"""
        mydir = shared_aqua_install

        # add mock grid file
        gridtest = os.path.join(mydir, 'supercazzola.yaml')
        dump_yaml(gridtest, {'grids': {'sindaco': {'path': '{{ grids }}/comesefosseantani.nc'}}})
        run_aqua(['-v', 'grids', 'add', gridtest])
        assert os.path.isfile(os.path.join(mydir, '.aqua/grids/supercazzola.yaml'))

        # add mock grid file but editable
        gridtest = os.path.join(mydir, 'garelli.yaml')
        dump_yaml(gridtest, {'grids': {'sindaco': {'path': '{{ grids }}/comesefosseantani.nc'}}})
        run_aqua(['-v', 'grids', 'add', gridtest, '-e'])
        assert os.path.islink(os.path.join(mydir, '.aqua/grids/garelli.yaml'))

        # remove grid file
        run_aqua(['-v', 'grids', 'remove', 'garelli.yaml'])
        assert not os.path.exists(os.path.join(mydir, '.aqua/grids/garelli.yaml'))

        # set the grids path in the config-aqua.yaml
        run_aqua(['-v', 'grids', 'set', os.path.join(mydir, 'pippo')])
        assert os.path.exists(os.path.join(mydir, 'pippo', 'grids'))
        assert os.path.exists(os.path.join(mydir, 'pippo', 'areas'))
        assert os.path.exists(os.path.join(mydir, 'pippo', 'weights'))
        config_file = load_yaml(os.path.join(mydir, '.aqua', 'config-aqua.yaml'))
        assert config_file['paths'] == {
            'grids': os.path.join(mydir, 'pippo', 'grids'),
            'areas': os.path.join(mydir, 'pippo', 'areas'),
            'weights': os.path.join(mydir, 'pippo', 'weights')
        }

        # add wrong fix file
        fixtest = os.path.join(mydir, 'antani.yaml')
        dump_yaml(fixtest, {'fixer_name':  'antani'})
        run_aqua(['fixes', 'add', fixtest])
        assert not os.path.exists(os.path.join(mydir, '.aqua/fixes/antani.yaml'))

        # error for already existing file
        gridtest = os.path.join(mydir, 'garelli.yaml')
        dump_yaml(gridtest, {'grids': {'sindaco': {'path': '{{ grids }}/comesefosseantani.nc'}}})
        run_aqua(['-v', 'grids', 'add', gridtest, '-e'])
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['-v', 'grids', 'add', gridtest, '-e'])
            assert excinfo.value.code == 1

        # remove non existing grid file
        run_aqua(['-v', 'grids', 'remove', 'garelli.yaml'])
        assert not os.path.exists(os.path.join(mydir, '.aqua/grids/garelli.yaml'))

        # error for already non existing file
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['-v', 'fixes', 'remove', 'ciccio.yaml'])
            assert excinfo.value.code == 1

        # set the grids path in the config-aqua.yaml with the block already existing
        run_aqua(['-v', 'grids', 'set', os.path.join(mydir, 'pippo')])
        run_aqua(['-v', 'grids', 'set', os.path.join(mydir, 'pluto')])
        assert os.path.exists(os.path.join(mydir, 'pluto', 'grids'))
        config_file = load_yaml(os.path.join(mydir, '.aqua', 'config-aqua.yaml'))
        assert config_file['paths'] == {
            'grids': os.path.join(mydir, 'pluto', 'grids'),
            'areas': os.path.join(mydir, 'pluto', 'areas'),
            'weights': os.path.join(mydir, 'pluto', 'weights')
        }

        # base set of tests for list
    def test_console_list(self, shared_aqua_install, run_aqua, capfd):
        """Basic tests for list command"""

        # getting fixture
        mydir = shared_aqua_install

        run_aqua(['add', 'ci'])
        run_aqua(['add', 'ciccio', '-e', 'AQUA_tests/catalog_copy'])
        run_aqua(['list', '-a'])

        out, _ = capfd.readouterr()
        assert 'AQUA current installed catalogs in' in out
        assert 'ci' in out
        assert 'ciccio (editable' in out
        assert 'ifs.yaml' in out
        assert 'HealPix.yaml' in out

        run_aqua(['avail', '--repository', 'DestinE-Climate-DT/Climate-DT-catalog'])
        out, _ = capfd.readouterr()

        assert 'climatedt-phase1' in out
        assert 'lumi-phase1' in out

        run_aqua(['-v', 'update', '-c', 'all'])

        out, _ = capfd.readouterr()
        assert '.aqua/catalogs/ci ..' in out


class TestAquaConsoleGridBuilder():
    """Tests for the aqua grids build CLI."""

    @pytest.mark.parametrize(
        "command_args", [
            ['grids', 'build', '--model', 'ERA5', '--exp', 'era5-hpz3', '--source', 'monthly'],
        ]
    )
    def test_aqua_console_gridbuilder(self, run_aqua, command_args, tmpdir):
        """Test the aqua grids build CLI"""
        run_aqua(command_args + ['--verify', '--outdir', str(tmpdir)])

# checks for query function
@pytest.fixture
def run_query_with_input(tmpdir):
    def _run_query(input_text, default_answer):
        testfile = os.path.join(tmpdir, TESTFILE)
        with open(testfile, 'w', encoding='utf-8') as f:
            f.write(input_text)
        sys.stdin = open(testfile, 'r', encoding='utf-8')
        try:
            result = query_yes_no("Question?", default_answer)
        finally:
            sys.stdin.close()
            os.remove(testfile)
        return result
    return _run_query


@pytest.mark.aqua
class TestQueryYesNo:
    """Class for query_yes_no tests"""

    def test_query_yes_no_invalid_input(self, run_query_with_input):
        result = run_query_with_input("invalid\nyes", "yes")
        assert result is True

    def test_query_yes_no_explicit_yes(self, run_query_with_input):
        result = run_query_with_input("yes", "no")
        assert result is True

    def test_query_yes_no_explicit_no(self, run_query_with_input):
        result = run_query_with_input("no", "yes")
        assert result is False

    def test_query_yes_no_default(self, run_query_with_input):
        result = run_query_with_input("no", None)
        assert result is False
