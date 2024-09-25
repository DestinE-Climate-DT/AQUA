"""Module for tests for AQUA cli"""

import os
import shutil
import sys
import subprocess
import pytest
from aqua.cli.main import AquaConsole, query_yes_no
from aqua.util import dump_yaml, load_yaml
from aqua import __version__ as version
from aqua import __path__ as pypath
from aqua.cli.diagnostic_config import diagnostic_config

testfile = 'testfile.txt'
machine = 'github'

def set_args(args):
    """Helper function to simulate command line arguments"""
    sys.argv = ['aqua'] + args


@pytest.fixture(scope="session")
def tmpdir(tmp_path_factory):
    """Fixture to create a temporary directory"""
    mydir = tmp_path_factory.mktemp('tmp')
    yield mydir
    shutil.rmtree(str(mydir))


@pytest.fixture
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


@pytest.fixture
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
        testfile = os.path.join(tmpdir, 'testfile')
        with open(testfile, 'w') as f:
            f.write(input_text)
        sys.stdin = open(testfile)
        aquacli = AquaConsole()
        aquacli.execute()
        sys.stdin.close()
        os.remove(testfile)
    return _run_aqua_console


@pytest.fixture
def run_aqua():
    """Fixture to run AQUA console with some interactive command"""
    def _run_aqua_console(args):
        set_args(args)
        aquacli = AquaConsole()
        aquacli.execute()
    return _run_aqua_console

def verify_config_files(base_dir, diagnostic_config):
    """
    Verify that the configuration files were copied correctly.

    Args:
        base_dir (str): The base directory where the files should be copied.
        diagnostic_config (dict): The diagnostic configuration dictionary.

    Returns:
        bool: True if all files are present, False otherwise.
    """
    all_files_present = True
    for diagnostic, configs in diagnostic_config.items():
        for config in configs:
            target_path = os.path.join(base_dir, config['target_path'], config['config_file'])
            print(f"Checking file: {target_path}")
            if not os.path.isfile(target_path):
                print(f"Missing file: {target_path}")
                all_files_present = False
            else:
                print(f"File exists: {target_path}")
    return all_files_present


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
        run_aqua(['install', machine])
        assert os.path.isdir(os.path.join(mydir, '.aqua'))
        assert os.path.isfile(os.path.join(mydir, '.aqua', 'config-aqua.yaml'))

        # do it twice!
        run_aqua_console_with_input(['-vv', 'install', machine], 'yes')
        assert os.path.exists(os.path.join(mydir, '.aqua'))
        for folder in ['fixes', 'data_models', 'grids']:
            assert os.path.isdir(os.path.join(mydir, '.aqua', folder))

        # add two catalogs
        for catalog in ['ci', 'levante']:
            run_aqua(['add', catalog])
            assert os.path.isdir(os.path.join(mydir, '.aqua/catalogs', catalog))
            config_file = load_yaml(os.path.join(mydir, '.aqua', 'config-aqua.yaml'))
            assert catalog in config_file['catalog']

        # add unexesting catalog from path
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['add', 'config/ueeeeee/ci'])
            assert excinfo.value.code == 1

        # add non existing catalog from default
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['-v', 'add', 'antani'])
            assert excinfo.value.code == 1

        # add existing folder which is not a catalog
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['add', 'config/fixes'])
            assert excinfo.value.code == 1

        # set catalog
        run_aqua(['set', 'ci'])
        assert os.path.isdir(os.path.join(mydir, '.aqua/catalogs/ci'))
        config_file = load_yaml(os.path.join(mydir, '.aqua', 'config-aqua.yaml'))
        assert config_file['catalog'][0] == 'ci'

        # set non existing catalog
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['-v', 'set', 'ciccio'])
            assert excinfo.value.code == 1

        # add catalog again and error
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['-v', 'add', 'ci'])
            assert excinfo.value.code == 1

        # update the installation files
        run_aqua(['-v', 'update'])
        assert os.path.isdir(os.path.join(mydir, '.aqua/fixes'))

        # update a catalog
        run_aqua(['-v', 'update', '-c', 'ci'])
        assert os.path.isdir(os.path.join(mydir, '.aqua/catalogs/ci'))

        # remove non-existing catalog
        os.makedirs(os.path.join(mydir, '.aqua/catalogs/ci'), exist_ok=True)
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['remove', 'pippo'])
            assert excinfo.value.code == 1

        # create a test for LRA
        with pytest.raises(ValueError, match="ERROR: lra_config.yaml not found: you need to have this configuration file!"):
            run_aqua(['lra'])

        # create a test for catgen
        with pytest.raises(ValueError, match="ERROR: config.yaml not found: you need to have this configuration file!"):
            run_aqua(['catgen', '--config', 'config.yaml'])

        # remove catalog
        run_aqua(['remove', 'ci'])
        assert not os.path.exists(os.path.join(mydir, '.aqua/catalogs/ci'))
        assert os.path.exists(os.path.join(mydir, '.aqua'))

        # uninstall and say no
        with pytest.raises(SystemExit) as excinfo:
            run_aqua_console_with_input(['uninstall'], 'no')
            assert excinfo.value.code == 0
            assert os.path.exists(os.path.join(mydir, '.aqua'))

        # uninstall and say yes
        run_aqua_console_with_input(['uninstall'], 'yes')
        assert not os.path.exists(os.path.join(mydir, '.aqua'))

    def test_console_lra(self, tmpdir, set_home, run_aqua, run_aqua_console_with_input): 
        """Test for running the LRA via the console"""

        mydir = str(tmpdir)
        set_home(mydir)

        # aqua install
        run_aqua(['install', machine])
        run_aqua(['add', 'ci'])

        # create fake config file
        lratest = os.path.join(mydir, 'faketrip.yaml')
        dump_yaml(lratest,{
                'target': {
                    'resolution': 'r200',
                    'frequency': 'monthly'
                },
                'paths': {
                    'outdir': os.path.join(mydir, 'lra_test'),
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

        # run the LRA and verify that at least one file exist
        run_aqua(['lra', '--config', lratest, '-w', '1', '-d'])
        path = os.path.join(os.path.join(mydir, 'lra_test'),
                            "IFS/test-tco79/r200/monthly/2t_test-tco79_r200_monthly_202002.nc")
        assert os.path.isfile(path)
        
        # remove aqua
        run_aqua_console_with_input(['uninstall'], 'yes')


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
        run_aqua(['install', machine])
        assert os.path.exists(os.path.join(mydir, '.aqua'))

        # add catalog with editable option
        run_aqua(['-v', 'add', 'ci', '-e', 'AQUA_tests/catalog_copy'])
        assert os.path.isdir(os.path.join(mydir, '.aqua/catalogs/ci'))

        # add catalog again and error
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['-v', 'add', 'ci', '-e', 'config/catalogs/ci'])
            assert excinfo.value.code == 1
        assert os.path.exists(os.path.join(mydir, '.aqua/catalogs/ci'))

        # error for update an editable catalog
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['-v', 'update', 'ci'])
            assert excinfo.value.code == 1

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

        # add wrong fix file
        fixtest = os.path.join(mydir, 'antani.yaml')
        dump_yaml(fixtest, {'fixer_name':  'antani'})
        run_aqua(['fixes', 'add', fixtest])
        assert not os.path.exists(os.path.join(mydir, '.aqua/fixes/antani.yaml'))

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

        # error for already existing file
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['-v', 'grids', 'add', gridtest, '-e'])
            assert excinfo.value.code == 1

        # add non existing grid file
        run_aqua(['-v', 'grids', 'remove', 'garelli.yaml'])
        assert not os.path.exists(os.path.join(mydir, '.aqua/grids/garelli.yaml'))

        # error for already non existing file
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['-v', 'fixes', 'remove', 'ciccio.yaml'])
            assert excinfo.value.code == 1

        # uninstall everything
        run_aqua_console_with_input(['uninstall'], 'yes')
        assert not os.path.exists(os.path.join(mydir, '.aqua'))

    def test_install_copies_config_files(self, tmpdir, set_home, run_aqua):
        """Test that configuration files are copied correctly during install.

        Args:
            tmpdir (str): Temporary directory
            set_home (fixture): Fixture to modify the HOME environment variable
            run_aqua (fixture): Fixture to run AQUA console with some interactive command
        """
        # Setup temporary home directory
        mydir = str(tmpdir)
        set_home(mydir)

        # Run aqua install
        run_aqua(['install', machine])

        # Verify the configuration files were copied correctly
        assert verify_config_files(os.path.join(mydir, '.aqua'), diagnostic_config)

    def test_console_with_links(self, tmpdir, set_home, run_aqua_console_with_input):

        # getting fixture
        mydir = str(tmpdir)
        set_home(mydir)

        # check unexesting installation
        with pytest.raises(SystemExit) as excinfo:
            run_aqua_console_with_input(['-v', 'install', machine, '-p', 'environment.yml'], 'yes')
            assert excinfo.value.code == 1

        # install from path with grids
        # run_aqua_console_with_input(['-v', 'install', '-g', os.path.join(mydir, 'supercazzola')], 'yes')
        # assert os.path.exists(os.path.join(mydir, '.aqua'))

        # uninstall everything
        # run_aqua_console_with_input(['uninstall'], 'yes')
        # assert not os.path.exists(os.path.join(mydir,'.aqua'))

        # install from path
        run_aqua_console_with_input(['-v', 'install', machine, '-p', os.path.join(mydir, 'vicesindaco')], 'yes')
        assert os.path.exists(os.path.join(mydir, 'vicesindaco'))

        # uninstall everything again
        run_aqua_console_with_input(['uninstall'], 'yes')
        assert not os.path.exists(os.path.join(mydir, '.aqua'))

    def test_console_editable(self, tmpdir, run_aqua, set_home, run_aqua_console_with_input):

        # getting fixture
        mydir = str(tmpdir)
        set_home(mydir)

        # check unexesting installation
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['-vv', 'install', machine, '-e', '.'])
            assert excinfo.value.code == 1

        # install from path with grids
        run_aqua(['-vv', 'install', machine, '--editable', 'config'])
        assert os.path.exists(os.path.join(mydir, '.aqua'))
        for folder in ['fixes', 'data_models', 'grids']:
            assert os.path.islink(os.path.join(mydir, '.aqua', folder))
        assert os.path.isdir(os.path.join(mydir, '.aqua', 'catalogs'))

        # install from path in editable mode
        run_aqua_console_with_input(['-vv', 'install', machine, '--editable', 'config',
                                     '--path', os.path.join(mydir, 'vicesindaco2')], 'yes')
        assert os.path.islink(os.path.join(mydir, '.aqua'))
        run_aqua_console_with_input(['uninstall'], 'yes')

        # install from path in editable mode but withoyt aqua link
        run_aqua_console_with_input(['-vv', 'install', machine, '--editable', 'config',
                                     '--path', os.path.join(mydir, 'vicesindaco1')], 'no')
        assert not os.path.exists(os.path.join(mydir, '.aqua'))
        assert os.path.isdir(os.path.join(mydir, 'vicesindaco1', 'catalogs'))

        # uninstall everything again, using AQUA_CONFIG env variable
        os.environ['AQUA_CONFIG'] = os.path.join(mydir, 'vicesindaco1')
        run_aqua_console_with_input(['uninstall'], 'yes')
        assert not os.path.exists(os.path.join(mydir, 'vicesindaco1'))
        del os.environ['AQUA_CONFIG']

        assert not os.path.exists(os.path.join(mydir, '.aqua'))

    # base set of tests for list
    def test_console_list(self, tmpdir, run_aqua, set_home, capfd, run_aqua_console_with_input):

        # getting fixture
        mydir = str(tmpdir)
        set_home(mydir)

        # aqua install
        run_aqua(['install', machine])
        run_aqua(['add', 'ci'])
        run_aqua(['add', 'ciccio', '-e', 'AQUA_tests/catalog_copy'])
        run_aqua(['list', '-a'])

        out, _ = capfd.readouterr()
        assert 'AQUA current installed catalogs in' in out
        assert 'ci' in out
        assert 'ciccio (editable' in out
        assert 'IFS.yaml' in out
        assert 'HealPix.yaml' in out
        assert 'ifs2cds.json' in out

        run_aqua(['avail'])
        out, _ = capfd.readouterr()

        assert 'climatedt-phase1' in out
        assert 'lumi-phase1' in out

        # uninstall everything again
        run_aqua_console_with_input(['uninstall'], 'yes')
        assert not os.path.exists(os.path.join(mydir, '.aqua'))

    def test_console_without_home(self, delete_home, run_aqua, tmpdir, run_aqua_console_with_input):

        # getting fixture
        delete_home()
        mydir = str(tmpdir)

        print(f"HOME is set to: {os.environ.get('HOME')}")

        # check unexesting installation
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['install', machine])
            assert excinfo.value.code == 1

        # install from path without home
        shutil.rmtree(os.path.join(mydir, 'vicesindaco'))
        run_aqua_console_with_input(['-v', 'install', machine, '-p', os.path.join(mydir, 'vicesindaco')], 'yes')
        assert os.path.isdir(os.path.join(mydir, 'vicesindaco'))
        assert os.path.isfile(os.path.join(mydir, 'vicesindaco', 'config-aqua.yaml'))
        assert not os.path.exists(os.path.join(mydir, '.aqua'))


# checks for query function
@pytest.fixture
def run_query_with_input(tmpdir):
    def _run_query(input_text, default_answer):
        testfile = os.path.join(tmpdir, 'testfile')
        with open(testfile, 'w') as f:
            f.write(input_text)
        sys.stdin = open(testfile)
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
