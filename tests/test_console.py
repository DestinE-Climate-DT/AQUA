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

testfile = 'testfile.txt'

# Helper function to simulate command line arguments
def set_args(args):
    sys.argv = ['aqua'] + args

# fixture to create temporary directory
@pytest.fixture(scope="session")
def tmpdir(tmp_path_factory):
    mydir = tmp_path_factory.mktemp('tmp')
    yield mydir 
    shutil.rmtree(str(mydir))

# fixture to modify the home directory
@pytest.fixture
def set_home():
    original_value = os.environ.get('HOME')
    def _modify_home(new_value):
        os.environ['HOME'] = new_value
    yield _modify_home
    os.environ['HOME'] = original_value

@pytest.fixture
def delete_home():
    original_value = os.environ.get('HOME')
    def _modify_home():
        del os.environ['HOME']
    yield _modify_home
    os.environ['HOME'] = original_value

# fixture to run AQUA console with some interactive command
@pytest.fixture
def run_aqua_console_with_input(tmpdir):
    def _run_aqua_console(args, input_text):
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

# fixture to run AQUA console with some interactive command
@pytest.fixture
def run_aqua():
    def _run_aqua_console(args):
        set_args(args)
        aquacli = AquaConsole()
        aquacli.execute()
    return _run_aqua_console


@pytest.mark.aqua
class TestAquaConsole():
    """Class for AQUA console tests"""

    def test_console_install(self):
        """Test for CLI call"""

        # test version
        result = subprocess.run(['aqua','--version'], check=False, capture_output=True, text=True)
        assert result.stdout.strip() == f'aqua v{version}'

        # test path
        result = subprocess.run(['aqua','--path'], check=False, capture_output=True, text=True)
        assert pypath[0] == result.stdout.strip()

    # base set of tests
    def test_console_base(self, tmpdir, set_home, run_aqua, run_aqua_console_with_input):
        """Basic tests"""

        # getting fixture
        mydir = str(tmpdir)
        set_home(mydir)

        # aqua install
        run_aqua(['install'])
        assert os.path.isdir(os.path.join(mydir,'.aqua'))
        assert os.path.isfile(os.path.join(mydir,'.aqua', 'config-aqua.yaml'))

        # do it twice!
        run_aqua_console_with_input(['-vv', 'install'], 'yes')
        assert os.path.exists(os.path.join(mydir,'.aqua'))
        for folder in ['fixes', 'data_models', 'grids']:
            assert os.path.isdir(os.path.join(mydir,'.aqua', folder))

        # add two catalogs
        for catalog in ['ci', 'levante']:
            run_aqua(['add', catalog])
            assert os.path.isdir(os.path.join(mydir,'.aqua/catalogs', catalog))
            config_file = load_yaml(os.path.join(mydir,'.aqua', 'config-aqua.yaml'))
            assert catalog in config_file['catalog']

        # add catalog from path
        run_aqua(['add', 'config/catalogs/lumi'])
        assert os.path.isdir(os.path.join(mydir,'.aqua/catalogs/lumi'))
        config_file = load_yaml(os.path.join(mydir,'.aqua', 'config-aqua.yaml'))
        assert 'lumi' in config_file['catalog']

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
        assert os.path.isdir(os.path.join(mydir,'.aqua/catalogs/ci'))
        config_file = load_yaml(os.path.join(mydir,'.aqua', 'config-aqua.yaml'))
        assert config_file['catalog'][0] == 'ci'

        # set non existing catalog
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['-v', 'set', 'ciccio'])
            assert excinfo.value.code == 1

        # add catalog again and error
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['-v', 'add', 'ci'])
            assert excinfo.value.code == 1

        # update a catalog
        run_aqua(['-v', 'update', 'ci'])
        assert os.path.isdir(os.path.join(mydir,'.aqua/catalogs/ci'))

        # remove non-existing catalog
        os.makedirs(os.path.join(mydir,'.aqua/catalogs/ci'), exist_ok=True)
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['remove', 'pippo'])
            assert excinfo.value.code == 1

        # remove catalog
        run_aqua(['remove', 'ci'])
        assert not os.path.exists(os.path.join(mydir,'.aqua/catalogs/ci'))
        assert os.path.exists(os.path.join(mydir,'.aqua'))

        # uninstall and say no
        with pytest.raises(SystemExit) as excinfo:
            run_aqua_console_with_input(['uninstall'], 'no')
            assert excinfo.value.code == 0
            assert os.path.exists(os.path.join(mydir,'.aqua'))

        # uninstall and say yes
        run_aqua_console_with_input(['uninstall'], 'yes')
        assert not os.path.exists(os.path.join(mydir,'.aqua'))
        
    def test_console_advanced(self, tmpdir, run_aqua, set_home, run_aqua_console_with_input):

        # getting fixture
        mydir = str(tmpdir)
        set_home(mydir)
      
        # check unexesting installation
        with pytest.raises(SystemExit) as excinfo:
            run_aqua_console_with_input(['uninstall'], 'yes')
            assert excinfo.value.code == 1

        # a new install
        run_aqua(['install'])
        assert os.path.exists(os.path.join(mydir,'.aqua'))

        # add catalog with editable option
        run_aqua(['-v', 'add', 'ci', '-e', 'config/catalogs/ci'])
        assert os.path.isdir(os.path.join(mydir,'.aqua/catalogs/ci'))

        # add catalog again and error
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['-v', 'add', 'ci', '-e', 'config/catalogs/ci'])
            assert excinfo.value.code == 1
        assert os.path.exists(os.path.join(mydir,'.aqua/catalogs/ci'))

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
        assert not os.path.exists(os.path.join(mydir,'.aqua/catalogs/baciugo'))

        # remove existing catalog from link
        run_aqua(['remove', 'ci'])
        assert not os.path.exists(os.path.join(mydir,'.aqua/catalogs/ci'))

        # add wrong fix file
        fixtest = os.path.join(mydir, 'antani.yaml')
        dump_yaml(fixtest, {'fixer_name':  'antani'})
        run_aqua(['fixes', 'add', fixtest])
        assert not os.path.exists(os.path.join(mydir,'.aqua/fixes/antani.yaml'))

        # add mock grid file
        gridtest = os.path.join(mydir, 'supercazzola.yaml')
        dump_yaml(gridtest, {'grids': {'sindaco': {'path': '{{ grids }}/comesefosseantani.nc'}}})
        run_aqua(['-v','grids', 'add', gridtest])
        assert os.path.isfile(os.path.join(mydir,'.aqua/grids/supercazzola.yaml'))

        # add mock grid file but editable
        gridtest = os.path.join(mydir, 'garelli.yaml')
        dump_yaml(gridtest, {'grids': {'sindaco': {'path': '{{ grids }}/comesefosseantani.nc'}}})
        run_aqua(['-v','grids','add', gridtest, '-e'])
        assert os.path.islink(os.path.join(mydir,'.aqua/grids/garelli.yaml'))

        # error for already existing file
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['-v','grids', 'add', gridtest, '-e'])
            assert excinfo.value.code == 1

        # add non existing grid file
        run_aqua(['-v','grids','remove', 'garelli.yaml'])
        assert not os.path.exists(os.path.join(mydir,'.aqua/grids/garelli.yaml'))

        # error for already non existing file
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['-v','fixes', 'remove', 'ciccio.yaml'])
            assert excinfo.value.code == 1

        # uninstall everything
        run_aqua_console_with_input(['uninstall'], 'yes')
        assert not os.path.exists(os.path.join(mydir,'.aqua'))

    def test_console_with_links(self, tmpdir, set_home, run_aqua_console_with_input):

        # getting fixture
        mydir = str(tmpdir)
        set_home(mydir)

        # check unexesting installation
        with pytest.raises(SystemExit) as excinfo:
            run_aqua_console_with_input(['-v', 'install', '-p', 'environment.yml'], 'yes')
            assert excinfo.value.code == 1

        # install from path with grids
        #run_aqua_console_with_input(['-v', 'install', '-g', os.path.join(mydir, 'supercazzola')], 'yes')
        #assert os.path.exists(os.path.join(mydir, '.aqua'))

        # uninstall everything
        #run_aqua_console_with_input(['uninstall'], 'yes')
        #assert not os.path.exists(os.path.join(mydir,'.aqua'))

        # install from path
        run_aqua_console_with_input(['-v', 'install', '-p', os.path.join(mydir, 'vicesindaco')], 'yes')
        assert os.path.exists(os.path.join(mydir, 'vicesindaco'))

        # uninstall everything again
        run_aqua_console_with_input(['uninstall'], 'yes')
        assert not os.path.exists(os.path.join(mydir,'.aqua'))


    def test_console_editable(self, tmpdir, run_aqua, set_home, run_aqua_console_with_input):

        # getting fixture
        mydir = str(tmpdir)
        set_home(mydir)

        # check unexesting installation
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['-vv', 'install', '-e', '.'])
            assert excinfo.value.code == 1

        # install from path with grids
        run_aqua(['-vv', 'install', '--editable', 'config'])
        assert os.path.exists(os.path.join(mydir, '.aqua'))
        for folder in ['fixes', 'data_models', 'grids']:
            assert os.path.islink(os.path.join(mydir,'.aqua', folder))
        assert os.path.isdir(os.path.join(mydir, '.aqua', 'catalogs'))

        # install from path in editable mode
        run_aqua_console_with_input(['-vv', 'install', '--editable', 'config', '--path', os.path.join(mydir, 'vicesindaco')], 'yes')
        assert os.path.islink(os.path.join(mydir, '.aqua'))
        run_aqua_console_with_input(['uninstall'], 'yes')

        # install from path in editable mode but withoyt aqua link
        run_aqua_console_with_input(['-vv', 'install', '--editable', 'config', '--path', os.path.join(mydir, 'vicesindaco')], 'no')
        assert not os.path.exists(os.path.join(mydir, '.aqua'))
        assert os.path.isdir(os.path.join(mydir, 'vicesindaco', 'catalogs'))
       
        # uninstall everything again, using AQUA_CONFIG env variable
        os.environ['AQUA_CONFIG'] = os.path.join(mydir, 'vicesindaco')
        run_aqua_console_with_input(['uninstall'], 'yes')
        assert not os.path.exists(os.path.join(mydir,'vicesindaco'))
        del os.environ['AQUA_CONFIG']

        assert not os.path.exists(os.path.join(mydir,'.aqua'))

    # base set of tests for list
    def test_console_list(self, tmpdir, run_aqua, set_home, capfd, run_aqua_console_with_input):

        # getting fixture
        mydir = str(tmpdir)
        set_home(mydir)

        # aqua install
        run_aqua(['install'])
        run_aqua(['add', 'ci'])
        run_aqua(['add', 'ciccio', '-e', 'config/catalogs/ci'])
        run_aqua(['list', '-a'])

        out, _ = capfd.readouterr()
        assert 'AQUA current installed catalogs in' in out
        assert 'ci' in out
        assert 'ciccio (editable' in out
        assert 'IFS.yaml' in out
        assert 'HealPix.yaml' in out
        assert 'ifs2cds.json' in out

        # uninstall everything again
        run_aqua_console_with_input(['uninstall'], 'yes')
        assert not os.path.exists(os.path.join(mydir,'.aqua'))

    def test_console_without_home(self, delete_home, run_aqua, tmpdir, run_aqua_console_with_input):

        # getting fixture
        delete_home()
        mydir = str(tmpdir)
        
        # check unexesting installation
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(['install'])
            assert excinfo.value.code == 1

        # install from path without home
        run_aqua_console_with_input(['-v', 'install', '-p', os.path.join(mydir, 'vicesindaco')], 'yes')
        assert os.path.isdir(os.path.join(mydir, 'vicesindaco'))
        assert os.path.isfile(os.path.join(mydir, 'vicesindaco', 'config-aqua.yaml'))
        assert not os.path.exists(os.path.join(mydir,'.aqua'))


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
