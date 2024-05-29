import os
import shutil
import sys
import pytest
from aqua.main import AquaConsole, query_yes_no
from aqua.util import dump_yaml, load_yaml

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
        AquaConsole()
        sys.stdin.close()
        os.remove(testfile)
    return _run_aqua_console


@pytest.mark.aqua
class TestAquaConsole():
    """Class for AQUA console tests"""

    # base set of tests
    def test_console_base(self, tmpdir, set_home, run_aqua_console_with_input):

        # getting fixture
        mydir = str(tmpdir)
        set_home(mydir)

        # aqua install
        set_args(['install'])
        AquaConsole()
        assert os.path.isdir(os.path.join(mydir,'.aqua'))
        assert os.path.isfile(os.path.join(mydir,'.aqua', 'config-aqua.yaml'))

        # do it twice!
        run_aqua_console_with_input(['-vv', 'install'], 'yes')
        assert os.path.exists(os.path.join(mydir,'.aqua'))
        for folder in ['fixes', 'data_models', 'grids']:
            assert os.path.isdir(os.path.join(mydir,'.aqua', folder))

        # add catalog
        for catalog in ['ci', 'levante']:
            set_args(['add', catalog])
            AquaConsole()
            assert os.path.isdir(os.path.join(mydir,'.aqua/machines', catalog))
            config_file = load_yaml(os.path.join(mydir,'.aqua', 'config-aqua.yaml'))
            assert config_file['machine'] == catalog

        # set catalog
        set_args(['set', 'ci'])
        AquaConsole()
        assert os.path.isdir(os.path.join(mydir,'.aqua/machines/ci'))
        config_file = load_yaml(os.path.join(mydir,'.aqua', 'config-aqua.yaml'))
        assert config_file['machine'] == 'ci'

        # add catalog again and error
        set_args(['-v', 'set', 'ciccio'])
        # check unexesting installation
        with pytest.raises(SystemExit) as excinfo:
            AquaConsole()
            assert excinfo.value.code == 1

        # add catalog again and error
        set_args(['-v', 'add', 'ci'])
        # check unexesting installation
        with pytest.raises(SystemExit) as excinfo:
            AquaConsole()
            assert excinfo.value.code == 1

        # remove non-existing catalog
        os.makedirs(os.path.join(mydir,'.aqua/machines/ci'), exist_ok=True)
        set_args(['remove', 'pippo'])
        with pytest.raises(SystemExit) as excinfo:
            AquaConsole()
            assert excinfo.value.code == 1

        # remove existing catalog
        set_args(['remove', 'ci'])
        AquaConsole()
        assert not os.path.exists(os.path.join(mydir,'.aqua/machines/ci'))
        assert os.path.exists(os.path.join(mydir,'.aqua'))

        # uninstall and say no
        with pytest.raises(SystemExit) as excinfo:
            run_aqua_console_with_input(['uninstall'], 'no')
            assert excinfo.value.code == 0
            assert os.path.exists(os.path.join(mydir,'.aqua'))

        # uninstall and say no
        run_aqua_console_with_input(['uninstall'], 'yes')
        assert not os.path.exists(os.path.join(mydir,'.aqua'))
        
    def test_console_advanced(self, tmpdir, set_home, run_aqua_console_with_input):

        # getting fixture
        mydir = str(tmpdir)
        set_home(mydir)
        
        # check unexesting installation
        with pytest.raises(SystemExit) as excinfo:
            run_aqua_console_with_input(['uninstall'], 'yes')
            assert excinfo.value.code == 1

        # a new install
        set_args(['install'])
        AquaConsole()
        assert os.path.exists(os.path.join(mydir,'.aqua'))

        # add catalog with editable option
        set_args(['-v', 'add', 'ci', '-e', 'config/machines/ci'])
        AquaConsole()
        assert os.path.isdir(os.path.join(mydir,'.aqua/machines/ci'))

        # add catalog again and error
        set_args(['-v', 'add', 'ci', '-e', 'config/machines/ci'])
        # check unexesting installation
        with pytest.raises(SystemExit) as excinfo:
            AquaConsole()
            assert excinfo.value.code == 1
        assert os.path.exists(os.path.join(mydir,'.aqua/machines/ci'))

        # add catalog again and error
        set_args(['-v', 'add', 'ci', '-e', 'config/machines/baciugo'])
        # check unexesting installation
        with pytest.raises(SystemExit) as excinfo:
            AquaConsole()
            assert excinfo.value.code == 1
        assert not os.path.exists(os.path.join(mydir,'.aqua/machines/baciugo'))

        # remove existing catalog from link
        set_args(['remove', 'ci'])
        AquaConsole()
        assert not os.path.exists(os.path.join(mydir,'.aqua/machines/ci'))

        # add wrong fix file
        fixtest = os.path.join(mydir, 'antani.yaml')
        dump_yaml(fixtest, {'fixer_name':  'antani'})
        set_args(['fixes-add', fixtest])
        AquaConsole()
        assert not os.path.exists(os.path.join(mydir,'.aqua/fixes/antani.yaml'))

        # add mock grid file
        gridtest = os.path.join(mydir, 'supercazzola.yaml')
        dump_yaml(gridtest, {'grids': {'sindaco': {'path': '{{ grids }}/comesefosseantani.nc'}}})
        set_args(['-v','grids-add', gridtest])
        AquaConsole()
        assert os.path.isfile(os.path.join(mydir,'.aqua/grids/supercazzola.yaml'))

        # add mock grid file but editable
        gridtest = os.path.join(mydir, 'garelli.yaml')
        dump_yaml(gridtest, {'grids': {'sindaco': {'path': '{{ grids }}/comesefosseantani.nc'}}})
        set_args(['-v','grids-add', gridtest, '-e'])
        AquaConsole()
        assert os.path.islink(os.path.join(mydir,'.aqua/grids/garelli.yaml'))

        # error for already existing file
        with pytest.raises(SystemExit) as excinfo:
            set_args(['-v','grids-add', gridtest, '-e'])
            AquaConsole()
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
        run_aqua_console_with_input(['-v', 'install', '-g', os.path.join(mydir, 'supercazzola')], 'yes')
        assert os.path.exists(os.path.join(mydir, '.aqua'))

        # uninstall everything
        run_aqua_console_with_input(['uninstall'], 'yes')
        assert not os.path.exists(os.path.join(mydir,'.aqua'))

        # install from path
        run_aqua_console_with_input(['-v', 'install', '-p', os.path.join(mydir, 'vicesindaco')], 'yes')
        assert os.path.exists(os.path.join(mydir, 'vicesindaco'))

        # uninstall everything again
        run_aqua_console_with_input(['uninstall'], 'yes')
        assert not os.path.exists(os.path.join(mydir,'.aqua'))


    def test_console_editable(self, tmpdir, set_home, run_aqua_console_with_input):

        # getting fixture
        mydir = str(tmpdir)
        set_home(mydir)

        # check unexesting installation
        with pytest.raises(SystemExit) as excinfo:
            set_args(['-vv', 'install', '-e', '.'])
            AquaConsole()
            assert excinfo.value.code == 1

        # install from path with grids
        set_args(['-vv', 'install', '--editable', 'config'])
        AquaConsole()
        assert os.path.exists(os.path.join(mydir, '.aqua'))
        for folder in ['fixes', 'data_models', 'grids']:
            assert os.path.islink(os.path.join(mydir,'.aqua', folder))
        assert os.path.isdir(os.path.join(mydir, '.aqua', 'machines'))

        # install from path in editable mode
        run_aqua_console_with_input(['-vv', 'install', '--editable', 'config', '--path', os.path.join(mydir, 'vicesindaco')], 'yes')
        assert os.path.islink(os.path.join(mydir, '.aqua'))
        run_aqua_console_with_input(['uninstall'], 'yes')

        # install from path in editable mode but withoyt aqua link
        run_aqua_console_with_input(['-vv', 'install', '--editable', 'config', '--path', os.path.join(mydir, 'vicesindaco')], 'no')
        assert not os.path.exists(os.path.join(mydir, '.aqua'))
        assert os.path.isdir(os.path.join(mydir, 'vicesindaco', 'machines'))
       
        # uninstall everything again, using AQUA_CONFIG env variable
        os.environ['AQUA_CONFIG'] = os.path.join(mydir, 'vicesindaco')
        run_aqua_console_with_input(['uninstall'], 'yes')
        assert not os.path.exists(os.path.join(mydir,'vicesindaco'))
        del os.environ['AQUA_CONFIG']

        assert not os.path.exists(os.path.join(mydir,'.aqua'))

    # base set of tests for list
    def test_console_list1(self, tmpdir, set_home, capfd, run_aqua_console_with_input):

        # getting fixture
        mydir = str(tmpdir)
        set_home(mydir)

        # aqua install
        set_args(['install'])
        AquaConsole()
        set_args(['add', 'ci'])
        AquaConsole()
        set_args(['add', 'ciccio', '-e', 'config/machines/ci'])
        AquaConsole()
        set_args(['list'])
        AquaConsole()

        out, _ = capfd.readouterr()
        assert 'AQUA current installed catalogs in' in out
        assert 'ci' in out
        assert 'ciccio (editable' in out

        # uninstall everything again
        run_aqua_console_with_input(['uninstall'], 'yes')
        assert not os.path.exists(os.path.join(mydir,'.aqua'))

    def test_console_without_home(self, delete_home, tmpdir, run_aqua_console_with_input):

        # getting fixture
        delete_home()
        mydir = str(tmpdir)
        
        # check unexesting installation
        with pytest.raises(SystemExit) as excinfo:
            set_args(['install'])
            AquaConsole()
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
