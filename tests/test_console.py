import pytest
import os
import shutil
import sys
from aqua.aqua_main import AquaConsole, query_yes_no
from aqua.util import dump_yaml

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

        # aqua init
        set_args(['init'])
        AquaConsole()
        assert os.path.exists(os.path.join(mydir,'.aqua'))

        # do it twice!
        run_aqua_console_with_input(['-vv', 'init'], 'yes')
        assert os.path.exists(os.path.join(mydir,'.aqua'))

        # add catalog
        set_args(['add', 'ci'])
        AquaConsole()
        assert os.path.exists(os.path.join(mydir,'.aqua/machines/ci'))

        # add catalog again and error
        set_args(['-v', 'add', 'ci'])
        AquaConsole()
        assert os.path.exists(os.path.join(mydir,'.aqua/machines/ci'))

        # run the list
        set_args(['list'])
        AquaConsole()

        # remove non-existing catalog
        os.makedirs(os.path.join(mydir,'.aqua/machines/ci'), exist_ok=True)
        set_args(['remove', 'pippo'])
        AquaConsole()

        # remove existing catalog
        set_args(['remove', 'ci'])
        AquaConsole()
        assert not os.path.exists(os.path.join(mydir,'.aqua/machines/ci'))

        # uninstall everything
        run_aqua_console_with_input(['uninstall'], 'yes')
        assert not os.path.exists(os.path.join(mydir,'.aqua'))

    def test_console_advanced(self, tmpdir, set_home, run_aqua_console_with_input):

        # getting fixture
        mydir = str(tmpdir)
        set_home(mydir)

        # check unexesting installation
        with pytest.raises(SystemExit) as excinfo:
            run_aqua_console_with_input(['uninstall'], 'yes')
            assert excinfo.value.code == 0

        # a new init
        set_args(['init'])
        AquaConsole()
        assert os.path.exists(os.path.join(mydir,'.aqua'))

        # add catalog with editable option - to be improved
        set_args(['-v', 'add', 'ci', '-e', 'config/machines/ci'])
        AquaConsole()
        assert os.path.exists(os.path.join(mydir,'.aqua/machines'))

        # add wrong fix file
        fixtest = os.path.join(mydir, 'antani.yaml')
        dump_yaml(fixtest, {'fixer_name':  'antani'})
        set_args(['fixes-add', fixtest])
        AquaConsole()
        assert not os.path.exists(os.path.join(mydir,'config/dir/fixes/antani.yaml'))

        # add mock grid file
        gridtest = os.path.join(mydir, 'supercazzola.yaml')
        dump_yaml(gridtest, {'grids': {'sindaco': {'path': '{{ grids }}/comesefosseantani.nc'}}})
        set_args(['-v','grids-add', gridtest])
        AquaConsole()
        assert os.path.exists(gridtest)

        # uninstall everything
        run_aqua_console_with_input(['uninstall'], 'yes')
        assert not os.path.exists(os.path.join(mydir,'.aqua'))

    def test_console_with_links(self, tmpdir, set_home, run_aqua_console_with_input):

        # getting fixture
        mydir = str(tmpdir)
        set_home(mydir)

        # install from path with grids
        run_aqua_console_with_input(['-v', 'init', '-g', os.path.join(mydir, 'supercazzola')], 'yes')
        assert os.path.exists(os.path.join(mydir, '.aqua'))

        # uninstall everything
        run_aqua_console_with_input(['uninstall'], 'yes')
        assert not os.path.exists(os.path.join(mydir,'.aqua'))

        # install from path
        run_aqua_console_with_input(['-v', 'init', '-p', os.path.join(mydir, 'vicesindaco')], 'yes')
        assert os.path.exists(os.path.join(mydir, 'vicesindaco'))

        # uninstall everything again
        run_aqua_console_with_input(['uninstall'], 'yes')
        assert not os.path.exists(os.path.join(mydir,'.aqua'))

    def test_console_without_home(self, delete_home, tmpdir, run_aqua_console_with_input):

        # getting fixture
        delete_home()
        mydir = str(tmpdir)
        
        # raise init without home
        with pytest.raises(ValueError):
            set_args(['init'])
            AquaConsole()

        # install from path without home
        run_aqua_console_with_input(['-v', 'init', '-p', os.path.join(mydir, 'vicesindaco')], 'yes')
        assert os.path.exists(os.path.join(mydir, 'vicesindaco'))

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
