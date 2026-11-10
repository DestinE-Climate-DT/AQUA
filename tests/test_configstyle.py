import matplotlib
import matplotlib.pyplot as plt
import pytest

from aqua.core.graphics import ConfigStyle


@pytest.fixture(autouse=True)
def _restore_rcparams():
    """ConfigStyle activates a matplotlib style: do not leak it to the following tests."""
    with matplotlib.rc_context():
        matplotlib.rcdefaults()
        yield


@pytest.mark.graphics
class TestConfigStyle:
    def test_configstyle_matplotlib(self):
        """A matplotlib style name is applied: fails if the style is only stored."""
        cs = ConfigStyle(style="ggplot")
        assert cs.style == "ggplot"
        assert plt.rcParams["axes.facecolor"] == "#E5E5E5"

    def test_configstyle_default(self):
        """Without a style the AQUA style file of the configuration is applied."""
        cs = ConfigStyle()
        assert cs.style == "aqua"
        assert cs.style_file.endswith("styles/aqua.mplstyle")
        assert plt.rcParams["axes.titlecolor"] == "darkslategrey"
        assert plt.rcParams["axes.titlepad"] == 12

    def test_wrong_style(self):
        """A style that is neither an AQUA file nor a matplotlib style is an error"""
        with pytest.raises(OSError, match="wrong_style"):
            ConfigStyle(style="wrong_style")
