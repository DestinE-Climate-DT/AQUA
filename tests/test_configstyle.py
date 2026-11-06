import matplotlib
import pytest

from aqua.core.graphics import ConfigStyle


@pytest.fixture(autouse=True)
def _restore_rcparams():
    """ConfigStyle activates a matplotlib style: do not leak it to the following tests."""
    with matplotlib.rc_context():
        yield


@pytest.mark.graphics
class TestConfigStyle:
    def test_configstyle_maptplotlib(self):
        """
        Test that ConfigStyle class is correctly initialized
        with a matplotlib style
        """
        style = "ggplot"

        cs = ConfigStyle(style=style)
        assert cs.style == style

    def test_configstyle_default(self):
        """
        Test that ConfigStyle class is correctly initialized with default style
        """
        cs = ConfigStyle()
        assert cs.style == "aqua"

    def test_wrong_style(self):
        """
        Test that ConfigStyle class is correctly initialized with wrong style
        """
        with pytest.raises(OSError):
            ConfigStyle(style="wrong_style")
