import os
import matplotlib.pyplot as plt
import pytest
from aqua.graphics import ConfigStyle

@pytest.mark.configstyle
def test_ConfigStyle():
    """
    Test that ConfigStyle class is correctly initialized
    """
    style = 'aqua.mplstyle'

    cs = ConfigStyle(style=style)
    assert cs.style == style

@pytest.mark.configstyle_default
def test_ConfigStyle_default():
    """
    Test that ConfigStyle class is correctly initialized with default style "
    """
    cs = ConfigStyle()
    assert cs.style is not None