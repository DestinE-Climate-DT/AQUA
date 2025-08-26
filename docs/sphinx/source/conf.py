# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html
import sys
import os
from aqua import __version__ as project_version

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "AQUA"
copyright = "2024, Climate DT Team"
author = "Climate DT Team"
version = str(project_version)

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ['sphinx.ext.autodoc', 'sphinx.ext.napoleon']
napoleon_google_docstring = True
napoleon_numpy_docstring = False

templates_path = ["_templates"]
autoclass_content = 'both'
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
# html_static_path = ["_static"]
html_theme_options = {
    "collapse_navigation": False,
    "sticky_navigation": True,
    "navigation_depth": 4,
}

# Add the path to the package to the sys.path
sys.path.insert(0, os.path.relpath('../../diagnostics'))
sys.path.insert(0, os.path.relpath('../../diagnostics/teleconnections'))
sys.path.insert(0, os.path.relpath('../../diagnostics/ocean3d'))
sys.path.insert(0, os.path.relpath('../../diagnostics/tropical_cyclones'))
