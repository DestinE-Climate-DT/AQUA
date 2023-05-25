# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "AQUA"
copyright = "2023, Climate DT Team"
author = "Climate DT Team"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ['sphinx.ext.autodoc', 'sphinx.ext.napoleon',
              'rst2pdf.pdfbuilder']
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

# -- Options for PDF output --------------------------------------------------
# Grouping the document tree into PDF files. List of tuples
# (source start file, target name, title, author, options).
#
# The options element is a dictionary that lets you override
# this config per-document. For example:
#
# ('index', 'MyProject', 'My Project', 'Author Name', {'pdf_compressed': True})
#
# would mean that specific document would be compressed
# regardless of the global 'pdf_compressed' setting.
pdf_documents = [
('index', 'aqua_doc', 'AQUA documentation', 'AQUA team'),
]
# A comma-separated list of custom stylesheets.
pdf_stylesheets = ['sphinx', 'a4']
# Additional options that are passed to rst2pdf.
pdf_use_index = False
pdf_use_coverpage = False