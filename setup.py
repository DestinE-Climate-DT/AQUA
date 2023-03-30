#!/usr/bin/env python

from setuptools import setup

setup(name='aqua',
      version='0.0.2',
      description='AQUA; a model evaluation framework for high-resolution climate model simulations',
      author='The AQUA team',
      author_email='p.davini@isac.cnr.it',
      url='https://github.com/oloapinivad/AQUA',
      python_requires='>=3.9, <3.11',
      packages=['aqua'],
      install_requires=[
        'cfgrib',
        'dask',
        'docker',
        'gribscan',
        'ecCodes',
        'intake',
        'intake-esm<=2021.8.17',
        'intake-xarray',
        'jinja2',
        'metpy',
        'numpy<1.24',
        'requests>=2.26.0',
        'pydantic',
        'pandas<2',
        'pyYAML',
        'sparse', 
        'xarray',
	'urllib3>=1.26.0,<1.27',
        #'smmregrid @ git+https://github.com/jhardenberg/smmregrid'
      ]
    )
