#!/usr/bin/env python

from setuptools import setup
import itertools

try:
    # for pip >= 10
    from pip._internal.req import parse_requirements
except ImportError:
    # for pip <= 9.0.3
    from pip.req import parse_requirements

def load_requirements(fname):
    install_reqs = parse_requirements(fname, session="test")
    
    try:
        requirements = [str(ir.req) for ir in install_reqs]
    except:
        requirements = [str(ir.requirement) for ir in install_reqs]

    return requirements

dependencies = {
  'extra_requirements'  : ['cfgrib', 'jinja2', 'pandas<2', 'pyYAML', 'urllib3<1.27'],
  'remote_requirements' : ['smmregrid @ git+https://github.com/jhardenberg/smmregrid']
}

setup(name='aqua',
      version='0.0.5',
      description='AQUA; a model evaluation framework for high-resolution climate model simulations',
      author='The AQUA team',
      author_email='p.davini@isac.cnr.it',
      url='https://github.com/oloapinivad/AQUA',
      python_requires='>=3.9, <3.11',
      packages=['aqua'],
      install_requires=[
         #dependencies['extra_requirements'],
         load_requirements("requirements/conda_requirements.txt"),
         load_requirements("requirements/pip_requirements.txt"),
         dependencies['extra_requirements'],
         #dependencies['remote_requirements'],
         ]
      )
