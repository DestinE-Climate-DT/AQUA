# create package
python3 -m pip install --upgrade build
python3 -m pip install --upgrade twine
python3 -m build

# upload to pypy test
python3 -m twine upload --repository testpypi dist/* --verbose  

# clean
pip uninstall aqua-core 

# install to pypy test
python -m pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple aqua-core