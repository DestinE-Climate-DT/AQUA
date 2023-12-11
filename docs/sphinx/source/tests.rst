Tests
=====

Testing is an essential part of developing and maintaining a robust Python package.
Our package uses ``pytest``, a widely-used testing framework in the Python ecosystem, 
for writing and running tests. 

Continuous Integration/Continuous Deployment (CI/CD) is currently handled by GitHub Actions, 
which runs the test suite on various Python versions whenever changes are pushed to the repository.
In the future (CI/CD) will also be run on HPC systems. 

Running Tests Locally
---------------------

Before running tests locally, ensure you have installed all the necessary dependencies, including ``pytest``.

To run the test suite, navigate to the root directory of the project and run the following:

.. code-block:: bash

    ./download_data_for_tests.sh

This will download the data needed for the tests and change the machine name in the ``config/config-aqua.yaml`` to ``ci``. 
Remember to change it to your machine name after the tests are finished.

Then, run the following command:

.. code-block:: bash

    python -m pytest ./tests/test_basic.py

This will run the basic test suite and print the results to the terminal. Have a look at the ``tests`` directory for more tests.


Writing Tests
-------------

Tests for our package are written using the pytest framework. Here's a basic template for a test function:

.. code-block:: python

    def test_function_name():
        # setup code
        result = function_to_test(argument1, argument2)
        expected_result = ...  # what you expect the result to be
        assert result == expected_result, "optional error message"


Remember to follow these guidelines when writing tests:

- Each test function should focus on one small aspect of a function's behavior.
- Test functions should be named descriptively, so it's clear what they're testing.
- Use assertions to check that the function's actual output matches the expected output.

Continuous Integration (CI)
---------------------------

We use GitHub Actions for Continuous Integration. 
GitHub Actions automatically runs our test suite on Python 3.11 whenever a change is pushed to the repository.

The configuration for GitHub Actions is located in the ``.github/workflows`` directory. 

Test Coverage
-------------

We are currently implementing test coverage using ``pytest-cov``.


