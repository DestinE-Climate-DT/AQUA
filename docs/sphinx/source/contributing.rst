Contributing to AQUA
====================

We welcome contributions to the AQUA project! 
Whether you're reporting bugs, suggesting new features, or contributing code, your involvement helps make AQUA better for everyone.
This guide outlines the process for contributing to AQUA.

Reporting Bugs
--------------

If you encounter any bugs or issues while using AQUA, please report them on the project's issue tracker on GitHub:

1. Navigate to the AQUA GitHub repository.
2. Click on the "Issues" tab.
3. Click the "New issue" button.
4. Choose to provide a detailed description of the issue, including steps to reproduce, expected behavior, and any relevant error messages.

Suggesting Features
-------------------

We'd love to hear from you if you have an idea for a new feature or enhancement in AQUA! To suggest a feature, follow these steps:

1. Navigate to the AQUA GitHub repository.
2. Click on the "Issues" tab.
3. Click the "New issue" button.
4. Provide a detailed description of the proposed feature, including use cases, benefits, and potential challenges.

Contributing Code
-----------------

We welcome contributions to the AQUA codebase, both to the framework and to the diagnostics.

To contribute code to AQUA, follow these general steps:

1. If you don't already have it, ask the development team to grant you access to the AQUA repository.
2. Create an issue in the github repository to discuss your proposed changes.
3. Clone the repository to your local machine.
4. Create a new branch for your feature or bugfix (e.g., ``git checkout -b my-feature``).
5. Commit your changes
6. When ready, push your branch to the github repository.
7. Create a pull request in the AQUA repository, describing your changes and referencing any related issues.
8. In the pull request on GitHub, you can run tests by adding the ``run tests`` label to the pull request.
   This will trigger the CI/CD pipeline to run the tests. Please do this only if needed, as the github action hours are limited.
9. Add a line to the ``CHANGELOG.md`` file in the `Unreleased` section, describing your changes.
10. Once your pull request is approved, it will be merged into the main branch by the development team. 

.. warning::
   Please do not merge pull requests into the main branch yourself and never, ever, commit any
   changes directly to the main branch!

A more detailed guide on these steps can be found in the `CONTRIBUTING.md <https://github.com/DestinE-Climate-DT/AQUA/blob/main/CONTRIBUTING.md>`_ file in the AQUA repository root folder.

Please note that all contributed codes must be licensed under the same terms as AQUA.

Documentation and Tutorials
---------------------------

We also welcome contributions to the AQUA documentation and tutorials.
If you have suggestions for improvements, want to help maintain the documentation, or have ideas for new tutorials,
please create an issue on the GitHub repository and/or submit a pull request with your proposed changes.

.. _tests:
Testing
-------

Testing is an essential part of developing and maintaining a robust Python package.
Our package uses ``pytest``, a widely-used testing framework in the Python ecosystem, 
for writing and running tests. 

Continuous Integration/Continuous Deployment (CI/CD) is currently handled by GitHub Actions, 
which runs the test suite on various Python versions whenever changes are pushed to the repository.
In the future (CI/CD) will also be run on HPC systems. 

Running Tests Locally
^^^^^^^^^^^^^^^^^^^^^

Before running tests locally, ensure you have installed all the necessary dependencies, including ``pytest``.
To run the test suite, navigate to the root directory of the project and run the following:

.. code-block:: bash

    ./download_data_for_tests.sh

This will download the data needed for the tests and change the machine name in the ``config/config-aqua.yaml`` to ``ci``. 
Remember to change it to your machine name after the tests are finished.

Then, run the following command:

.. code-block:: bash

    pytest -m aqua

This will run the basic test suite and print the results to the terminal.
Have a look at the ``tests`` directory for more tests.

Writing Tests
^^^^^^^^^^^^^

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
