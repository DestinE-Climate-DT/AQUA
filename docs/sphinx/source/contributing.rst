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

AQUA is tested with ``pytest``. Continuous Integration is handled by GitHub Actions through several test suites
(PR tests, nightly, release, PyPI and Conda), each checking a different way of installing AQUA.
On Pull Requests, tests are triggered by labels (e.g. ``run tests``, ``nightly``, ``release``).

To run the core tests locally, download the test data into ``AQUA_tests/`` in the repository root
from the HTTP mirror, configure AQUA with the ``ci`` catalog and run ``pytest``:

.. code-block:: bash

    mkdir -p AQUA_tests && cd AQUA_tests
    wget -nv -r -np -nH -R "index.html*" --cut-dirs=3 http://wilma.to.isac.cnr.it/paolo/aqua-tests/data-tests/
    cd ..
    pytest -n 4 --dist loadgroup -m aqua

.. note::
   The ``download_data_for_tests.sh`` script is deprecated. Test data is versioned with DVC,
   but pulling it requires credentials for the DVC remote.

The full testing strategy (suites, PR labels, reusable workflow and composite actions, pytest markers,
parallelism, artifacts, failure reports, local setup and guidelines to change the CI) is documented in the
`CONTRIBUTING.md <https://github.com/DestinE-Climate-DT/AQUA/blob/main/CONTRIBUTING.md#testing>`_ file.
