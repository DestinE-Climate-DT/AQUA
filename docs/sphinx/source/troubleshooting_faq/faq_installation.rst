.. _faq_installation:

I am getting an error when trying to install AQUA on LUMI
=========================================================

Resource is busy
----------------

It is possible that, if you're recreating the environment, the code breaks while removing the folder ``~/mambaforge/aqua/bin``, complaining the resource is busy.
In this case you may have some processes running in the background. 
You can check them with ``ps -ef | grep aqua | grep $USER``. and kill them manually if needed.

Pip dependency resolution
-------------------------

You may encounter an error messange indicating a problem with the pip's dependency resolution process,
which may involve outdated dependencies for certain packages, e.g.:
  
.. code-block:: text
  
  ERROR: pip's dependency resolver does not currently take into account all the packages that are installed.
  This behaviour is the source of the following dependency conflicts.
  one-pass 0.4.2 requires tqdm, which is not installed..

These dependency resolution issues can often stem from outdated or conflicting information stored within the ``.local`` folder.
A potential solution is to remove the ``.local`` folder located in your home directory.
By doing so, you effectively eliminate any cached data or outdated information associated with Python packages, including dependency metadata and pip will begin its operations with a clean slate.

Permission denied
-----------------

If you are recreating an installation it is possible to encounter an error similar to this:

.. code-block:: bash

  PermissionError: [Errno 13] Permission denied: '/users/lrb_465000454_fdb/mars/versions/current/bin/../bin'
  [ ERROR ] Configuration construction failed 
  [ ERROR ] Set CW_DEBUG_KEEP_FILES env variable to keep build files

This may be due to a previous path added to the ``PATH`` variable in a previous installation and not available anymore.
In this case please remove manually the ``load_aqua.sh`` file from the previous installation and try again.

