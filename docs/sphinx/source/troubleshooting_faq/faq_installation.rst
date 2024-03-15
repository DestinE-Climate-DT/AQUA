.. _faq_installation:

I am getting an error when trying to install AQUA on LUMI
=========================================================

- It is possible that, if you're recreating the environment, the code breaks while removing the folder ``~/mambaforge/aqua/bin``, complaining the resource is busy.
  In this case you may have some processes running in the background. 
  You can check them with ``ps -ef | grep aqua | grep $USER``. and kill them manually if needed.
- You may encounter an error messange indicating a problem with the pip's dependency resolution process,
  which may involve outdated dependencies for certain packages, e.g.:
  ``ERROR: pip's dependency resolver does not currently take into account all the packages that are installed. This behaviour is the source of the following dependency conflicts. one-pass 0.4.2 requires tqdm, which is not installed.``.
  These dependency resolution issues can often stem from outdated or conflicting information stored within the ``.local`` folder.
  A potential solution is to remove the ``.local`` folder located in your home directory.
  By doing so, you effectively eliminate any cached data or outdated information associated with Python packages, including dependency metadata and pip will begin its operations with a clean slate.

