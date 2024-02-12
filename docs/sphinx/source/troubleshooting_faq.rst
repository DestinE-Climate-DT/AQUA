Troubleshooting and FAQ
=======================

This section answers frequently asked questions and addresses common issues that users may encounter when working with AQUA.

**Q: I am getting an error when trying to install AQUA on LUMI, what can I do?**

It is possible that, if you're recreating the environment, the code breaks while removing the folder ``~/mambaforge/aqua/bin``, complaining the resource is busy.
In this case you may have some processes running in the background. 
You can check them with ``ps -ef | grep aqua`` and kill them manually if needed.

**Q: How do I cite AQUA in my research or publications?**

A: When using AQUA for research purposes, please cite the package 
and its authors using the information provided in this documentation's :doc:`references_acknowledgments` section.
