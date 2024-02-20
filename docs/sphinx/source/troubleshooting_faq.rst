Troubleshooting and FAQ
=======================

This section answers frequently asked questions and addresses common issues that users may encounter when working with AQUA.

**Q: I am getting an error when trying to install AQUA on LUMI, what can I do?**

It is possible that, if you're recreating the environment, the code breaks while removing the folder ``~/mambaforge/aqua/bin``, complaining the resource is busy.
In this case you may have some processes running in the background. 
You can check them with ``ps -ef | grep aqua`` and kill them manually if needed.

**Q: I am getting TypeError: 'GeometryCollection' object is not subscriptable when using the `plot_single_map` function. What can I do?**

Cartopy and Matplotlib can generate some issue, expecially when plotting masked data.
If you are using the `plot_single_map` function and you are getting this error,
you can try to set the `transform_first` kwarg to `True` in the `plot_single_map` function.
This will transform the data to the target projection before plotting the contour, and it may solve the issue.
Alternatively, you can try to set the `contour` arg to `False` and enable the pcolor plot,
which may solve the issue as well.

**Q: How do I cite AQUA in my research or publications?**

A: When using AQUA for research purposes, please cite the package 
and its authors using the information provided in this documentation's :doc:`references_acknowledgments` section.
