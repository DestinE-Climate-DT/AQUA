I am getting TypeError: 'GeometryCollection' object is not subscriptable when using plot_single_map function
============================================================================================================

Cartopy and Matplotlib can generate some issue, expecially when plotting masked data.
If you are using the ``plot_single_map`` function and you are getting this error,
you can try to set the ``transform_first`` kwarg to ``True`` in the ``plot_single_map`` function.
This will transform the data to the target projection before plotting the contour, and it may solve the issue.
Alternatively, you can try to set the ``contour`` arg to ``False`` and enable the pcolor plot,
which may solve the issue as well.