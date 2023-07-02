"""
Functions module.

This module contains functions that are used in the dummy module.
The dummy module is used to give an example of how to write a diagnostic.

Functions here contained are then used in the dummy class.

You should ideally move here blocks of code that you used in the notebooks.
Formalize them as functions.

Most of the import should stay here or in the class definition file.
Your notebook should only import the class and call the run method.
Eventually plot functions can be imported in the notebook.
"""


def dummy_func(data=None):
    """
    Dummy function.

    This function is used to give an example of how to write a diagnostic.
    Here you should write an explanation of what the function does.
    In this case it just multiplies the data by 2.

    Args:
        data (xr.Dataset): data to be used in the diagnostic

    Returns:
        xr.Dataset: data analysed in the diagnostic

    Raises:
        ValueError: if data is None
    """
    if data is None:
        raise ValueError("Data is None")

    # Do something with data
    data = data*2.0

    return data
