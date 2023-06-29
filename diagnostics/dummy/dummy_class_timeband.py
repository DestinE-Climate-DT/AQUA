""" The module contains a simple example of the class for dummy diagnostic.

.. moduleauthor:: AQUA team

"""

class DummyDiagnostic:
    """This class is a minimal version of the Dummy Diagnostic.
    """
    def __init__(self,
            s_year      = None,
            f_year      = None,
            s_month     = None,
            f_month     = None):


        """ The constructor of the class.

        Args:
            s_year (int, optional):                 The start year of the time interval. Defaults to None.
            f_year (int, optional):                 The end year of the time interval. Defaults to None.
            s_month (int, optional):                The start month of the time interval. Defaults to None.
            f_month (int, optional):                The end month of the time interval. Defaults to None."""


        self.s_year     = s_year
        self.f_year     = f_year   
        self.s_month    = s_month
        self.f_month    = f_month    

    def class_attributes_update(self, s_year=None, f_year=None, s_month=None, f_month=None):
        """ Function to update the class attributes.

        Args:
            s_year (int, optional):         The starting year of the Dataset.  Defaults to None.
            f_year (int, optional):         The ending year of the Dataset.  Defaults to None.
            s_month (int, optional):        The starting month of the Dataset.  Defaults to None.
            f_month (int, optional):        The ending month of the Dataset.
        Returns:
            NoneType: None
        """

        if s_year is not None and isinstance(s_year, int):
            self.s_year = s_year
        elif s_year is not None and not isinstance(s_year, int):
            raise Exception("s_year must to be integer")

        if f_year is not None and isinstance(f_year, int):
            self.f_year = f_year
        elif f_year is not None and not isinstance(f_year, int):
            raise Exception("f_year must to be integer")

        if s_month is not None and isinstance(s_month, int):
            self.s_month = s_month
        elif s_month is not None and not isinstance(s_month, int):
            raise Exception("s_month must to be integer")

        if f_month is not None and isinstance(f_month, int):
            self.f_month = f_month
        elif f_month is not None and not isinstance(f_month, int):
            raise Exception("f_month must to be integer")


    def time_band(self, data, s_year=None, f_year=None, s_month=None, f_month=None):
        """ Function to select the Dataset for specified time range

        Args:
            data (xarray):                  The Dataset
            s_year (int, optional):         The starting year of the Dataset.  Defaults to None.
            f_year (int, optional):         The ending year of the Dataset.  Defaults to None.
            s_month (int, optional):        The starting month of the Dataset.  Defaults to None.
            f_month (int, optional):        The ending month of the Dataset.  Defaults to None.

        Returns:
            xarray: The Dataset only for selected time range.
        """

        self.class_attributes_update(s_year=s_year, f_year=f_year, s_month=s_month, f_month=f_month)

        if self.s_year != None and self.f_year == None:
            data= data.where(data['time.year'] == self.s_year, drop=True)
        elif self.s_year != None and self.f_year != None:
            data = data.where(data['time.year'] >= self.s_year, drop=True)
            data = data.where(data['time.year'] <= self.f_year, drop=True)

        if self.s_month != None and self.f_month != None:
            data = data.where(data['time.month'] >= self.s_month, drop=True)
            data = data.where(data['time.month'] <= self.f_month, drop=True)
        elif self.s_year == None and self.f_year == None:
            print('The function returns the original dataset because all arguments are None')
        return data
