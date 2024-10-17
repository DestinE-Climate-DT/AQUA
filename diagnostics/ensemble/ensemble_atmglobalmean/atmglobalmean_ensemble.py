import os
import gc

import xarray as xr
from aqua import Reader
from aqua.logger import log_configure
from aqua.exceptions import NoObservationError, NoDataError
from aqua.util import create_folder
from aqua.util import add_pdf_metadata,time_to_string
from matplotlib.pyplot as plt
xr.set_options(keep_attrs=True)

class AtmglobalmeanEnsemble():
    def __init__(self):
        """
        """
        pass
    
    def retrieve_data(self):
        pass

    def plot(self):
        pass

    def save_pdf(self):
        pass

    def run(self):
        pass
