import matplotlib.pyplot as plt

from aqua.logger import log_configure
from aqua.util import to_list


class PlotGregory:
    def __init__(self, monthly_data_2t=None, monthly_data_net_toa=None,
                 annual_data_2t=None, annual_data_net_toa=None,
                 monthly_ref_2t=None, monthly_ref_net_toa=None,
                 annual_ref_2t=None, annual_ref_net_toa=None,
                 annual_std_2t=None, annual_std_net_toa=None,
                 loglevel: str = 'WARNING'):
        """
        Initialize the class with the data to be plotted

        Args:
            monthly_data_2t: List of monthly 2m temperature data
            monthly_data_net_toa: List of monthly net toa data
            annual_data_2t: List of annual 2m temperature data
            annual_data_net_toa: List of annual net toa data
            monthly_ref_2t: List of monthly reference 2m temperature data
            monthly_ref_net_toa: List of monthly reference net toa data
            annual_ref_2t: List of annual reference 2m temperature data
            annual_ref_net_toa: List of annual reference net toa data
            annual_std_2t: List of annual standard deviation of 2m temperature data
            annual_std_net_toa: List of annual standard deviation of net toa data
            loglevel: Logging level. Default is 'WARNING'
        """

        self.loglevel = loglevel
        self.logger = log_configure(self.loglevel, 'PlotGregory')

        self.monthly_data = {'2t': to_list(monthly_data_2t), 'net_toa': to_list(monthly_data_net_toa)}
        self.annual_data = {'2t': to_list(annual_data_2t), 'net_toa': to_list(annual_data_net_toa)}
        self.ref_monthly_data = {'2t': to_list(monthly_ref_2t), 'net_toa': to_list(monthly_ref_net_toa)}
        self.ref_annual_data = {'2t': to_list(annual_ref_2t), 'net_toa': to_list(annual_ref_net_toa)}
        self.std_annual_data = {'2t': to_list(annual_std_2t), 'net_toa': to_list(annual_std_net_toa)}

        self.data_dict = {'monthly': self.monthly_data, 'annual': self.annual_data}
        self.ref_dict = {'monthly': self.ref_monthly_data, 'annual': self.ref_annual_data}
        self.std_dict = {'monthly': self.std_monthly_data, 'annual': self.std_annual_data}

    def plot(self, freq=['monthly', 'annual']):
        """
        Plot the data

        Args:
            freq: List of frequency for plotting. Default is ['monthly', 'annual']
        """

        ax_monthly = None
        ax_annual = None

        if 'monthly' in freq and 'annual' in freq:
            fig, (ax_monthly, ax_annual) = plt.subplots(1, 2, figsize=(12, 6))
        elif 'monthly' in freq:
            fig, ax_monthly = plt.subplots(1, 1, figsize=(6, 6))
        elif 'annual' in freq:
            fig, ax_annual = plt.subplots(1, 1, figsize=(6, 6))
        else:
            raise ValueError('Invalid frequency for plotting, allowed values are "monthly" and "annual"')

        if ax_monthly:
            self.plot_monthly(ax_monthly)
        if ax_annual:
            self.plot_annual(ax_annual)

        # TODO: save the plot

    def plot_monthly(self, ax):
        pass

    def plot_annual(self, ax):
        pass
