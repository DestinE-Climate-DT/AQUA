import matplotlib.pyplot as plt
from aqua.graphics import ConfigStyle, plot_gregory_monthly, plot_gregory_annual
from aqua.util import to_list
from .base import PlotBaseMixin


class PlotGregory(PlotBaseMixin):
    def __init__(self, diagnostic_name: str = 'gregory',
                 t2m_monthly_data=None, net_toa_monthly_data=None,
                 t2m_annual_data=None, net_toa_annual_data=None,
                 t2m_monthly_ref=None, net_toa_monthly_ref=None,
                 t2m_annual_ref=None, net_toa_annual_ref=None,
                 t2m_annual_std=None, net_toa_annual_std=None,
                 loglevel: str = 'WARNING'):
        """
        Initialize the class with the data to be plotted

        Args:
            t2m_monthly_data: List of monthly 2m temperature data
            net_toa_monthly_data: List of monthly net toa data
            t2m_annual_data: List of annual 2m temperature data
            net_toa_annual_data: List of annual net toa data
            t2m_monthy_ref: Monthly reference 2m temperature data
            net_toa_monthy_ref: Monthly reference net toa data
            t2m_annual_ref: Aannual reference 2m temperature data
            net_toa_annual_ref: Annual reference net toa data
            t2m_annual_std: Annual standard deviation of 2m temperature data
            net_toa_annual_std: Annual standard deviation of net toa data
            loglevel: Logging level. Default is 'WARNING'
        """
        super().__init__(loglevel=loglevel, diagnostic_name=diagnostic_name)

        self.monthly_data = {'t2m': to_list(t2m_monthly_data), 'net_toa': to_list(net_toa_monthly_data)}
        self.annual_data = {'t2m': to_list(t2m_annual_data), 'net_toa': to_list(net_toa_annual_data)}
        self.monthly_ref = {'t2m': t2m_monthly_ref, 'net_toa': net_toa_monthly_ref}
        self.annual_ref = {'t2m': t2m_annual_ref, 'net_toa': net_toa_annual_ref}
        self.annual_std = {'t2m': t2m_annual_std, 'net_toa': net_toa_annual_std}

        self.data_dict = {'monthly': self.monthly_data, 'annual': self.annual_data}
        self.ref_dict = {'monthly': self.monthly_ref, 'annual': self.annual_ref}
        self.std_dict = {'monthly': None, 'annual': self.annual_std}

        self.len_data = self._check_data_length()
        self.logger.debug(f'Number of dataset: {self.len_data}')
        self.get_data_info()

    def plot(self, freq=['monthly', 'annual'], title: str = None, 
             data_labels: list = None, ref_label: str = None, style: str = 'aqua'):
        """
        Plot the data

        Args:
            freq: List of frequency for plotting. Default is ['monthly', 'annual']
            title: Title of the plot. Default is None
            data_labels: List of labels for the data. Default is None
            ref_label: Label for the reference data. Default is None
            style: Style of the plot. Default is 'aqua'
        """
        ConfigStyle(style=style)
        ax_monthly = None
        ax_annual = None

        has_monthly = (
            'monthly' in freq and
            any(len(d) >= 2 for d in self.monthly_data['t2m'])
        )
        has_annual = (
            'annual' in freq and
            any(len(d) >= 2 for d in self.annual_data['t2m'])
        )

        self.logger.debug(f'Requested plot freq: {freq}, has_monthly: {has_monthly}, has_annual: {has_annual}')

        if has_monthly and has_annual:
            fig, (ax_monthly, ax_annual) = plt.subplots(1, 2, figsize=(12, 6))
            mon_label = data_labels
            ann_label = None
        elif has_monthly and not has_annual:
            fig, ax_monthly = plt.subplots(1, 1, figsize=(6, 6))
            mon_label = data_labels
            ann_label = None
        elif not has_monthly and has_annual:
            fig, ax_annual = plt.subplots(1, 1, figsize=(6, 6))
            mon_label = None
            ann_label = data_labels
        else:
            raise ValueError('Not enough data to plot. '
                             'At least one of monthly or annual data must have at least 2 data points.')

        if ax_monthly:
            fig, ax_monthly = self.plot_monthly(fig, ax_monthly,
                                                data_labels=mon_label,
                                                ref_label=ref_label)
        if ax_annual:
            fig, ax_annual = self.plot_annual(fig, ax_annual, data_labels=ann_label)

        # We extract the handles and labels from each axis
        # since the labels are defined in the plotting function
        handles_common, labels_common = [], []
        for f in freq:
            if f == 'monthly':
                ax = ax_monthly
            elif f == 'annual':
                ax = ax_annual
            if ax is not None:
                h_for, l_for = ax.get_legend_handles_labels()
                handles_common.extend(h_for)
                labels_common.extend(l_for)

        # Create a single legend at the bottom
        fig.legend(handles_common, labels_common, loc="lower center", ncol=2)

        # Adjust layout to make space
        fig.subplots_adjust(bottom=0.2)

        if title:
            fig.suptitle(title)

        return fig

    def set_title(self):
        """Set the title for the plot"""
        title = 'Gregory Plot '

        for i, model in enumerate(self.models):
            title += f'{model}'
            title += f' {self.exps[i]}'

        return title

    def set_ref_label(self):
        """Set the reference label for the plot"""
        # TODO: Improve this to avoid the Try/Except
        try:
            ref_label = f"{self.ref_models["t2m"]} {self.ref_exps["t2m"]}"
            ref_label += f" {self.ref_models["net_toa"]} {self.ref_exps["net_toa"]}"
        except KeyError:
            ref_label = None

        return ref_label

    def set_description(self):
        """Set the description for the plot"""
        description = 'Gregory Plot'
        for i, model in enumerate(self.models):
            description += f' {model}'
            description += f' {self.exps[i]}'
        if self.ref_models['t2m'] is not None or self.ref_models['net_toa'] is not None:
            description += ' using as a reference'
        if self.ref_models['t2m'] is not None:
            description += f' {self.ref_models["t2m"]} {self.ref_exps["t2m"]} (2 m temperature)'
        if self.ref_models['net_toa'] is not None:
            description += f' {self.ref_models["net_toa"]} {self.ref_exps["net_toa"]} (net TOA).'
        return description

    def plot_monthly(self, fig: plt.Figure, ax: plt.Axes,
                     data_labels: list = None, ref_label: str = None):
        """
        Plot the monthly data

        Args:
            fig: Figure object
            ax: Axes object
            data_labels: List of labels for the data. Default is None
            ref_label: Label for the reference data. Default is None

        Returns:
            fig: Figure object
            ax: Axes object
        """
        fig, ax = plot_gregory_monthly(t2m_monthly_data=self.monthly_data['t2m'],
                                       net_toa_monthly_data=self.monthly_data['net_toa'],
                                       t2m_monthly_ref=self.monthly_ref['t2m'],
                                       net_toa_monthly_ref=self.monthly_ref['net_toa'],
                                       fig=fig, ax=ax, loglevel=self.loglevel,
                                       labels=data_labels, ref_label=ref_label)
        return fig, ax

    def plot_annual(self, fig: plt.Figure, ax: plt.Axes,
                    data_labels: list = None):
        """
        Plot the annual data

        Args:
            fig: Figure object
            ax: Axes object
            data_labels: List of labels for the data. Default is None

        Returns:
            fig: Figure object
            ax: Axes object
        """
        fig, ax = plot_gregory_annual(t2m_annual_data=self.annual_data['t2m'],
                                      net_toa_annual_data=self.annual_data['net_toa'],
                                      t2m_annual_ref=self.annual_ref['t2m'],
                                      net_toa_annual_ref=self.annual_ref['net_toa'],
                                      t2m_std=self.std_dict['annual']['t2m'],
                                      net_toa_std=self.std_dict['annual']['net_toa'],
                                      fig=fig, ax=ax, loglevel=self.loglevel,
                                      labels=data_labels)
        return fig, ax

    def get_data_info(self):
        """
        We extract the data needed for labels, description etc
        from the data arrays attributes.

        The attributes are:
        - AQUA_catalog
        - AQUA_model
        - AQUA_exp
        """
        for var in self.data_dict.values():
            for data in var.values():
                self.catalogs = [d.AQUA_catalog for d in data]
                self.models = [d.AQUA_model for d in data]
                self.exps = [d.AQUA_exp for d in data]

        if self.ref_dict['monthly']['t2m'] is not None:
            t2m_catalog = self.ref_dict['monthly']['t2m'].AQUA_catalog
            t2m_model = self.ref_dict['monthly']['t2m'].AQUA_model
            t2m_exp = self.ref_dict['monthly']['t2m'].AQUA_exp
        elif self.ref_dict['annual']['t2m'] is not None:
            t2m_catalog = self.ref_dict['annual']['t2m'].AQUA_catalog
            t2m_model = self.ref_dict['annual']['t2m'].AQUA_model
            t2m_exp = self.ref_dict['annual']['t2m'].AQUA_exp
        else:
            t2m_catalog = None
            t2m_model = None
            t2m_exp = None

        if self.ref_dict['monthly']['net_toa'] is not None:
            net_toa_catalog = self.ref_dict['monthly']['net_toa'].AQUA_catalog
            net_toa_model = self.ref_dict['monthly']['net_toa'].AQUA_model
            net_toa_exp = self.ref_dict['monthly']['net_toa'].AQUA_exp
        elif self.ref_dict['annual']['net_toa'] is not None:
            net_toa_catalog = self.ref_dict['annual']['net_toa'].AQUA_catalog
            net_toa_model = self.ref_dict['annual']['net_toa'].AQUA_model
            net_toa_exp = self.ref_dict['annual']['net_toa'].AQUA_exp
        else:
            net_toa_catalog = None
            net_toa_model = None
            net_toa_exp = None

        self.ref_catalogs = {'t2m': t2m_catalog, 'net_toa': net_toa_catalog}
        self.ref_models = {'t2m': t2m_model, 'net_toa': net_toa_model}
        self.ref_exps = {'t2m': t2m_exp, 'net_toa': net_toa_exp}

    def _check_data_length(self):
        """
        Check that the length of monthly and annual data is the same
        and returns the value
        """
        len_data = None
        for freq, data in self.data_dict.items():
            for var, d in data.items():
                if len(d) > 0:
                    if len_data is None:
                        len_data = len(d)
                    elif len_data != len(d):
                        raise ValueError(f'Length of {var} {freq} data is not the same')
        return len_data
