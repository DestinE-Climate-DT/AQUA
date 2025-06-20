class PlotLatLonProfiles:
    
    def __init__(self, loglevel: str = 'WARNING'):
    
        # Data info initalized as empty
        self.loglevel = loglevel
        self.catalogs = None
        self.models = None
        self.exps = None
        self.ref_catalogs = None
        self.ref_models = None
        self.ref_exps = None
        self.std_startdate = None
        self.std_enddate = None

    def set_data_labels(self):
        """
        Set the data labels for the plot.
        The labels are extracted from the data arrays attributes.

        Returns:
            data_labels (list): List of data labels for the plot.
        """
        data_labels = []
        for i in range(self.len_data):
            label = f'{self.models[i]} {self.exps[i]}'
            data_labels.append(label)

        self.logger.debug('Data labels: %s', data_labels)
        return data_labels

    def set_title(self, region: str = None, var: str = None, units: str = None, diagnostic: str = None):
        """
        Set the title for the plot.

        Args:
            region (str): Region to be used in the title.
            var (str): Variable name to be used in the title.
            units (str): Units of the variable to be used in the title.
            diagnostic (str): Diagnostic name to be used in the title.

        Returns:
            title (str): Title for the plot.
        """
        title = f'{diagnostic} '
        if var is not None:
            title += f'for {var} '

        if units is not None:
            title += f'[{units}] '

        if region is not None:
            title += f'[{region}] '

        if self.len_data == 1:
            title += f'for {self.catalogs[0]} {self.models[0]} {self.exps[0]} '

        self.logger.debug('Title: %s', title)
        return title
    
    def set_description(self, region: str = None, diagnostic: str = None):
        """
        Set the caption for the plot.
        The caption is extracted from the data arrays attributes and the
        reference data arrays attributes.
        The caption is stored as 'Description' in the metadata dictionary.

        Args:
            region (str): Region to be used in the caption.
            diagnostic (str): Diagnostic name to be used in the caption.

        Returns:
            description (str): Caption for the plot.
        """

        description = f'{diagnostic} '
        if region is not None:
            description += f'for region {region} '

        for i in range(self.len_data):
            description += f'for {self.catalogs[i]} {self.models[i]} {self.exps[i]} '

        for i in range(self.len_ref):
            if self.ref_models[i] == 'ERA5':
                description += f'with reference {self.ref_models[i]} '
            else:
                description += f'with reference {self.ref_models[i]} {self.ref_exps[i]} '

        if self.std_startdate is not None and self.std_enddate is not None:
            description += f'with standard deviation from {self.std_startdate} to {self.std_enddate} '

        self.logger.debug('Description: %s', description)
        return description
    
    def save_plot(self, fig, var: str = None, description: str = None, region: str = None, rebuild: bool = True,
                    outputdir: str = './', dpi: int = 300, format: str = 'png', diagnostic: str = None):
        """
        Save the plot to a file.

        Args:
            fig (matplotlib.figure.Figure): Figure object.
            var (str): Variable name to be used in the title and description.
            description (str): Description of the plot.
            region (str): Region to be used in the title and description.
            rebuild (bool): If True, rebuild the plot even if it already exists.
            outputdir (str): Output directory to save the plot.
            dpi (int): Dots per inch for the plot.
            format (str): Format of the plot ('png' or 'pdf'). Default is 'png'.
            diagnostic (str): Diagnostic name to be used in the filename as diagnostic_product.
        """
        outputsaver = OutputSaver(diagnostic='timeseries', 
                                catalog=self.catalogs[0],
                                model=self.models[0],
                                exp=self.exps[0],
                                outdir=outputdir,
                                rebuild=rebuild,
                                loglevel=self.loglevel)

        metadata = {"Description": description, "dpi": dpi }
        extra_keys = {'diagnostic_product': diagnostic}

        if var is not None:
            extra_keys.update({'var': var})
        if region is not None:
            region = region.replace(' ', '').lower() if region is not None else None
            extra_keys.update({'region': region})

        if format == 'png':
            outputsaver.save_png(fig, diagnostic_product=diagnostic, extra_keys=extra_keys, metadata=metadata)
        elif format == 'pdf':
            outputsaver.save_pdf(fig, diagnostic_product=diagnostic, extra_keys=extra_keys, metadata=metadata)
        else:
            raise ValueError(f'Format {format} not supported. Use png or pdf.')
        
        
    def run(self, var: str, units: str = None, region: str = None, outputdir: str = './',
            rebuild: bool = True, dpi: int = 300, format: str = 'png'):
        """
        Run the PlotTimeseries class.

        Args:
            var (str): Variable name to be used in the title and description.
            units (str): Units of the variable to be used in the title.
            region (str): Region to be used in the title and description.
            outputdir (str): Output directory to save the plot.
            rebuild (bool): If True, rebuild the plot even if it already exists.
            dpi (int): Dots per inch for the plot.
            format (str): Format of the plot ('png' or 'pdf'). Default is 'png'.
        """

        self.logger.info('Running PlotTimeseries')
        data_label = self.set_data_labels()
        ref_label = self.set_ref_label()
        description = self.set_description(region=region)
        title = self.set_title(region=region, var=var, units=units)
        fig, _ = self.plot_lat_lon_profiles(data_labels=data_label, ref_label=ref_label, title=title)
        region_short = region.replace(' ', '').lower() if region is not None else None
        self.save_plot(fig, var=var, description=description, region=region_short, rebuild=rebuild,
                       outputdir=outputdir, dpi=dpi, format=format)
        self.logger.info('PlotTimeseries completed successfully')

    def get_data_info(self):
        """
        We extract the data needed for labels, description etc
        from the data arrays attributes.

        The attributes are:
        - AQUA_catalog
        - AQUA_model
        - AQUA_exp
        - std_startdate
        - std_enddate
        """
        for data in [self.monthly_data, self.annual_data]:
            if data is not None:
                # Make a list from the data array attributes
                self.catalogs = [d.AQUA_catalog for d in data]
                self.models = [d.AQUA_model for d in data]
                self.exps = [d.AQUA_exp for d in data]
                break
        self.logger.debug(f'Catalogs: {self.catalogs}')
        self.logger.debug(f'Models: {self.models}')
        self.logger.debug(f'Experiments: {self.exps}')

        # TODO: support ref list
        for ref in [self.ref_monthly_data, self.ref_annual_data]:
            if ref is not None:
                self.ref_catalogs = ref.AQUA_catalog
                self.ref_models = ref.AQUA_model
                self.ref_exps = ref.AQUA_exp
                break
        self.logger.debug(f'Reference: {self.ref_catalogs} {self.ref_models} {self.ref_exps}')

        for std in [self.std_monthly_data, self.std_annual_data]:
            if std is not None:
                self.std_startdate = std.std_startdate if std.std_startdate is not None else None
                self.std_enddate = std.std_enddate if std.std_enddate is not None else None
                break
        self.logger.debug(f'Standard deviation dates: {self.std_startdate} - {self.std_enddate}')

    def set_title(self, region: str = None, var: str = None, units: str = None):
        """
        Set the title for the plot.

        Args:
            region (str): Region to be used in the title.
            var (str): Variable name to be used in the title.
            units (str): Units of the variable to be used in the title.

        Returns:
            title (str): Title for the plot.
        """
        title = super().set_title(region=region, var=var, units=units, diagnostic='Time series')
        return title

    def set_description(self, region: str = None):
        """
        Set the caption for the plot.
        The caption is extracted from the data arrays attributes and the
        reference data arrays attributes.
        The caption is stored as 'Description' in the metadata dictionary.

        Args:
            region (str): Region to be used in the caption.

        Returns:
            description (str): Caption for the plot.
        """
        description = super().set_description(region=region, diagnostic='Time series')
        return description
    
    def plot_timeseries(self, data_labels=None, ref_label=None, title=None):
        """
        Plot the time series data.

        Args:
            data_labels (list): List of data labels.
            ref_label (str): Reference label.
            title (str): Title of the plot.

        Returns:
            fig (matplotlib.figure.Figure): Figure object.
            ax (matplotlib.axes.Axes): Axes object.
        """
        fig, ax = plot_timeseries(monthly_data=self.monthly_data,
                                  ref_monthly_data=self.ref_monthly_data,
                                  std_monthly_data=self.std_monthly_data,
                                  annual_data=self.annual_data,
                                  ref_annual_data=self.ref_annual_data,
                                  std_annual_data=self.std_annual_data,
                                  data_labels=data_labels, ref_label=ref_label,
                                  title=title, return_fig=True, loglevel=self.loglevel)

        return fig, ax

    def save_plot(self, fig, var: str, description: str = None, region: str = None, rebuild: bool = True,
                  outputdir: str = './', dpi: int = 300, format: str = 'png'):
        """
        Save the plot to a file.

        Args:
            fig (matplotlib.figure.Figure): Figure object.
            var (str): Variable name to be used in the title and description.
            description (str): Description of the plot.
            region (str): Region to be used in the title and description.
            rebuild (bool): If True, rebuild the plot even if it already exists.
            outputdir (str): Output directory to save the plot.
            dpi (int): Dots per inch for the plot.
            format (str): Format of the plot ('png' or 'pdf'). Default is 'png'.
        """
        super().save_plot(fig=fig, var=var, description=description,
                          region=region, rebuild=rebuild,
                          outputdir=outputdir, dpi=dpi, format=format, diagnostic='timeseries')

    def _check_data_length(self):
        """
        Check if all data arrays have the same length.
        Does the same for the reference data.
        If not, raise a ValueError.

        Return:
            data_length (int): Length of the data arrays.
            ref_length (int): Length of the reference data arrays.
        """
        data_length = 0
        ref_length = 0

        if self.monthly_data and self.annual_data:
            if len(self.monthly_data) != len(self.annual_data):
                raise ValueError('Monthly and annual data list must have the same length')
            else:
                data_length = len(self.monthly_data)

        if self.ref_monthly_data is not None or self.ref_annual_data is not None:
            ref_length = 1
        # # TODO: uncomment when support to list is implemented
        # if self.ref_monthly_data and self.ref_annual_data:
        #     if len(self.ref_monthly_data) != len(self.ref_annual_data):
        #         raise ValueError('Reference monthly and annual data list must have the same length')
        #     else:
        #         ref_length = len(self.ref_monthly_data)

        # if self.std_monthly_data and self.std_annual_data:
        #     if len(self.std_monthly_data) != len(self.std_annual_data):
        #         raise ValueError('Standard deviation monthly and annual data list must have the same length')
        #     else:
        #         if len(self.std_monthly_data) != ref_length:
        #             raise ValueError('Standard deviation monthly and annual data list must have the same length as reference data')

        return data_length, ref_length
