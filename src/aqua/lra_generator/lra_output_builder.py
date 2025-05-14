"""
LRA Output Path Builder
"""

import os

class OutputPathBuilder:
    def __init__(self, catalog, model, exp, var, 
                 realization='r1', resolution=None, frequency=None,
                 stat='nostat', region='global', level=None, **kwargs):
        """
        Initialize the OutputPathBuilder with the necessary parameters.
        Params:
            catalog (str): Catalog name.
            model (str): Model name.
            exp (str): Experiment name.
            var (str): Variable name.
            realization (str): Realization name. Default is 'r1'.
            resolution (str): Resolution. Default is None.
            frequency (str): Frequency. Default is None.
            stat (str): Statistic type. Default is 'nostat'.
            region (str): Region. Default is 'global'.
            level (str): Level. Default is None.
            kwargs: Additional keyword arguments for flexibility.
        """

        self.catalog = catalog
        self.model = model
        self.exp = exp
        self.var = var
        self.realization = realization
        self.resolution = resolution
        self.frequency = frequency
        self.stat = stat
        self.region = region
        self.level = level
        self.kwargs = kwargs or {}

    def _deduce_resolution(self, original_xarray):
        # Dummy implementation: replace with real deduction logic
        return '1deg'

    def _deduce_frequency(self, original_xarray):
        # Dummy implementation: replace with real deduction logic
        return 'monthly'

    def set_from_xarray(self, xarray_obj):
        """Guess resolution and frequency from xarray."""
        if self.resolution is None:
            self.resolution = self._deduce_resolution(xarray_obj)
        if self.frequency is None:
            self.frequency = self._deduce_frequency(xarray_obj)

    def create_directory(self):
        parts = [
            self.catalog, self.model, self.exp, self.realization,
            self.resolution, self.frequency, self.stat, self.region
        ]
        folder = os.path.join(*[p for p in parts if p])
        #os.makedirs(folder, exist_ok=True)
        return folder

    def create_filename(self, year, month=None, day=None):
        varname = f"{self.var}_{self.level}" if self.level else self.var

        components = [
            varname,
            self.catalog,
            self.model,
            self.exp,
            self.realization,
            self.resolution,
            self.frequency,
            self.stat,
            self.region,
        ]

        # Convert kwargs to flat string
        kwargs_str = "_".join(f"{k}{v}" for k, v in self.kwargs.items()) if self.kwargs else ""
        components.append(kwargs_str)

        yyyy = f"{year:04d}" if year else ""
        mm = f"{month:02d}" if month else ""
        dd = f"{day:02d}" if day else ""
        components.append(f"{yyyy}{mm}{dd}")

        filename = "_".join(str(c) for c in components if c) + ".nc"
    
        return filename