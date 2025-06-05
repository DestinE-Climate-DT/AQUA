"""
LRA Output Path Builder
"""

import os
from typing import Optional

class OutputPathBuilder:
    """
    Class to build output paths for LRA data files.
    """

    def __init__(self, catalog: str, model: str, exp: str, var: str,
             realization: str = 'r1', resolution: Optional[str] = None,
             frequency: Optional[str] = None, stat: str = 'mean',
             region: str = 'global', level: Optional[str] = None,
             **kwargs):
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


    def set_from_reader(self, reader_obj):
        """Guess resolution and frequency from xarray."""
        if self.resolution is None:
            self.resolution = reader_obj.src_grid_name
        if self.frequency is None:
            self.frequency = "native"

    def build_path(self, basedir, year, month=None, day=None):
        """create the full path to the output file."""
        folder = self.build_directory()
        filename = self.build_filename(year, month, day)
        return os.path.join(basedir, folder, filename)

    def build_directory(self):
        """create the output directory based on the parameters."""
        parts = [
            self.catalog, self.model, self.exp, self.realization,
            self.resolution, self.frequency, self.stat, self.region
        ]
        folder = os.path.join(*[p for p in parts if p])
        #os.makedirs(folder, exist_ok=True)
        return folder

    def build_filename(self, year, month=None, day=None):
        """create the filename based on the parameters."""
        varname = f"{self.var}{self.level}" if self.level else self.var

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

        # Combine date parts with their format lengths
        date_parts = {
            'year': (year, 4),
            'month': (month, 2),
            'day': (day, 2),
        }

        # loop to format the dates
        date_components = ""
        for _, (value, length) in date_parts.items():
            date_formatted = self.format_component(value, length)
            if date_formatted:
                date_components = date_components + date_formatted


        # collapse all the component to create the final file
        filename = "_".join(str(c) for c in components + [date_components] if c) + ".nc"
    
        return filename
    

    def format_component(self, value, length):
        """Format a value as zero-padded string if it's not a wildcard, else return as-is."""
        if value is None:
            return ""
        if self.is_wildcard(value):
            return value
        return f"{int(value):0{length}d}"
    
    @staticmethod
    def is_wildcard(s):
        """Guess if a string includes a wildcard"""
        return any(char in str(s) for char in ['*', '?', '[', ']'])
