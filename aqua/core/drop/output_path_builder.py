"""
DROP Output Path Builder
"""

import os
from typing import Optional
from aqua.core.util import format_realization


class OutputPathBuilder:
    """
    Class to build output paths for DROP data files.
    """

    def __init__(self, catalog: str, model: str, exp: str,
                 resolution: Optional[str] = None,
                 realization: Optional[str] = None,
                 frequency: Optional[str] = None, stat: Optional[str] = None,
                 region: Optional[str] = None, level: Optional[str] = None,
                 **kwargs):
        """
        Initialize the OutputPathBuilder with the necessary parameters.
        Params:
            catalog (str): Catalog name.
            model (str): Model name.
            exp (str): Experiment name.
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
        self.resolution = resolution

        self.realization = format_realization(realization)  # ensure realization is formatted correctly
        self.frequency = frequency if frequency is not None else 'native'
        self.stat = stat if stat is not None else 'nostat'
        self.region = region if region is not None else 'global'

        self.level = level
        self.kwargs = kwargs or {}

    def build_path(self, basedir, var, year=None, month=None, day=None):
        """Ceate the full path to the output file.

        Args:
            basedir (str): Base directory for the output files.
            var (str): Variable name to include in the filename. Can be a wildcard.
            year (int, optional): Year to include in the filename. Defaults to None.
                                  Can be a wildcard.
            month (int, optional): Month to include in the filename. Defaults to None.
                                   Can be a wildcard.
            day (int, optional): Day to include in the filename. Defaults to None.
                                 Can be a wildcard.
        Returns:
            str: The full path to the output file.
        """
        folder = self.build_directory()
        filename = self.build_filename(var, year, month, day)
        return os.path.join(basedir, folder, filename)

    def build_directory(self):
        """
        Create the output directory based on the class parameters.

        Returns:
            str: The directory path for the output files.
        """
        parts = [
            self.catalog, self.model, self.exp, self.realization,
            self.resolution, self.frequency, self.stat, self.region
        ]
        folder = os.path.join(*[p for p in parts if p])
        return folder

    def build_filename(self, var=None, year=None, month=None, day=None):
        """
        Create the filename based on the class parameters.
        Variable and year are set as wildcards by default if not provided.
        Date format is forced to be zero-padded (e.g., 2023, 01, 01).

        Args:
            var (str, optional): Variable name to include in the filename. Defaults to None. Can be a wildcard.
            year (int, optional): Year to include in the filename. Defaults to None. Can be a wildcard.
            month (int, optional): Month to include in the filename. Defaults to None. Can be a wildcard.
            day (int, optional): Day to include in the filename. Defaults to None. Can be a wildcard.

        Returns:
            str: The filename for the output file.
        """

        # Use the provided variable or default to wildcard '*'
        var = "*" if var is None else var
        year = "*" if year is None else year

        # specific case for potential levels
        varname = f"{var}{self.level}" if self.level else var

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
        """
        Format a value as zero-padded string if it's not a wildcard, else return as-is.

        Args:
            value (int or str): The value to format. Can be an integer or a wildcard
            length (int): The length to zero-pad the value to, if applicable.
        """

        if value is None:
            return ""
        if self.is_wildcard(value):
            return value
        return f"{int(value):0{length}d}"

    @staticmethod
    def is_wildcard(s):
        """Guess if a string includes a wildcard"""
        return any(char in str(s) for char in ['*', '?', '[', ']'])
