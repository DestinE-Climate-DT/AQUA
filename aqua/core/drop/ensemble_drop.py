"""
Ensemble DROP class

This class extends DROP to support multiple realizations
and ensemble reductions such as ensemble mean/std/min/max.

The ensemble reduction is performed point-wise across the
realization dimension before writing the final DROP output.
"""

import xarray as xr

from aqua.core.reader import Reader

from .drop import Drop


AVAILABLE_ENSEMBLE_STATS = [
    "mean",
    "std",
    "min",
    "max",
]


class EnsembleDrop(Drop):
    """
    DROP class supporting ensemble reductions.

    Example
    -------
    EnsembleDrop(
        catalog="mycatalog",
        model="ICON",
        exp="historical",
        source="native",
        var="tas",
        resolution="r100",
        frequency="monthly",
        ensemble_members=[
            "r1i1p1f1",
            "r2i1p1f1",
            "r3i1p1f1",
        ],
        ensemble_stat="mean",
    )
    """

    def __init__(
        self,
        ensemble_members,
        ensemble_stat="mean",
        ensemble_dim="realization",
        *args,
        **kwargs,
    ):
        """
        Initialize EnsembleDrop.

        Parameters
        ----------
        ensemble_members : list[str]
            List of realizations to load.

        ensemble_stat : str
            Ensemble statistic to compute.
            Available:
                - mean
                - std
                - min
                - max

        ensemble_dim : str
            Name of ensemble dimension.
            Default: realization
        """

        if not isinstance(ensemble_members, list):
            raise TypeError(
                "ensemble_members must be a list."
            )

        if len(ensemble_members) == 0:
            raise ValueError(
                "ensemble_members cannot be empty."
            )

        if ensemble_stat not in AVAILABLE_ENSEMBLE_STATS:
            raise ValueError(
                f"Invalid ensemble_stat: {ensemble_stat}. "
                f"Available: {AVAILABLE_ENSEMBLE_STATS}"
            )

        self.ensemble_members = ensemble_members
        self.ensemble_stat = ensemble_stat
        self.ensemble_dim = ensemble_dim

        #
        # IMPORTANT:
        # represent ensemble products as synthetic realizations
        #
        kwargs["realization"] = (
            f"ensemble-{ensemble_stat}"
        )

        super().__init__(*args, **kwargs)

    def retrieve(self):
        """
        Retrieve all ensemble members and concatenate them
        along the realization dimension.
        """

        self.logger.info(
            "Loading %d ensemble members",
            len(self.ensemble_members),
        )

        datasets = []

        for member in self.ensemble_members:

            self.logger.info(
                "Loading ensemble member %s",
                member,
            )

            reader = Reader(
                model=self.model,
                exp=self.exp,
                source=self.source,
                regrid=(
                    self.resolution
                    if self.resolution != "native"
                    else None
                ),
                catalog=self.catalog,
                loglevel=self.loglevel,
                rebuild=self.rebuild,
                startdate=self.startdate,
                enddate=self.enddate,
                fix=self.fix,
                engine=self.engine,
                realization=member,
                **self.kwargs,
            )

            ds = reader.retrieve(var=self.var)

            #
            # add realization coordinate
            #
            ds = ds.expand_dims(
                {
                    self.ensemble_dim: [member]
                }
            )

            datasets.append(ds)

        self.logger.info(
            "Concatenating ensemble members..."
        )

        self.data = xr.concat(
            datasets,
            dim=self.ensemble_dim,
            coords="minimal",
            compat="override",
            combine_attrs="override",
        )

        self.logger.info(
            "Ensemble dataset successfully created."
        )

        self.reader = reader

    def apply_ensemble_stat(self, data):
        """
        Apply ensemble reduction.

        Parameters
        ----------
        data : xr.DataArray or xr.Dataset

        Returns
        -------
        xr.DataArray or xr.Dataset
        """

        self.logger.info(
            "Applying ensemble %s over %d members",
            self.ensemble_stat,
            len(self.ensemble_members),
        )

        if self.ensemble_stat == "mean":
            return data.mean(dim=self.ensemble_dim)

        if self.ensemble_stat == "std":
            return data.std(dim=self.ensemble_dim)

        if self.ensemble_stat == "min":
            return data.min(dim=self.ensemble_dim)

        if self.ensemble_stat == "max":
            return data.max(dim=self.ensemble_dim)

        raise ValueError(
            f"Unsupported ensemble statistic "
            f"{self.ensemble_stat}"
        )

    def append_history(self, data):
        """
        Append ensemble metadata/history.
        """

        data = super().append_history(data)

        history = (
            f"ensemble {self.ensemble_stat} "
            f"computed over "
            f"{len(self.ensemble_members)} members"
        )

        if "history" in data.attrs:
            data.attrs["history"] += f"\n{history}"
        else:
            data.attrs["history"] = history

        data.attrs["ensemble_members"] = ",".join(
            self.ensemble_members
        )

        data.attrs["ensemble_size"] = len(
            self.ensemble_members
        )

        data.attrs["ensemble_stat"] = (
            self.ensemble_stat
        )

        return data

    def _write_var_catalog(self, var):
        """
        Override DROP variable writing to include
        ensemble reduction.
        """

        self.logger.info(
            "Processing ensemble variable %s...",
            var,
        )

        temp_data = self.data[var]

        #
        # temporal reduction first
        #
        if self.frequency:

            temp_data = self.reader.timstat(
                temp_data,
                self.stat,
                freq=self.frequency,
                exclude_incomplete=(
                    self.exclude_incomplete
                ),
                func_kwargs=self.stat_kwargs,
            )

        #
        # ensemble reduction second
        #
        temp_data = self.apply_ensemble_stat(
            temp_data
        )

        #
        # regrid
        #
        if (
            self.resolution
            and self.resolution != "native"
        ):
            temp_data = self.reader.regrid(
                temp_data
            )

            temp_data = self._remove_regridded(
                temp_data
            )

        #
        # regional selection
        #
        if self.region:

            temp_data = self.reader.select_area(
                temp_data,
                lon=self.region["lon"],
                lat=self.region["lat"],
                drop=self.drop,
            )

        #
        # continue with standard DROP logic
        #
        years = sorted(
            set(temp_data.time.dt.year.values)
        )

        if self.performance_reporting:
            years = [years[0]]

        for year in years:

            self.logger.info(
                "Processing year %s...",
                str(year),
            )

            yearfile = self.get_filename(
                var,
                year=year,
            )

            year_data = temp_data.sel(
                time=temp_data.time.dt.year == year
            )

            months = sorted(
                set(year_data.time.dt.month.values)
            )

            if self.performance_reporting:
                months = [months[0]]

            for month in months:

                outfile = self.get_filename(
                    var,
                    year=year,
                    month=month,
                )

                month_data = year_data.sel(
                    time=(
                        year_data.time.dt.month
                        == month
                    )
                )

                if self.definitive:

                    tmpfile = self.get_filename(
                        var,
                        year=year,
                        month=month,
                        tmp=True,
                    )

                    self.write_chunk(
                        month_data,
                        tmpfile,
                    )

                del month_data

            del year_data

            if self.definitive and self.compact:
                self._concat_var_year(
                    var,
                    year,
                )

        del temp_data
