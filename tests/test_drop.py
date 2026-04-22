import os
import re
import shutil

import pandas as pd
import pytest
import xarray as xr
from conftest import LOGLEVEL

from aqua import Drop
from aqua.core.drop.catalog_entry_builder import CatalogEntryBuilder
from aqua.core.drop.drop import available_stats
from aqua.core.drop.output_path_builder import OutputPathBuilder

DROP_PATH = "ci/IFS/test-tco79/r1/r100/monthly/mean/global"
DROP_PATH_DAILY = "ci/IFS/test-tco79/r1/r100/daily/mean/europe"

# pytestmark groups tests that run sequentially on the same worker to avoid conflicts
pytestmark = [pytest.mark.aqua, pytest.mark.console, pytest.mark.xdist_group(name="dask_operations")]


@pytest.fixture(params=[{"model": "IFS", "exp": "test-tco79", "source": "long", "var": "2t", "outdir": "drop_test"}])
def drop_arguments(request):
    """Provides DROP arguments as a dictionary."""
    return request.param


class TestOutputPathBuilder:
    """Class containing tests for OutputPathBuilder."""

    expected = [
        None,
        "ci/IFS/test-tco79/r1/native/nostat/europe/2t_ci_IFS_test-tco79_r1_native_nostat_europe_202001.nc",
        None,
    ]

    @pytest.mark.parametrize(
        "resolution, frequency, realization, region, stat, expected",
        [
            ("r100", "monthly", "r1", "global", "mean", expected[0]),
            (None, None, None, "europe", None, expected[1]),
            ("r200", "daily", "r2", "global", "nostat", expected[2]),
        ],
    )
    def test_build_path(self, drop_arguments, resolution, frequency, realization, region, stat, expected):
        """Test building output path."""
        builder = OutputPathBuilder(
            catalog="ci",
            model=drop_arguments["model"],
            exp=drop_arguments["exp"],
            resolution=resolution,
            frequency=frequency,
            realization=realization,
            stat=stat,
            region=region,
        )
        path = builder.build_path(
            os.path.join(os.getcwd(), drop_arguments["outdir"]), var=drop_arguments["var"], year=2020, month=1
        )

        if not expected:
            drop_path = f"ci/IFS/test-tco79/{realization}/{resolution}/{frequency}/{stat}/{region}"
            expected = os.path.join(
                os.getcwd(),
                drop_arguments["outdir"],
                drop_path,
                f"2t_ci_IFS_test-tco79_{realization}_{resolution}_{frequency}_{stat}_{region}_202001.nc",
            )
        else:
            expected = os.path.join(os.getcwd(), drop_arguments["outdir"], expected)

        assert path == expected


class TestCatalogEntryBuilder:
    """Class containing tests for CatalogEntryBuilder."""

    @pytest.mark.parametrize(
        "resolution, frequency, realization, region, stat, source_grid_name, chunks",
        [
            ("r100", "monthly", "r1", "global", "mean", "lon-lat-r100", {"time": 12, "lat": 180, "lon": 360}),
            ("r200", "daily", "r1", "global", "mean", "lon-lat", {"time": 365, "lat": 90, "lon": 180}),
            ("r050s", "monthly", "r4", "europe", "max", "lon-lat", {"time": 12, "lat": 361, "lon": 720}),
        ],
    )
    def test_create_entry_name(
        self,
        drop_arguments,
        resolution,
        frequency,
        realization,
        region,
        stat,
        source_grid_name,
        chunks,
    ):
        """Test creation of entry name."""
        builder = CatalogEntryBuilder(
            catalog="ci",
            **drop_arguments,
            resolution=resolution,
            frequency=frequency,
            realization=realization,
            stat=stat,
            region=region,
            loglevel=LOGLEVEL,
        )
        entry_name = builder.create_entry_name()
        block = builder.create_entry_details(basedir=drop_arguments["outdir"], source_grid_name=source_grid_name)
        entry_name_zarr = builder.create_entry_name(output_format="zarr")
        block_zarr = builder.create_entry_details(
            basedir=drop_arguments["outdir"], source_grid_name=source_grid_name, output_format="zarr"
        )

        if resolution == "r100" and frequency == "monthly":
            expected_name = f"lra-{resolution}-{frequency}"
        else:
            expected_name = f"{resolution}-{frequency}"

        assert entry_name == expected_name
        assert entry_name_zarr == expected_name + "-zarr"
        assert block["driver"] == "netcdf"
        assert block["parameters"].keys() == {"realization", "stat", "region"}
        assert block_zarr["driver"] == "zarr"

        builder2 = CatalogEntryBuilder(
            catalog="ci",
            **drop_arguments,
            resolution=resolution,
            frequency=frequency,
            realization="r2",
            stat=stat,
            region=region,
            loglevel=LOGLEVEL,
        )
        newblock = builder2.create_entry_details(
            basedir=drop_arguments["outdir"],
            catblock=block,
            source_grid_name=source_grid_name,
        )
        assert newblock["args"]["urlpath"] == block["args"]["urlpath"]
        assert newblock["parameters"]["realization"]["allowed"] == [realization, "r2"]
        assert newblock["args"]["chunks"] == chunks


class TestDROP:
    """Class containing DROP tests."""

    def test_definitive_false(self, drop_arguments, tmp_path):
        """Test DROP with definitive=False."""
        test = Drop(
            catalog="ci", **drop_arguments, tmpdir=str(tmp_path), resolution="r100", frequency="monthly", loglevel=LOGLEVEL
        )

        test.retrieve()
        test.drop_generator()
        assert os.path.isdir(os.path.join(os.getcwd(), drop_arguments["outdir"], DROP_PATH))
        shutil.rmtree(os.path.join(drop_arguments["outdir"]))

    @pytest.mark.parametrize("nworkers", [1, 2])
    def test_definitive_true(self, drop_arguments, tmp_path, nworkers):
        test = Drop(
            catalog="ci",
            **drop_arguments,
            tmpdir=str(tmp_path),
            nproc=nworkers,
            resolution="r100",
            frequency="monthly",
            definitive=True,
            loglevel=LOGLEVEL,
        )

        test.retrieve()
        test.data = test.data.sel(time="2020-01")
        test.drop_generator()

        file_path = os.path.join(
            os.getcwd(),
            drop_arguments["outdir"],
            DROP_PATH,
            "2t_ci_IFS_test-tco79_r1_r100_monthly_mean_global_202001.nc",
        )
        test.check_integrity(varname=drop_arguments["var"])
        assert os.path.isfile(file_path)

        file = xr.open_dataset(file_path)
        assert len(file.time) == 1
        assert pytest.approx(file["2t"][0, 1, 1].item()) == 248.0704
        shutil.rmtree(os.path.join(drop_arguments["outdir"]))

    def test_regional_subset(self, drop_arguments, tmp_path):
        """Test DROP with regional subset."""
        region = {"name": "europe", "lon": [-10, 30], "lat": [35, 70]}

        test = Drop(
            catalog="ci",
            **drop_arguments,
            tmpdir=str(tmp_path),
            resolution="r100",
            frequency="daily",
            definitive=True,
            loglevel=LOGLEVEL,
            region=region,
        )

        test.retrieve()
        test.data = test.data.sel(time="2020-01-20")
        test.drop_generator()

        file_path = os.path.join(
            os.getcwd(),
            drop_arguments["outdir"],
            DROP_PATH_DAILY,
            "2t_ci_IFS_test-tco79_r1_r100_daily_mean_europe_202001.nc",
        )
        assert os.path.isfile(file_path), "File not found: {}".format(file_path)

        xfield = xr.open_dataset(file_path).where(lambda x: x.notnull(), drop=True)
        assert xfield.lat.min() > 35
        assert xfield.lat.max() < 70
        shutil.rmtree(os.path.join(drop_arguments["outdir"]))

    def test_dask_overwrite(self, drop_arguments, tmp_path):
        """Test DROP with overwrite=True and Dask initialization."""
        test = Drop(
            catalog="ci",
            **drop_arguments,
            tmpdir=str(tmp_path),
            resolution="r100",
            frequency="monthly",
            nproc=4,
            loglevel=LOGLEVEL,
            definitive=True,
            overwrite=True,
        )

        test.retrieve()
        test.drop_generator()
        assert os.path.isdir(os.path.join(os.getcwd(), drop_arguments["outdir"], DROP_PATH))
        shutil.rmtree(os.path.join(drop_arguments["outdir"]))

    def test_exclude_incomplete(self, drop_arguments, tmp_path):
        """Test DROP's exclude_incomplete option."""
        test = Drop(
            catalog="ci",
            **drop_arguments,
            tmpdir=str(tmp_path),
            resolution="r100",
            frequency="monthly",
            definitive=True,
            loglevel=LOGLEVEL,
            exclude_incomplete=True,
        )

        test.retrieve()
        test.drop_generator()

        missing_file = os.path.join(
            os.getcwd(),
            drop_arguments["outdir"],
            DROP_PATH,
            "2t_ci_IFS_test-tco79_r1_r100_monthly_mean_global_202008.nc",
        )
        existing_file = os.path.join(
            os.getcwd(),
            drop_arguments["outdir"],
            DROP_PATH,
            "2t_ci_IFS_test-tco79_r1_r100_monthly_mean_global_202002.nc",
        )

        assert not os.path.exists(missing_file)
        assert os.path.exists(existing_file)

        file = xr.open_dataset(existing_file)
        assert len(file.time) == 1
        test.check_integrity(varname=drop_arguments["var"])
        shutil.rmtree(os.path.join(drop_arguments["outdir"]))

    def test_concat_var_year(self, drop_arguments, tmp_path):
        """Test concatenation of monthly files into a single yearly file."""
        resolution = "r100"
        frequency = "monthly"
        year = 2022

        test = Drop(
            catalog="ci", **drop_arguments, tmpdir=str(tmp_path), resolution=resolution, frequency=frequency, loglevel=LOGLEVEL
        )

        # Use the writer already initialized in Drop.__init__()
        for month in range(1, 13):
            mm = f"{month:02d}"
            filename = test.writer.get_filename(drop_arguments["var"], year, month=mm)
            timeobj = pd.Timestamp(f"{year}-{mm}-01")
            ds = xr.Dataset({drop_arguments["var"]: xr.DataArray([0], dims=["time"], coords={"time": [timeobj]})})
            ds.to_netcdf(filename)

        test.writer.concat_year_files(drop_arguments["var"], year)
        outfile = test.writer.get_filename(drop_arguments["var"], year)

        assert os.path.exists(outfile)
        shutil.rmtree(os.path.join(drop_arguments["outdir"]))

    @pytest.mark.parametrize(
        "output_format,compact_method",
        [
            ("netcdf", "cdo"),
            ("netcdf", "xarray"),
            ("zarr", "xarray"),
        ],
    )
    def test_concat_var_year_methods(self, drop_arguments, tmp_path, output_format, compact_method):
        """Test concatenation of monthly files using CDO/xarray for NetCDF and xarray for Zarr."""
        resolution = "r100"
        frequency = "monthly"
        year = 2022

        test = Drop(
            catalog="ci",
            **drop_arguments,
            tmpdir=str(tmp_path),
            compact=compact_method,
            resolution=resolution,
            frequency=frequency,
            output_format=output_format,
            loglevel=LOGLEVEL,
        )

        # Create monthly files in the appropriate format
        for month in range(1, 13):
            mm = f"{month:02d}"
            filename = test.writer.get_filename(drop_arguments["var"], year, month=mm)
            timeobj = pd.Timestamp(f"{year}-{mm}-01")
            ds = xr.Dataset({drop_arguments["var"]: xr.DataArray([0], dims=["time"], coords={"time": [timeobj]})})

            if output_format == "netcdf":
                ds.to_netcdf(filename)
            else:  # zarr
                ds.to_zarr(filename, mode="w")

        # Test concatenation with the specified method
        test.writer.concat_year_files(drop_arguments["var"], year)
        outfile = test.writer.get_filename(drop_arguments["var"], year)

        assert os.path.exists(outfile), f"Yearly {output_format} file not created with {compact_method} method"
        shutil.rmtree(os.path.join(drop_arguments["outdir"]))

    def test_unknown_statistic(self, drop_arguments, tmp_path):
        """Test DROP with an unknown statistic."""
        error = f"Please specify a valid statistic: {available_stats}."
        with pytest.raises(ValueError, match=re.escape(error)):
            Drop(
                catalog="ci",
                **drop_arguments,
                tmpdir=str(tmp_path),
                resolution="r100",
                frequency="monthly",
                stat="unknown_stat",
                loglevel=LOGLEVEL,
            )

    def test_wrong_stat_kwargs(self, drop_arguments, tmp_path):
        """Test DROP with histogram stat but wrong stat_kwargs."""
        error = "stat_kwargs must be a dictionary."
        with pytest.raises(TypeError, match=re.escape(error)):
            test = Drop(
                catalog="ci",
                **drop_arguments,
                tmpdir=str(tmp_path),
                frequency="monthly",
                stat="histogram",
                stat_kwargs=512,  # This should be a dict, not an int
                loglevel=LOGLEVEL,
            )
            test.retrieve()
            test.drop_generator()

    def test_zarr_output(self, drop_arguments, tmp_path):
        """Test DROP with Zarr output format."""
        test = Drop(
            catalog="ci",
            **drop_arguments,
            tmpdir=str(tmp_path),
            resolution="r100",
            frequency="monthly",
            output_format="zarr",
            definitive=True,
            loglevel=LOGLEVEL,
        )

        test.retrieve()
        test.data = test.data.sel(time=slice("2020-01", "2020-03"))
        test.drop_generator()

        # With only 3 months, concatenation should NOT happen (requires 12 months)
        # Monthly stores should exist (check February as example)
        zarr_filename = test.outbuilder.build_filename(var="2t", year=2020, month="02")
        zarr_filename = os.path.splitext(zarr_filename)[0] + ".zarr"  # Replace .nc with .zarr
        zarr_store = os.path.join(
            os.getcwd(),
            drop_arguments["outdir"],
            DROP_PATH,
            zarr_filename,
        )
        assert os.path.isdir(zarr_store), f"Monthly Zarr store not found: {zarr_store}"

        # Validate zarr content (monthly stores don't have consolidated metadata)
        ds = xr.open_zarr(zarr_store, consolidated=False)
        assert len(ds.time) == 1  # Only February data
        assert "2t" in ds.data_vars
        # Use .values to handle dask arrays
        assert pytest.approx(float(ds["2t"][0, 1, 1].values)) == 240.32689

        shutil.rmtree(os.path.join(drop_arguments["outdir"]))

    def test_zarr_consolidate(self, drop_arguments, tmp_path):
        """Test DROP with Zarr output and metadata consolidation (always enabled)."""
        test = Drop(
            catalog="ci",
            **drop_arguments,
            tmpdir=str(tmp_path),
            resolution="r100",
            frequency="monthly",
            output_format="zarr",
            definitive=True,
            loglevel=LOGLEVEL,
        )

        test.retrieve()
        test.data = test.data.sel(time=slice("2020-01", "2020-02"))
        test.drop_generator()

        # With only 2 months, concatenation should NOT happen (requires 12 months)
        # Monthly stores should exist (check January as example)
        zarr_filename = test.outbuilder.build_filename(var="2t", year=2020, month="01")
        zarr_filename = os.path.splitext(zarr_filename)[0] + ".zarr"  # Replace .nc with .zarr
        zarr_store = os.path.join(
            os.getcwd(),
            drop_arguments["outdir"],
            DROP_PATH,
            zarr_filename,
        )
        assert os.path.isdir(zarr_store), f"Monthly Zarr store not found: {zarr_store}"

        # Monthly stores do not have consolidated metadata
        # Only yearly stores (created by concatenation) have it
        ds = xr.open_zarr(zarr_store, consolidated=False)
        assert len(ds.time) == 1  # Only January data

        # Verify that the store can be read successfully
        assert "2t" in ds.data_vars

        shutil.rmtree(os.path.join(drop_arguments["outdir"]))
