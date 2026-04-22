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
from aqua.core.lock import SafeFileLock
from aqua.core.reader.reader import Reader
from aqua.core.util import dump_yaml, load_yaml

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

    @pytest.mark.parametrize("output_format", ["netcdf", "zarr"])
    def test_catalog_exclude_incomplete(self, drop_arguments, tmp_path, output_format):
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
            overwrite=True,
            output_format=output_format,
        )

        test.retrieve()
        test.drop_generator()
        ext = test.writer.get_extension()

        missing_file = os.path.join(
            os.getcwd(),
            drop_arguments["outdir"],
            DROP_PATH,
            f"2t_ci_IFS_test-tco79_r1_r100_monthly_mean_global_202008{ext}",
        )
        existing_file = os.path.join(
            os.getcwd(),
            drop_arguments["outdir"],
            DROP_PATH,
            f"2t_ci_IFS_test-tco79_r1_r100_monthly_mean_global_202002{ext}",
        )

        assert not os.path.exists(missing_file), f"Incomplete file should be excluded: {missing_file}"
        assert os.path.exists(existing_file), f"Complete file should exist: {existing_file}"

        # Create catalog entry
        test.create_catalog_entry()

        # Determine source name based on format
        if output_format == "netcdf":
            source_name = "lra-r100-monthly"
        else:  # zarr
            source_name = "lra-r100-monthly-zarr"

        # Use Reader to access the newly created source through intake
        reader = Reader(
            catalog="ci",
            model=drop_arguments["model"],
            exp=drop_arguments["exp"],
            source=source_name,
            loglevel=LOGLEVEL,
        )

        # Read data through intake and verify it matches
        data = reader.retrieve(var=drop_arguments["var"])
        assert "2t" in data.data_vars, f"Variable '2t' not found in {output_format} intake data"
        assert len(data.time) == 6, f"{output_format} Reader data should have 6 timesteps instead of {len(data.time)}"

        catalogfile = os.path.join(test.configdir, "catalogs", test.catalog, "catalog", test.model, test.exp + ".yaml")
        with SafeFileLock(catalogfile + ".lock"):
            cat_file = load_yaml(catalogfile)
            del cat_file["sources"][source_name]
            dump_yaml(outfile=catalogfile, cfg=cat_file)

        # file = xr.open_dataset(existing_file)
        # assert len(file.time) == 1
        # test.check_integrity(varname=drop_arguments["var"])
        shutil.rmtree(os.path.join(drop_arguments["outdir"]))

    @pytest.mark.parametrize(
        "output_format,compact_method,num_months,should_concat",
        [
            ("netcdf", "cdo", 3, False),  # <12 months: monthly files remain
            ("netcdf", "cdo", 12, True),  # 12 months: yearly file created
            ("netcdf", "xarray", 3, False),  # <12 months: monthly files remain
            ("netcdf", "xarray", 12, True),  # 12 months: yearly file created
            ("zarr", "xarray", 3, False),  # <12 months: monthly stores remain
            ("zarr", "xarray", 12, True),  # 12 months: yearly store created
        ],
    )
    def test_concat_threshold(self, drop_arguments, tmp_path, output_format, compact_method, num_months, should_concat):
        """Test concatenation threshold: requires exactly 12 months for both NetCDF and Zarr."""
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

        # Create monthly files/stores in the appropriate format
        for month in range(1, num_months + 1):
            mm = f"{month:02d}"
            filename = test.writer.get_filename(drop_arguments["var"], year, month=mm)
            timeobj = pd.Timestamp(f"{year}-{mm}-01")
            ds = xr.Dataset({drop_arguments["var"]: xr.DataArray([month], dims=["time"], coords={"time": [timeobj]})})

            if output_format == "netcdf":
                ds.to_netcdf(filename)
            else:  # zarr
                ds.to_zarr(filename, mode="w")

        # Attempt concatenation
        result = test.writer.concat_year_files(drop_arguments["var"], year)
        yearly_file = test.writer.get_filename(drop_arguments["var"], year)

        if should_concat:
            # With 12 months: yearly file/store should be created, monthly ones removed
            assert result is True, f"Concatenation should succeed with {num_months} months"
            assert os.path.exists(yearly_file), f"Yearly {output_format} file/store not found"

            # Verify monthly files/stores are removed
            for month in range(1, 13):
                mm = f"{month:02d}"
                monthly_file = test.writer.get_filename(drop_arguments["var"], year, month=mm)
                assert not os.path.exists(monthly_file), f"Monthly file/store {monthly_file} should be removed"

            # Verify yearly file/store content
            if output_format == "netcdf":
                ds_yearly = xr.open_dataset(yearly_file)
            else:  # zarr
                ds_yearly = xr.open_zarr(yearly_file, consolidated=True)

            assert len(ds_yearly.time) == 12, "Yearly file should have 12 timesteps"
            ds_yearly.close()
        else:
            # With <12 months: concatenation should not happen, monthly files/stores remain
            assert result is False, f"Concatenation should not happen with {num_months} months"
            assert not os.path.exists(yearly_file), f"Yearly {output_format} file/store should not exist"

            # Verify monthly files/stores still exist
            for month in range(1, num_months + 1):
                mm = f"{month:02d}"
                monthly_file = test.writer.get_filename(drop_arguments["var"], year, month=mm)
                assert os.path.exists(monthly_file), f"Monthly file/store {monthly_file} should exist"

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

    def test_zarr_writer_validation(self, drop_arguments, tmp_path):
        """Test ZarrWriter validation edge cases."""
        from aqua.core.drop.drop_writer_zarr import ZarrWriter

        writer = ZarrWriter(tmpdir=str(tmp_path), outdir=drop_arguments["outdir"], loglevel=LOGLEVEL)

        # Case 1: Store without 'time' dimension
        ds_no_time = xr.Dataset({"var": xr.DataArray([1, 2, 3], dims=["x"])})
        invalid_store = os.path.join(str(tmp_path), "no_time.zarr")
        ds_no_time.to_zarr(invalid_store, mode="w")
        assert writer.validate(invalid_store) is False

        # Case 2: Store with empty time
        ds_empty = xr.Dataset({"var": xr.DataArray([], dims=["time"], coords={"time": []})})
        empty_store = os.path.join(str(tmp_path), "empty_time.zarr")
        ds_empty.to_zarr(empty_store, mode="w")
        assert writer.validate(empty_store) is False

        # Case 3: Store with duplicate timestamps
        times = [pd.Timestamp("2020-01-01"), pd.Timestamp("2020-01-01")]
        ds_dup = xr.Dataset({"var": xr.DataArray([1, 2], dims=["time"], coords={"time": times})})
        dup_store = os.path.join(str(tmp_path), "duplicate.zarr")
        ds_dup.to_zarr(dup_store, mode="w")
        assert writer.validate(dup_store) is False

        # Case 4: Store with unsorted timestamps
        times_unsorted = [pd.Timestamp("2020-02-01"), pd.Timestamp("2020-01-01")]
        ds_unsorted = xr.Dataset({"var": xr.DataArray([1, 2], dims=["time"], coords={"time": times_unsorted})})
        unsorted_store = os.path.join(str(tmp_path), "unsorted.zarr")
        ds_unsorted.to_zarr(unsorted_store, mode="w")
        assert writer.validate(unsorted_store) is False

        # Case 5: Valid store
        times_valid = [pd.Timestamp("2020-01-01"), pd.Timestamp("2020-02-01")]
        ds_valid = xr.Dataset({"var": xr.DataArray([1, 2], dims=["time"], coords={"time": times_valid})})
        valid_store = os.path.join(str(tmp_path), "valid.zarr")
        ds_valid.to_zarr(valid_store, mode="w")
        assert writer.validate(valid_store) is True

    def test_zarr_encoding_without_chunks(self, drop_arguments, tmp_path):
        """Test ZarrWriter encoding when chunks is None."""
        from aqua.core.drop.drop_writer_zarr import ZarrWriter

        # Create writer with chunks=None
        writer = ZarrWriter(tmpdir=str(tmp_path), outdir=drop_arguments["outdir"], loglevel=LOGLEVEL)
        writer.chunks = None

        ds = xr.Dataset(
            {"var": xr.DataArray([1, 2, 3], dims=["time"], coords={"time": pd.date_range("2020-01-01", periods=3)})}
        )
        encoding = writer._get_encoding(ds)

        # Should return None when chunks is None
        assert encoding is None

    def test_write_without_dask_client(self, drop_arguments, tmp_path):
        """Test write_variable without dask_client (single process)."""
        test = Drop(
            catalog="ci",
            **drop_arguments,
            tmpdir=str(tmp_path),
            resolution="r100",
            frequency="monthly",
            nproc=1,  # Single process, no dask cluster
            definitive=True,
            loglevel=LOGLEVEL,
        )

        test.retrieve()
        test.data = test.data.sel(time="2020-01")
        test.drop_generator()  # Should work without dask_client

        file_path = os.path.join(
            os.getcwd(),
            drop_arguments["outdir"],
            DROP_PATH,
            "2t_ci_IFS_test-tco79_r1_r100_monthly_mean_global_202001.nc",
        )
        assert os.path.isfile(file_path)
        shutil.rmtree(os.path.join(drop_arguments["outdir"]))
