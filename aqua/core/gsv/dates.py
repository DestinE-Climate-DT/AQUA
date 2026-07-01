"""Date-resolution helpers for FDB/GSV sources (DestinE GSV schema specific).

This mixin groups every way the available data window can be discovered for a
DestinE GSV experiment: from an explicit catalog entry, from an ``fdb_info`` file,
from the bridge STAC API, or by auto-scanning the FDB root. It is kept separate from
the generic partitioning machinery (:mod:`aqua.core.gsv.partitioned`) because these
strategies are tied to the DestinE deployment (STAC endpoint, FDB config layout,
filename conventions) and would differ for another backend.

The mixin expects the host class to provide: ``self.logger``, ``self.engine``,
``self.dummy_run``, ``self.timestyle``, ``self.fdb_info_file``, ``self.fdbhome``,
``self.fdbpath``, ``self.hpc_expver``, ``self._request`` and the
``self.data_start_date``/``self.data_end_date``/``self.bridge_start_date``/
``self.bridge_end_date``/``self.startdate``/``self.enddate`` attributes.
"""

import fnmatch
import os

import requests
from ruamel.yaml import YAML

from aqua.core.util import to_list

BRIDGE_API_URL = "https://qubed.lumi.apps.dte.destination-earth.eu/api/v2/stac"  # LUMI QUBED STAC API


class FDBDatesMixin:
    """Resolve data/bridge availability dates for a DestinE GSV source."""

    def _define_start_end_dates(self, data_start_date, data_end_date, bridge_start_date, bridge_end_date):
        """
        Define the start and end dates of the data and bridge

        Args:
            data_start_date (str): Start date of the available data.
            data_end_date (str): End date of the available data.
            bridge_start_date (str): Start date of the bridge data.
            bridge_end_date (str): End date of the bridge data.
        """
        # Getting info from the FDB info file
        if self.engine == "fdb" and not self.dummy_run:
            fdb_info = self._read_fdb_info()
        else:
            fdb_info = None

        # Data dates
        self._setup_data_dates(data_start_date, data_end_date, fdb_info)

        # Bridge dates
        self._setup_bridge_dates(bridge_start_date, bridge_end_date, fdb_info)
        self._adjust_bridge_bounds()

    def _read_fdb_info(self):
        """
        Read FDB information from file if available

        Returns:
            dict or None: FDB information if available, None otherwise
        """
        if self.fdb_info_file:
            self.logger.debug("Reading FDB info from file %s", self.fdb_info_file)
            return self.get_fdb_definitions_from_file(self.fdb_info_file)
        return None

    def _setup_data_dates(self, data_start_date, data_end_date, fdb_info):
        """
        Setup data start and end dates

        Args:
            data_start_date (str): Start date of the available data.
            data_end_date (str): End date of the available data.
            fdb_info (dict or None): FDB information if available
        """
        if self.fdb_info_file and fdb_info:
            self.data_start_date = fdb_info["data"]["data_start_date"]
            self.data_end_date = fdb_info["data"]["data_end_date"]
            self.hpc_expver = fdb_info["data"]["expver"]
        else:
            if data_start_date == "auto" or data_end_date == "auto":
                self.logger.debug("Autoguessing of the FDB start and end date enabled.")
                if self.timestyle == "yearmonth":
                    raise ValueError(
                        "Auto date selection not supported for timestyle=yearmonth. Please specify start and end date!"
                    )
                self.data_start_date, self.data_end_date = self.parse_fdb(data_start_date, data_end_date)
            else:
                self.data_start_date = data_start_date
                self.data_end_date = data_end_date

    def _setup_bridge_dates(self, bridge_start_date, bridge_end_date, fdb_info):
        """
        Setup bridge start and end dates

        Args:
            bridge_start_date (str): Start date of the bridge data.
            bridge_end_date (str): End date of the bridge data.
            fdb_info (dict or None): FDB information if available
        """
        if self.fdb_info_file and fdb_info and fdb_info.get("bridge"):
            self._setup_bridge_dates_from_file(fdb_info)
        else:
            if bridge_start_date == "stac" or bridge_end_date == "stac":
                self._setup_bridge_dates_from_stac()
            else:
                self._setup_bridge_dates_from_input(bridge_start_date, bridge_end_date)

    def _setup_bridge_dates_from_file(self, fdb_info):
        """
        Setup bridge dates from FDB info file

        Args:
            fdb_info (dict): FDB information from file
        """
        self.bridge_start_date = fdb_info["bridge"]["bridge_start_date"]
        self.bridge_end_date = fdb_info["bridge"]["bridge_end_date"]
        self._request["expver"] = fdb_info["bridge"]["expver"]

    def _setup_bridge_dates_from_stac(self):
        """
        Setup bridge dates from STAC API
        """
        self.logger.debug("Reading FDB info from bridge STAC API")
        self.bridge_start_date, self.bridge_end_date = self.get_dates_from_stac_api(self._request, BRIDGE_API_URL)
        self.bridge_end_date = self.bridge_end_date + "T2300"
        self.bridge_start_date = self.bridge_start_date + "T0000"
        self.logger.debug("STAC API bridge start data: %s, bridge end date: %s", self.bridge_start_date, self.bridge_end_date)

    def _setup_bridge_dates_from_input(self, bridge_start_date, bridge_end_date):
        """
        Setup bridge dates from input parameters

        Args:
            bridge_start_date (str): Start date of the bridge data.
            bridge_end_date (str): End date of the bridge data.
        """
        # deprecated method that guess from text file and fall back
        self.bridge_start_date = self._read_bridge_date(bridge_start_date)
        self.bridge_end_date = self._read_bridge_date(bridge_end_date)

    def _adjust_bridge_bounds(self):
        """
        Adjust bridge bounds if not specified or set to 'complete'
        """
        if self.bridge_start_date == "complete" or self.bridge_end_date == "complete":
            self.bridge_start_date = self.data_start_date
            self.bridge_end_date = self.data_end_date
        if not self.bridge_start_date and self.bridge_end_date:
            self.bridge_start_date = self.data_start_date
        if not self.bridge_end_date and self.bridge_start_date:
            self.bridge_end_date = self.data_end_date

    def _define_retrieve_dates(self, startdate, enddate):
        """
        Define the start and end dates for the retrieval
        """

        if not startdate:
            self.startdate = self.data_start_date
        else:
            self.startdate = startdate

        if not enddate:
            self.enddate = self.data_end_date
        else:
            self.enddate = enddate

    def get_fdb_definitions_from_file(self, fdb_info_file):
        """
        Get the FDB definitions from a file

        Args:
            file (str): path to the file

        Returns:
            dict: definitions
        """
        if not os.path.exists(fdb_info_file):
            self.logger.error("FDB info file %s does not exist", fdb_info_file)
            return None

        yaml = YAML()

        try:
            with open(fdb_info_file, "r") as file:
                fdb_info = yaml.load(file)
        except (OSError, yaml.YAMLError) as e:
            self.logger.error("Error reading or parsing YAML file %s: %s", fdb_info_file, str(e))
            return None

        # The 'data' block is mandatory and present since the first chunck of simulation
        # The 'bridge' block is written only if some data is on bridge (see #1760)
        if "data" in fdb_info:
            try:
                fdb_info["data"]["data_start_date"] = self._validate_info_date(fdb_info, "data", "start")
                fdb_info["data"]["data_end_date"] = self._validate_info_date(fdb_info, "data", "end")
            except KeyError:
                self.logger.error("FDB info file %s does not contain HPC dates in correct format", fdb_info_file)
                return None
        else:
            self.logger.error("FDB info file %s does not contain 'data' section, which is mandatory", fdb_info_file)
            return None
        if "bridge" in fdb_info:
            try:
                fdb_info["bridge"]["bridge_start_date"] = self._validate_info_date(fdb_info, "bridge", "start")
                fdb_info["bridge"]["bridge_end_date"] = self._validate_info_date(fdb_info, "bridge", "end")
            # if bridge dates are wrongly defined, set the bridge block to None
            except KeyError:
                self.logger.error("FDB info file %s does not contain bridge dates in correct form", fdb_info_file)
                fdb_info["bridge"] = None
        else:
            fdb_info["bridge"] = None

        return fdb_info

    def _validate_info_date(self, fdb_info_file, location="data", kind="start"):

        if location not in ["data", "bridge"]:
            raise ValueError(f"location {location} should be either data or local")

        if kind not in ["start", "end"]:
            raise ValueError(f"kind {kind} should be either start or end")

        return self._todatetime(fdb_info_file[location][f"{location}_{kind}_date"]).strftime("%Y%m%dT%H%M")

    def parse_fdb(self, start_date, end_date):
        """
        Parse the FDB config file and return the start and end dates of the data.
        This works only with the DE GSV schema.

        Args:
            start_date (str): if 'auto' the start date is found automatically. Else it is the start date.
            end_date (str): if 'auto' the end date is found automatically. Else it is the end date.

        Returns:
            tuple: start and end dates
        """
        if not self.fdbhome and not self.fdbpath:
            raise ValueError("Automatic dates requested but no FDB home or FDB path specified in catalog.")

        yaml = YAML()

        if self.fdbhome:  # FDB_HOME takes precedence but assumes a fixed subdirectory structure
            yamlfile = os.path.join(self.fdbhome, "etc/fdb/config.yaml")
        else:
            yamlfile = self.fdbpath

        with open(yamlfile, "r") as file:
            cfg = yaml.load(file)

        if "fdbs" in cfg:
            root = cfg["fdbs"][0]["spaces"][0]["roots"][0]["path"]
        else:
            root = cfg["spaces"][0]["roots"][0]["path"]

        req = self._request
        expver = req["expver"]
        if self.hpc_expver:
            expver = self.hpc_expver

        file_mask = (
            f"{req['class']}:{req['dataset']}:{req['activity']}:"
            f"{req['experiment']}:{req['generation']}:{req['model']}:"
            f"{req['realization']}:{expver}:{req['stream']}:*"
        )

        file_mask = file_mask.lower()
        file_list = [f for f in os.listdir(root) if fnmatch.fnmatch(f.lower(), file_mask)]

        datesel = [filename[-8:] for filename in file_list if (filename[-8:].isdigit() and len(filename[-8:]) == 8)]
        datesel.sort()

        if len(datesel) == 0:
            raise ValueError("Auto date selection in catalog but no valid dates found in FDB")
        else:
            if start_date == "auto":
                start_date = datesel[0] + "T0000"
            if end_date == "auto":
                end_date = datesel[-2] + "T2300"
            self.logger.info("Automatic FDB date range: %s - %s", start_date, end_date)

        return start_date, end_date

    @staticmethod
    def get_dates_from_stac_api(params, base_url=BRIDGE_API_URL):
        """
        Function to get from the STAC data bridge the available
        dates of a dataset on the bridge

        Args:
            params (dict): Dictionary of parameters to interrogate the STAC API.
                        In principle, the same as the usual FDB request
            base_url (str): URL for the STAC API

        Returns:
            tuple: A tuple containing the start and end dates of the dataset
        """

        # Define the base URL for the STAC API
        params["root"] = "root"
        params["param"] = to_list(params["param"])[0]
        for p in ["date", "time", "step", "year", "month"]:
            params.pop(p, None)  # remove date/time/step/year/month if present

        # new stac API requires lowercased keys
        params = {k: v.lower() if isinstance(v, str) else v for k, v in params.items()}

        # network problems can happen, so we need to handle them
        try:
            response = requests.get(base_url, params=params, timeout=10)
        except requests.Timeout as e:
            raise TimeoutError("STAC API request timed out after 10 seconds.") from e
        except requests.RequestException as e:
            raise ConnectionError("STAC API request failed") from e

        # Check the response status code
        if response.status_code == 400:
            raise ValueError(f"Bad request to STAC API: {response.text}")
        if response.status_code == 503:
            raise ValueError(f"Service unavailable: {response.text}")
        if response.status_code != 200:
            raise ValueError(f"Unexpected response from STAC API: {response.status_code} - {response.text}")

        # parse the JSON response
        try:
            stac_json = response.json()
        except ValueError as exc:
            raise ValueError("Failed to parse STAC API response as JSON") from exc

        dateblock = [el for el in stac_json["links"] if el.get("title") == "date"]
        if not dateblock:
            raise ValueError(f"The first link in the response is not a date link, but {dateblock}")

        # specific extraction of the dates: new format following the qube STAC API
        dates = dateblock[0].get("variables").get("date").get("enum")
        sorted_dates = sorted(dates)

        return sorted_dates[0], sorted_dates[-1]
