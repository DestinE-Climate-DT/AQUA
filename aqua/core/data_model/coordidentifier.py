"""
Module to identify the nature of coordinates of an Xarray object.
"""

import xarray as xr
import numpy as np
from aqua.core.logger import log_configure
from .coord_utils import get_coord_defaults, is_pressure, is_meter


class CoordIdentifier:
    """
    Class to identify the nature of coordinates of an Xarray object.
    It aims at detecting the longitude, latitude, time and any other vertical
    by inspecting the attributes of the coordinates provided by the user.
    "internal names" are used to refer to the coordinates in the data model.
    "coordinate names" refer to the actual names of the coordinates in the Xarray object.

    Args:
        coords (xarray.Coordinates): The coordinates of Dataset to be analysed.
        loglevel (str): The log level to use. Default is 'WARNING'.
    """

    # Scoring weights for coordinate identification
    SCORE_WEIGHTS = {
        "name": 100,
        "standard_name": 100,
        "axis": 50,
        "units": 50,
        "long_name": 50,
    }

    def __init__(self, coords: xr.Coordinates, loglevel="WARNING"):
        """
        Constructor of the CoordIdentifier class.
        """
        self.loglevel = loglevel
        self.logger = log_configure(self.loglevel, "CoordIdentifier")

        if not isinstance(coords, xr.Coordinates):
            raise TypeError("coords must be an Xarray Coordinates object.")
        self.coords = coords

        # internal name definition for the coordinates
        self.coord_dict = {
            "latitude": [],
            "longitude": [],
            "time": [],
            "isobaric": [],
            "depth": [],
            "height": [],
        }

        # Score methods for each internal coordinate
        self.score_methods = {
            "latitude": self._score_latitude,
            "longitude": self._score_longitude,
            "isobaric": self._score_isobaric,
            "depth": self._score_depth,
            "time": self._score_time,
            "height": self._score_height,
        }

        # default coordinate values from config
        self.default_coords = {
            "latitude": get_coord_defaults("latitude"),
            "longitude": get_coord_defaults("longitude"),
            "time": get_coord_defaults("time"),
            "isobaric": get_coord_defaults("isobaric"),
            "depth": get_coord_defaults("depth"),
            "height": get_coord_defaults("height"),
        }

    def identify_coords(self):
        """
        Identify the coordinates of the Xarray object using a point-based scoring system.
        For each coordinate, scores are calculated against all internal coordinate types.
        The coordinate name is assigned to the internal name with the highest score.
        """

        # Evaluate scores for all coordinates
        scores = self._evaluate_scores()

        # Fill the coordinate dictionary based on scores
        self._fill_coord_dict(scores)

        # Clean and rank the coordinate dictionary
        self._rank_coord_dict()

        # Deduplicate coordinates with multiple assignments
        self._deduplicate_coords()

        # Log the identified coordinates
        self._log_coord_matches()

        return self.coord_dict

    def _evaluate_scores(self):
        """
        Evaluate scores for all coordinates names against all internal coordinate types.
        Returns:
            dict: A nested dictionary with scores for each internal coordinate.
        """
        scores = {}
        for coord_name, coord in self.coords.items():
            self.logger.debug("Identifying coordinate: %s", coord_name)

            # Score this coordinate against all types
            scores[coord_name] = {}
            for coord_type, score_func in self.score_methods.items():
                score, matched_attrs = score_func(coord)
                scores[coord_name][coord_type] = (score, matched_attrs)
                if score > 0:
                    self.logger.debug(
                        "Score for %s as %s: %d (matched: %s)",
                        coord_name,
                        coord_type,
                        score,
                        matched_attrs,
                    )
        return scores

    def _fill_coord_dict(self, scores):
        """
        Fill the coordinate dictionary with identified coordinates and their attributes.
        Args:
            scores (dict): The scores for each coordinate and type.
        """
        for coord_name, coord in self.coords.items():
            for coord_type, (score, matched_attrs) in scores[coord_name].items():
                if score > 0:  # Only consider coordinates with positive scores
                    if coord_type == "time":
                        coord_info = self._get_time_attributes(
                            coord, score, matched_attrs
                        )
                    else:
                        coord_info = self._get_attributes(
                            coord,
                            coord_name=coord_type,
                            confidence_score=score,
                            matched_attributes=matched_attrs,
                        )
                    self.coord_dict[coord_type].append(coord_info)

    def _deduplicate_coords(self):
        """
        Deduplicate coordinates: coordinate name can be assigned only once irrespective of the
        internal coordinate type. If multiple assignments exist, the one with the highest score is kept.

        e.g. "lat" coordinate assigned to both "latitude" and "depth" - keep the one with highest score.
        and set the other to None. Note that this is different from ranking within the same internal coordinate type.
        """
        name_groups = {}
        for key, value in self.coord_dict.items():
            if isinstance(value, dict) and value.get("name") is not None:                
                name = value["name"]
                if name not in name_groups:
                    name_groups[name] = []
                element = value.get("confidence_score", 0.0)
                name_groups[name].append((key, element))

        # Second pass: resolve conflicts by confidence_score
        for name, entries in name_groups.items():
            if len(entries) > 1:
                # Sort by confidence_score (highest first)
                entries.sort(key=lambda x: x[1], reverse=True)
                
                # Check if multiple entries have the same highest score
                max_score_entries = [entry for entry in entries if entry[1] == entries[0][1]]
                
                if len(max_score_entries) > 1:
                    # Multiple coordinates with same highest score - disable all
                    self.logger.warning(
                        "Coordinate '%s' assigned to multiple types with identical scores: %s. Disabling data model check for this coordinate.",
                        name,
                        [(key, score) for key, score in max_score_entries],
                    )
                    for key, _ in entries:
                        self.coord_dict[key] = None
                else:
                    # Clear winner - keep the best, remove the rest
                    self.logger.info(
                        "Coordinate '%s' assigned to multiple types: %s. Selecting '%s' with highest score %s.",
                        name,
                        [key for key, _ in entries],
                        entries[0][0],
                        entries[0][1],
                    )
                    for key, _ in entries[1:]:
                        self.coord_dict[key] = None

        return self.coord_dict

    def _rank_coord_dict(self):
        """
        Rank the coordinate dictionary if two coordinates names are found for the same internal coordinate.
        Set to None the coordinates that are empty.
        If multiple coordinates are found for the same internal coordinate:
        - If scores differ, keep the one with highest score
        - If scores are identical, disable (set to None) and log warning

        e.g. both "lat" and "latitude" identified as "latitude" - keep the one with highest score.
        """
        for key, value in self.coord_dict.items():
            # If no matches found, set to None
            if len(value) == 0:
                self.coord_dict[key] = None
            # if one match found, keep it
            elif len(value) == 1:
                self.coord_dict[key] = value[0]
            # if multiple matches found, compare scores
            else:
                # Multiple matches found - compare scores
                scores = [x.get("confidence_score", 0) for x in value]
                max_score = max(scores)
                max_score_indices = [i for i, s in enumerate(scores) if s == max_score]

                # If only one has the highest score, keep it
                if len(max_score_indices) == 1:
                    best_idx = max_score_indices[0]
                    self.logger.debug(
                        "Multiple %s coordinates found: %s. Selecting '%s' with highest score %s.",
                        key,
                        [x["name"] for x in value],
                        value[best_idx]["name"],
                        max_score,
                    )
                    self.coord_dict[key] = value[best_idx]
                # Multiple coordinates with same highest score - disable
                else:
                    self.logger.warning(
                        "Multiple %s coordinates found with identical scores: %s. Disabling data model check for this coordinate.",
                        key,
                        [(x["name"], x.get("confidence_score", "N/A")) for x in value],
                    )
                    self.coord_dict[key] = None

    def _log_coord_matches(self):
        """
        Print log messages for identified and unidentified coordinates.
        """
        identified = [
            key for key, value in self.coord_dict.items() if value is not None
        ]
        unidentified = [key for key, value in self.coord_dict.items() if value is None]

        if identified:
            self.logger.debug("Successfully identified coordinates: %s", identified)
        if unidentified:
            self.logger.debug("Coordinates not identified: %s", unidentified)

    def _get_time_attributes(
        self, coord, confidence_score=None, matched_attributes=None
    ):
        """
        Get the attributes of the time coordinates.

        Args:
            coord (xarray.Coordinates): The coordinate to define the attributes.
            confidence_score (int): The confidence score from identification.
            matched_attributes (list): List of attributes that matched in scoring.

        Returns:
            dict: A dictionary containing the attributes of the coordinate.
        """
        attrs = {
            "name": coord.name,
            "dims": coord.dims,
            "units": coord.attrs.get("units"),
            "calendar": coord.attrs.get("calendar"),
            "bounds": coord.attrs.get("bounds"),
        }
        if confidence_score is not None:
            attrs["confidence_score"] = confidence_score
        if matched_attributes is not None:
            attrs["matched_attributes"] = matched_attributes
        return attrs

    def _get_attributes(
        self,
        coord,
        coord_name="longitude",
        confidence_score=None,
        matched_attributes=None,
    ):
        """
        Get the attributes of the coordinates.

        Args:
            coord (xarray.Coordinates): The coordinate to define the attributes.
            coord_name (str): The type of coordinate ("longitude", "latitude", "isobaric", "depth").
            confidence_score (int): The confidence score from identification.
            matched_attributes (list): List of attributes that matched in scoring.

        Returns:
            dict: A dictionary containing the attributes of the coordinate.
        """
        coord_range = (coord.values.min(), coord.values.max())
        direction = None
        positive = None
        horizontal = ["latitude", "longitude"]
        vertical = ["isobaric", "depth", "height"]

        if coord.ndim == 1 and coord_name in horizontal:
            direction = (
                "increasing" if coord.values[-1] > coord.values[0] else "decreasing"
            )

        # this might require an update since it is only diagnostic
        if coord_name in vertical:
            positive = coord.attrs.get("positive")
            if not positive:
                if is_pressure(coord.attrs.get("units")):
                    positive = "down"
                elif is_meter(coord.attrs.get("units")):
                    positive = "up"
                else:
                    if coord.values.size >= 2:
                        positive = "down" if coord.values[0] > 0 else "up"

        attributes = {
            "name": coord.name,
            "dims": coord.dims,
            "units": coord.attrs.get("units"),
            "range": coord_range,
            "bounds": coord.attrs.get("bounds"),
        }

        if coord_name in horizontal:
            attributes["stored_direction"] = direction
        elif coord_name in vertical:
            attributes["positive"] = positive

        if coord_name == "longitude":
            attributes["convention"] = self._guess_longitude_range(coord)

        if confidence_score is not None:
            attributes["confidence_score"] = confidence_score
        if matched_attributes is not None:
            attributes["matched_attributes"] = matched_attributes

        return attributes

    @staticmethod
    def _guess_longitude_range(longitude) -> str:
        """
        Guess if the longitude range is from 0 to 360 or from -180 to 180,
        ensuring the grid is global.
        """

        # Guess the longitude range
        if np.any(longitude.values < 0):
            return "centered"
        elif np.any(longitude.values > 180):
            return "positive"
        else:
            return "ambigous"

    def _score_latitude(self, coord):
        """
        Score a coordinate for latitude identification.

        Returns:
            tuple: (score, matched_attributes)
        """
        score = 0
        matched = []

        if coord.name in self.default_coords["latitude"]:
            score += self.SCORE_WEIGHTS["name"]
            matched.append("name")
        if coord.attrs.get("standard_name") == "latitude":
            score += self.SCORE_WEIGHTS["standard_name"]
            matched.append("standard_name")
        if coord.attrs.get("axis") == "Y":
            score += self.SCORE_WEIGHTS["axis"]
            matched.append("axis")
        if coord.attrs.get("units") == "degrees_north":
            score += self.SCORE_WEIGHTS["units"]
            matched.append("units")

        return score, matched

    def _score_longitude(self, coord):
        """
        Score a coordinate for longitude identification.

        Returns:
            tuple: (score, matched_attributes)
        """
        score = 0
        matched = []

        if coord.name in self.default_coords["longitude"]:
            score += self.SCORE_WEIGHTS["name"]
            matched.append("name")
        if coord.attrs.get("standard_name") == "longitude":
            score += self.SCORE_WEIGHTS["standard_name"]
            matched.append("standard_name")
        if coord.attrs.get("axis") == "X":
            score += self.SCORE_WEIGHTS["axis"]
            matched.append("axis")
        if coord.attrs.get("units") == "degrees_east":
            score += self.SCORE_WEIGHTS["units"]
            matched.append("units")

        return score, matched

    def _score_time(self, coord):
        """
        Score a coordinate for time identification.

        Returns:
            tuple: (score, matched_attributes)
        """
        score = 0
        matched = []

        if coord.name in self.default_coords["time"]:
            score += self.SCORE_WEIGHTS["name"]
            matched.append("name")
        if coord.attrs.get("axis") == "T":
            score += self.SCORE_WEIGHTS["axis"]
            matched.append("axis")
        if coord.attrs.get("standard_name") == "time":
            score += self.SCORE_WEIGHTS["standard_name"]
            matched.append("standard_name")

        return score, matched

    def _score_isobaric(self, coord):
        """
        Score a coordinate for isobaric (pressure) identification.
        Handles special case: checks hardcoded name, standard_name, and MetPy unit analysis.

        Returns:
            tuple: (score, matched_attributes)
        """
        score = 0
        matched = []

        if coord.name in self.default_coords["isobaric"]:
            score += self.SCORE_WEIGHTS["name"]
            matched.append("name")
        if coord.attrs.get("standard_name") == "air_pressure":
            score += self.SCORE_WEIGHTS["standard_name"]
            matched.append("standard_name")
        if is_pressure(coord.attrs.get("units")):
            score += self.SCORE_WEIGHTS["units"]
            matched.append("units")

        return score, matched

    def _score_depth(self, coord):
        """
        Score a coordinate for depth identification.
        Handles special case: checks hardcoded name, standard_name, substring in name/long_name.

        Returns:
            tuple: (score, matched_attributes)
        """
        score = 0
        matched = []

        if coord.name in self.default_coords["depth"]:
            score += self.SCORE_WEIGHTS["name"]
            matched.append("name")
        if coord.attrs.get("standard_name") == "depth":
            score += self.SCORE_WEIGHTS["standard_name"]
            matched.append("standard_name")
        if coord.attrs.get("axis") == "Z":
            score += self.SCORE_WEIGHTS["axis"]  # Use axis weight for substring match
            matched.append("axis")
        if is_meter(coord.attrs.get("units")):
            score += self.SCORE_WEIGHTS["units"]
            matched.append("units")
        if "depth" in coord.attrs.get("long_name", ""):
            score += self.SCORE_WEIGHTS["long_name"]
            matched.append("long_name_contains_depth")

        return score, matched

    def _score_height(self, coord):
        """
        Score a coordinate for height identification.
        Handles special case: checks hardcoded name, standard_name, substring in name/long_name.
        Returns:
            tuple: (score, matched_attributes)
        """
        score = 0
        matched = []

        if coord.name in self.default_coords["height"]:
            score += self.SCORE_WEIGHTS["name"]
            matched.append("name")
        if coord.attrs.get("standard_name") in ["height_above_ground", "height"]:
            score += self.SCORE_WEIGHTS["standard_name"]
            matched.append("standard_name")
        if coord.attrs.get("axis") == "Z":
            score += self.SCORE_WEIGHTS["axis"]  # Use axis weight for substring match
            matched.append("axis")
        if is_meter(coord.attrs.get("units")):
            score += self.SCORE_WEIGHTS["units"]
            matched.append("units")
        if "height" in coord.attrs.get("long_name", ""):
            score += self.SCORE_WEIGHTS["long_name"]
            matched.append("long_name_contains_height")

        return score, matched
