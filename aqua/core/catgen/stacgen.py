#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
STAC-based AQUA Catalog Generator.

Discovers metadata from a STAC API endpoint (e.g., LUMI QUBED) via a
hierarchical facet tree, producing deterministic FDB-like request dicts.
"""

import os
import re
import urllib.request
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import Any, Iterator

import requests

from aqua.core.configurer import ConfigPath
from aqua.core.lock import SafeFileLock
from aqua.core.logger import log_configure
from aqua.core.util import dump_yaml, load_yaml

DEPTH_URL = "https://platform.destine.eu/docs/climate-dt-user-guide/doc/data/ocean_model_levels.html"


@dataclass
class CatalogNode:
    """Hierarchical facet tree node for STAC metadata discovery.

    Attributes:
        key: Facet name at this level, e.g. "dataset", "activity".
        value: This node's value, e.g. "climate-dt", "baseline".
        children: Child nodes keyed by child value.
    """

    key: str  # facet name at this level, e.g. "dataset", "activity"
    value: str  # this node's value, e.g. "climate-dt", "baseline"
    # keyed by child value
    children: dict[str, "CatalogNode"] = field(default_factory=dict)

    def iter_requests(
        self,
        leaves_only: bool = False,
        include_univocal: bool = False,
        _prefix: dict[str, Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Walk the tree, yielding accumulated facet dicts per node.

        Args:
            leaves_only: If True, yield only leaf nodes.
            include_univocal: If True, yield nodes with exactly one child.
            _prefix: (Internal) Accumulated request dict so far.

        Yields:
            dict: FDB-like request dicts with accumulated facet keys.
        """
        prefix = dict(_prefix or {})
        prefix[self.key] = self.value

        if not self.children:
            yield prefix
            return

        if not leaves_only:
            if include_univocal and len(self.children) == 1:
                yield prefix
            elif not include_univocal:
                yield prefix

        for child in self.children.values():
            yield from child.iter_requests(leaves_only=leaves_only, include_univocal=include_univocal, _prefix=prefix)

    def all_requests(self, leaves_only: bool = False, include_univocal: bool = False) -> list[dict[str, Any]]:
        """Convenience list wrapper around iter_requests.

        Args:
            leaves_only: If True, return only leaf nodes.
            include_univocal: If True, include nodes with exactly one child.

        Returns:
            list: FDB-like request dicts.
        """
        return list(self.iter_requests(leaves_only=leaves_only, include_univocal=include_univocal))

    def leaves(self, _prefix=None) -> Iterator[tuple["CatalogNode", dict[str, Any]]]:
        """Yield (node, accumulated_prefix) for every leaf in the subtree.

        Args:
            _prefix: (Internal) Accumulated request dict so far.

        Yields:
            tuple: (leaf_node, accumulated_request_dict).
        """
        prefix = dict(_prefix or {})
        prefix[self.key] = self.value
        if not self.children:
            yield self, prefix
        else:
            for child in self.children.values():
                yield from child.leaves(_prefix=prefix)

    def expand_level(
        self,
        key_name: str,
        query_fn,
        items_fn,
        allowed: set[Any] | None = None,
        logger=None,
    ) -> tuple["CatalogNode", int]:
        """For every current leaf, query STAC and attach children at key_name level.

        Optionally restricts children to `allowed` set. Leaves that yield no
        valid children are pruned from the tree via bubble-prune mechanism.

        Args:
            key_name: Facet key to expand (e.g. "experiment").
            query_fn: Callable that accepts request dict and returns STAC response.
            items_fn: Callable that extracts enum values from STAC response.
            allowed: Optional set of allowed values; non-matching branches pruned.
            logger: Optional logger for debug/info messages.

        Returns:
            tuple: (self, total_children_added_count).
        """
        leaves_list = list(self.leaves())
        if logger:
            logger.debug("expand_level(%s): %d leaves", key_name, len(leaves_list))

        children_added = 0
        nodes_to_remove = []

        for leaf_node, request in leaves_list:
            response = query_fn(request)
            values = items_fn(response, key_name)

            if logger:
                logger.debug(
                    "  %s -> %d values for %s: %s",
                    request,
                    len(values),
                    key_name,
                    values,
                )

            if allowed is not None:
                filtered = [v for v in values if v in allowed]
                if not filtered and values:
                    logger.error(
                        "Filter for '%s' specifies %s but STAC returned %s — no match, pruning branch. Context: %s",
                        key_name,
                        allowed,
                        set(values),
                        request,
                    )
                    nodes_to_remove.append(leaf_node)
                    continue
                if logger and filtered != values:
                    logger.debug(
                        "    filtered to %d values (allowed: %s)",
                        len(filtered),
                        allowed,
                    )
                values = filtered

            added = [CatalogNode(key=key_name, value=v) for v in values]
            for n in added:
                leaf_node.children[n.value] = n
            children_added += len(added)
            if not added:
                nodes_to_remove.append(leaf_node)

        for node in nodes_to_remove:
            self._remove_node(node)

        if logger:
            logger.info(
                "expand_level(%s): +%d children, -%d branches",
                key_name,
                children_added,
                len(nodes_to_remove),
            )

        return self, children_added

    def _remove_node(self, target: "CatalogNode"):
        """Remove target from tree and bubble-prune empty ancestors up to root.

        Args:
            target: Node to remove.
        """

        def _prune(node: "CatalogNode", target: "CatalogNode", root: "CatalogNode") -> bool:
            # Returns True if node became empty and should be pruned by its parent
            for value, child in list(node.children.items()):
                if child is target:
                    del node.children[value]
                    return node is not root and not node.children
                if _prune(child, target, root):
                    del node.children[value]
                    return node is not root and not node.children
            return False

        _prune(self, target, root=self)

    def add_path(self, *pairs: tuple[str, Any]) -> "CatalogNode":
        """Add a chain of (key, value) pairs as nested descendants.

        Reuses existing nodes with matching keys; creates new nodes as needed.

        Args:
            *pairs: Variable number of (key, value) tuples.

        Returns:
            CatalogNode: The leaf node of the added chain.
        """
        node = self
        for key, value in pairs:
            if value in node.children:
                node = node.children[value]
            else:
                child = CatalogNode(key=key, value=value)
                node.children[child.value] = child
                node = child
        return node


class _TableExtractor(HTMLParser):
    """Extract the depth top table from the HTML page for ocean model levels."""

    def __init__(self):
        super().__init__()
        self.sections = []  # list of (heading_text, [[cell, cell, ...], ...])
        self._in_h2 = False
        self._in_table = False
        self._in_cell = False
        self._cur_heading = ""
        self._cur_table = []
        self._cur_row = []
        self._cur_cell = ""

    def handle_starttag(self, tag, attrs):
        if tag == "h2":
            self._in_h2 = True
            self._cur_heading = ""
        elif tag == "table":
            self._in_table = True
            self._cur_table = []
        elif tag == "tr" and self._in_table:
            self._cur_row = []
        elif tag in ("td", "th") and self._in_table:
            self._in_cell = True
            self._cur_cell = ""

    def handle_endtag(self, tag):
        if tag == "h2":
            self._in_h2 = False
        elif tag == "table":
            self._in_table = False
            if self._cur_heading:
                self.sections.append((self._cur_heading.strip(), self._cur_table))
                self._cur_heading = ""  # only attach table to nearest preceding h2
        elif tag == "tr" and self._in_table:
            if self._cur_row:
                self._cur_table.append(self._cur_row)
        elif tag in ("td", "th") and self._in_table:
            self._in_cell = False
            self._cur_row.append(self._cur_cell.strip())

    def handle_data(self, data):
        if self._in_h2:
            self._cur_heading += data
        elif self._in_cell:
            self._cur_cell += data


def get_depth_top(model: str, url: str = DEPTH_URL) -> list[float]:
    """
    Return the list of 'Depth top (m)' values (indexed by level, 0-based)
    for a given ocean model: 'ICON', 'IFS-NEMO', or 'IFS-FESOM'.
    """
    html = urllib.request.urlopen(url, timeout=30).read().decode("utf-8")

    parser = _TableExtractor()
    parser.feed(html)

    for heading, table in parser.sections:
        if heading.lower().startswith(model.lower()):
            header = [c.lower() for c in table[0]]
            idx = next(i for i, h in enumerate(header) if "depth top" in h)
            return [float(re.sub(r"[^\d.\-]", "", row[idx])) for row in table[1:]]

    raise ValueError(f"Model '{model}' not found on page")


class AquaSTACGenerator:
    """STAC-based catalog generator for AQUA."""

    def __init__(self, catalog: str, bridge_url: str, loglevel="WARNING"):
        """
        Args:
            catalog (str): Catalog identifier, e.g. "climate-dt-gen2".
            bridge_url (str): STAC API URL for the bridge node.
            loglevel (str, optional): Logging level. Defaults to "WARNING".
        """
        self.logger = log_configure(log_level=loglevel, log_name="AquaSTACGenerator")
        self.loglevel = loglevel
        self.catalog = catalog
        self.stac_url = bridge_url
        self.tree = None
        self.full_request = None
        self.logger.info("Initializing STAC catalog generator for %s", catalog)

    def explore_tree(self, layers: list[str], filters: dict[str, list] | None = None):
        """Build the facet tree by iteratively expanding STAC layers.

        Starts with root node containing catalog identifier and generation, then
        expands layer-by-layer via STAC queries. Applies optional filters and
        prunes branches with no matches.

        Args:
            layers: Ordered facet keys, e.g. ["activity", "experiment", "realization"].
            filters: Optional dict {key: value_or_list} to restrict expansion.
                     Values are normalized to sets for O(1) membership tests.

        Returns:
            None. Updates self.tree and self.full_request (leaves + univocal nodes).
        """
        # Normalise all filter values to sets for O(1) membership tests
        filters = {k: {v} if isinstance(v, (str, int)) else set(v) for k, v in (filters or {}).items()}

        name, gen = self._parse_catalog()
        self.tree = CatalogNode(key="dataset", value=name)
        self.tree.add_path(("generation", gen))

        for key_name in layers:
            allowed = filters.get(key_name)
            _, children_added = self.tree.expand_level(
                key_name,
                query_fn=self._query_stac,
                items_fn=self.items_from_response,
                allowed=allowed,
                logger=self.logger,
            )
            if children_added == 0:
                self.logger.warning(
                    "No children added for layer '%s'. Stopping tree expansion.",
                    key_name,
                )
                break

        self.full_request = self.tree.all_requests(leaves_only=True, include_univocal=True)
        self.logger.info("explore_tree: %d requests (leaves + univocal)", len(self.full_request))
        self.logger.debug("Requests: %s", self.full_request)

    def complete_request(self):
        """Enrich each leaf request with date/time/param enums from STAC.

        For each complete request dict in self.full_request, query STAC API and
        extract variable enums (realization, date, time, param), then update the
        request dict in-place.

        Raises:
            RuntimeError: If called before explore_tree().
        """
        if not self.full_request:
            raise RuntimeError("Call explore_tree() before complete_request().")
        for params in self.full_request:
            self.logger.info("Completing request: %s", params)
            stac_response = self._query_stac(params=params)
            self._enrich_params(params, stac_response)

    def _get_resolution(self, request: dict) -> tuple[str, int]:
        """Map STAC resolution value to (healpix, km) tuple.

        Args:
            request: Request dict containing "resolution" key.

        Returns:
            tuple: (healpix_grid_name, spatial_resolution_km).

        Raises:
            ValueError: If resolution value is not recognized.
        """
        # HACK: resolution is hard-coded
        # Validate resolution first — it is not stored as a key so KeyError won't catch it
        resolution = request.get("resolution")
        activity = request.get("activity", "").lower()
        if resolution == "standard":
            if activity in ["baseline", "projections"]:
                healpix, km = "hpz7", 5  # km not recoverable from request
            elif activity == "story-nudging":
                healpix, km = "hpz7", 10
            else:
                self.logger.warning("Unrecognized activity '%s' for standard resolution; defaulting to hpz7/5km", activity)
                healpix, km = "hpz7", 5
        elif resolution == "high":
            if activity in ["baseline", "projections"]:
                healpix, km = "hpz10", 5
            elif activity == "story-nudging":
                healpix, km = "hpz9", 10
            else:
                self.logger.warning("Unrecognized activity '%s' for high resolution; defaulting to hpz10/5km", activity)
                healpix, km = "hpz10", 5
        else:
            raise ValueError(f"Unrecognized resolution value: {resolution!r}")
        return healpix, km

    def _load_grids_config(self) -> dict:
        """Load and cache matching_grids.yaml configuration.

        Returns:
            dict: Parsed YAML with grid definitions and templates.
        """
        if not hasattr(self, "_grids_config"):
            config_path = os.path.join(ConfigPath().configdir, "catgen", "matching_grids.yaml")
            self._grids_config = load_yaml(config_path)
        return self._grids_config

    def _get_grid_from_config(self, request: dict) -> str:
        """Deduce grid name from matching_grids.yaml configuration.

        Assumes baseline and projection activities use 'production' tier grids;
        all others use 'develop' tier. Looks up model-specific atmospheric and
        ocean grids, then applies grid_mappings templates.

        Args:
            request: Request dict with model, levtype, activity, resolution.

        Returns:
            str: Grid identifier (e.g. "nemo-eORCA12-hpz7-nested-v3").

        Raises:
            KeyError: If model or levtype not found in matching_grids.yaml.
        """
        grids = self._load_grids_config()

        # Determine tier: production for baseline/projection, develop otherwise
        activity = request.get("activity", "").lower()
        tier = "production" if activity in ("baseline", "projection") else "develop"

        model = request.get("model", "").lower()
        levtype = request.get("levtype", "").lower()
        healpix, _ = self._get_resolution(request)

        grid_mappings = grids.get("grid_mappings", {})

        if levtype in ("o2d", "o3d"):
            # Ocean grid case
            ocean_grid = grids.get("ocean_grid", {}).get(model, {}).get(tier)
            grid_map = grid_mappings.get(levtype, {})
            template = grid_map.get(model)
            if template and ocean_grid:
                result = template.format(ocean_grid=ocean_grid, aqua_grid=healpix)
                self.logger.debug(
                    "Grid mapped (ocean): %s/%s (tier=%s) → %s",
                    model,
                    levtype,
                    tier,
                    result,
                )
                return result
        else:
            # Atmosphere/surface grid case
            grid_map = grid_mappings.get(levtype, {})
            template = grid_map.get("default") or grid_mappings.get("default")
            if template:
                result = template.format(aqua_grid=healpix)
                self.logger.debug("Grid mapped (atm): %s/%s → %s", model, levtype, result)
                return result

        # Fallback if mapping not found
        fallback = f"{healpix}-nested"
        self.logger.warning(
            "Grid mapping not found for %s/%s (tier=%s), using fallback: %s",
            model,
            levtype,
            tier,
            fallback,
        )
        return fallback

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _parse_catalog(self) -> tuple[str, int]:
        """Parse 'name-genN' catalog string into (name, generation) pair.

        Args:
            (self.catalog): Expected format "name-genN" (case-insensitive).

        Returns:
            tuple: (lowercased_name, generation_int).

        Raises:
            ValueError: If catalog string does not match expected format.
        """
        match = re.match(r"^(.*)-gen(\d+)$", self.catalog.strip(), re.IGNORECASE)
        if not match:
            raise ValueError(f"Cannot parse catalog identifier: {self.catalog!r}")
        name, gen = match.groups()
        if name == "climatedt":
            name = "climate-dt"  # normalize legacy name
        return name.lower(), int(gen)

    def _enrich_params(
        self,
        params: dict,
        stac_response: dict,
        keys: tuple[str, ...] = ("realization", "date", "time", "param", "levelist"),
    ):
        """Add date/time/param enums from STAC response links into params (in-place).

        Args:
            params: Request dict to update.
            stac_response: STAC API response JSON.
            keys: Tuple of enum keys to extract from response.

        Returns:
            None. Modifies params in-place.
        """
        for element in stac_response.get("links", []):
            title = element["title"]
            variables = element.get("variables", {})
            if title in keys and "enum" in variables.get(title, {}):
                params[title] = variables[title]["enum"]
                self.logger.debug("Enriched param '%s' with %d values", title, len(params[title]))

    def _query_stac(self, params: dict) -> dict:
        """Query the STAC API and return the parsed JSON response.

        Normalizes all string param values to lowercase and adds 'root' sentinel.
        Handles network errors and HTTP status codes with appropriate exceptions.

        Args:
            params: FDB-like request parameters (will not be mutated).

        Returns:
            dict: Parsed STAC JSON response.

        Raises:
            TimeoutError: If request exceeds 10 seconds.
            ConnectionError: If network request fails.
            ValueError: On HTTP error or invalid JSON.
        """
        params = {k: v.lower() if isinstance(v, str) else v for k, v in params.items()}
        params["root"] = "root"
        self.logger.debug("STAC query params: %s", params)

        try:
            response = requests.get(self.stac_url, params=params, timeout=10)
        except requests.Timeout as e:
            raise TimeoutError("STAC API request timed out after 10 seconds.") from e
        except requests.RequestException as e:
            raise ConnectionError("STAC API request failed") from e

        if response.status_code == 400:
            raise ValueError(f"Bad request to STAC API: {response.text}")
        if response.status_code == 503:
            raise ValueError(f"Service unavailable: {response.text}")
        if response.status_code != 200:
            raise ValueError(f"Unexpected STAC response: {response.status_code} - {response.text}")

        try:
            stac_json = response.json()
        except ValueError as exc:
            raise ValueError("Failed to parse STAC API response as JSON") from exc

        self.logger.debug("STAC links: %s", [lnk.get("title") for lnk in stac_json.get("links", [])])
        return stac_json

    @staticmethod
    def items_from_response(response: dict, key: str) -> list:
        """Extract enum values for a given facet key from STAC response links.

        Args:
            response: STAC API response JSON.
            key: Facet key name to extract (e.g. "experiment", "model").

        Returns:
            list: Enum values for the key, or empty list if not found.
        """
        for element in response.get("links", []):
            if element["title"] == key:
                return element["variables"][key]["enum"]
        return []

    def get_context(self, request: dict) -> dict:
        """Build the Jinja2 rendering context for one STAC request.

        Expects complete_request() to have been called first so that
        'date', 'time', 'param', and 'realization' lists are present.

        Maps levtype and model to grid name, derives savefreq/chunks from time
        steps per day, and extracts key fields for catalog entry template.

        Args:
            request: Fully enriched request dict with all facet enums populated.

        Returns:
            dict: Context dict with keys:
                - source: Derived source name (frequency-healpix-levtype)
                - grid: Grid identifier (healpix-nested or model-specific)
                - savefreq, chunks: Frequency strings (h, D, MS)
                - num_of_realizations, default_realization: Realization info
                - data_start_date, data_end_date: Date range for catalog
                - time, param, variables: Time step and parameter info
                - plus all original request keys
        """
        times = request.get("time", [])
        levtype = request.get("levtype", "")

        healpix, _ = self._get_resolution(request)
        grid = self._get_grid_from_config(request)

        # savefreq and chunks derived from number of time steps per day
        if not times:
            savefreq, chunks = "MS", "MS"  # monthly stream
        elif len(times) == 1:
            savefreq, chunks = "D", "D"
        elif len(times) >= 4 and levtype == "pl":
            savefreq, chunks = "h", "6h"
        else:
            savefreq, chunks = "h", "D"
        frequency = "hourly" if savefreq == "h" else "daily" if savefreq == "D" else "monthly"

        context = {**request}
        context["source"] = f"{frequency}-{healpix}-{request['levtype']}"
        context["num_of_realizations"] = len(request.get("realization", []))
        context["default_realization"] = 1
        context["data_start_date"] = request.get("date", [])[0].replace("-", "")
        context["data_end_date"] = request.get("date", [])[-1].replace("-", "") + "T2300"
        context["time"] = times[0] if times else None
        context["param"] = request.get("param", [])[0]
        context["savefreq"] = savefreq
        context["chunks"] = chunks
        context["variables"] = request.get("param", [])
        context["grid"] = grid
        context["levels"] = get_depth_top(request.get("model").upper()) if levtype == "o3d" else None
        context["description"] = (
            f"STAC-derived catalog entry for {context['source']} ({request.get('model')}, {request.get('experiment')})"
        )
        context.pop("date")

        self.logger.debug("Context for source %s: %s", context["source"], context)

        return context

    # def generate_source(self, output_path: str, extra_context: dict | None = None):
    #     """Render catalog_entry.j2 for every request in full_request and dump to YAML.

    #     Merges variables across requests with the same source name (same
    #     frequency-healpix-levtype). Applies extra_context to each rendered context
    #     before template rendering.

    #     Args:
    #         output_path: Destination YAML file path.
    #         extra_context: Optional dict with extra keys for all templates
    #             (e.g. grid, description, expid, fixer_name, fdb_home).

    #     Returns:
    #         None. Writes YAML file to output_path.

    #     Raises:
    #         RuntimeError: If called before explore_tree() + complete_request().
    #     """
    #     if not self.full_request:
    #         raise RuntimeError("Call explore_tree() and complete_request() before generate_source().")

    #     template = os.path.join(ConfigPath().configdir, "catgen", "catalog_entry.j2")
    #     extra_context = extra_context or {}
    #     all_sources = {}

    #     for request in self.full_request:
    #         context = {**self.get_context(request), **extra_context}
    #         source_name = context["source"]
    #         self.logger.debug("Rendering template for source: %s", source_name)
    #         try:
    #             rendered = load_yaml(template, definitions=context, catgen=True)
    #             if source_name in all_sources:
    #                 # merge variables into existing entry rather than overwriting
    #                 all_sources[source_name]["metadata"]["variables"].extend(context["variables"])
    #                 self.logger.debug("Merged variables into existing source %s", source_name)
    #             else:
    #                 all_sources[source_name] = rendered[source_name]
    #         except Exception as e:
    #             self.logger.error("Error rendering template for source %s: %s", source_name, e)

    #     os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    #     dump_yaml(output_path, {"sources": all_sources})
    #     self.logger.info("Catalog written to %s (%d sources)", output_path, len(all_sources))

    def generate_catalog(
        self,
        catalog_dir_path: str,
        catalog_name: str = "climatedt-gen2-stac",
        extra_context: dict | None = None,
    ):
        """Generate STAC-derived catalogs organized by (model, experiment) triplets.

        Groups all requests by (model, km) pairs, renders templates per group,
        and creates hierarchical directory structure with main.yaml and catalog.yaml
        metadata files. Uses thread-safe YAML writes via SafeFileLock.

        Creates file structure:
            {catalog_dir_path}/catalogs/{catalog_name}/catalog/{MODEL}/
                ├─ main.yaml (index of experiments)
                ├─ {experiment1}.yaml
                └─ {experiment2}.yaml
            {catalog_dir_path}/catalogs/{catalog_name}/catalog.yaml (model index)

        Args:
            catalog_dir_path: Base path for the climate catalog storage.
            catalog_name: Catalog name. Defaults to "climatedt-gen2-stac".
            extra_context: Optional dict with extra keys for all templates.

        Returns:
            None. Writes YAML files to disk.

        Raises:
            RuntimeError: If called before explore_tree() + complete_request().
        """
        if not self.full_request:
            raise RuntimeError("Call explore_tree() and complete_request() before generate_catalog().")

        template = os.path.join(ConfigPath().configdir, "catgen", "catalog_entry.j2")
        extra_context = extra_context or {}

        # Group requests by (model, experiment) to generate one file per combination
        grouped = {}
        for request in self.full_request:
            _, km = self._get_resolution(request)
            experiment = f"{request['activity']}-{request['experiment'].replace('.', '').replace('-', '')}"
            model = f"{request['model'].upper()}-{km}km"
            key = (model, experiment)

            if key not in grouped:
                grouped[key] = []
            grouped[key].append(request)

        self.logger.info(
            "Generating catalogs for %d (model, experiment) combinations",
            len(grouped),
        )

        for (model, experiment), req_list in grouped.items():
            all_sources = {}

            for request in req_list:
                context = {**self.get_context(request), **extra_context}
                source_name = context["source"]
                self.logger.debug("Rendering %s for %s/%s", source_name, model, experiment)

                try:
                    rendered = load_yaml(template, definitions=context, catgen=True)
                    if source_name in all_sources:
                        all_sources[source_name]["metadata"]["variables"].extend(context["variables"])
                        self.logger.debug("Merged variables into %s", source_name)
                    else:
                        all_sources[source_name] = rendered[source_name]
                except Exception as e:
                    self.logger.error("Error rendering %s: %s", source_name, e)

            # Create output directory and file
            output_dir = os.path.join(catalog_dir_path, "catalogs", catalog_name, "catalog", model)
            os.makedirs(output_dir, exist_ok=True)

            output_file = os.path.join(output_dir, f"{experiment}.yaml")
            dump_yaml(output_file, {"sources": all_sources})
            self.logger.info(
                "Wrote %s with %d sources to %s",
                f"{experiment}.yaml",
                len(all_sources),
                output_dir,
            )

            # Update main.yaml for the model
            main_yaml_path = os.path.join(output_dir, "main.yaml")
            self._update_main_yaml(main_yaml_path, experiment, all_sources)

        # Update catalog.yaml
        catalog_yaml_path = os.path.join(catalog_dir_path, "catalogs", catalog_name, "catalog.yaml")
        self._update_catalog_yaml(catalog_yaml_path, grouped.keys())

        # generate a machine file default
        machine_file_path = os.path.join(catalog_dir_path, "catalogs", catalog_name, "machine.yaml")
        machine_content = {
            "default": {
                "paths": {"grids": "./AQUA_tests/grids", "weights": "./AQUA_tests/weights", "areas": "./AQUA_tests/weights"}
            }
        }
        dump_yaml(machine_file_path, machine_content)

    def _update_main_yaml(self, main_yaml_path: str, experiment: str, sources: dict):
        """Update main.yaml with a source entry for the experiment (thread-safe).

        Adds or updates an entry mapping experiment name to its YAML file.
        Uses SafeFileLock to protect concurrent writes.

        Args:
            main_yaml_path: Path to main.yaml for the model.
            experiment: Experiment identifier (e.g. "baseline-amip").
            sources: Dict of rendered sources for this experiment.

        Returns:
            None. Modifies main.yaml in-place.
        """

        with SafeFileLock(main_yaml_path + ".lock", loglevel=self.loglevel):
            if os.path.exists(main_yaml_path):
                main_yaml = load_yaml(main_yaml_path)
            else:
                main_yaml = {"sources": {}}

            # Extract description from first source's metadata
            description = None
            for source_data in sources.values():
                if "description" in source_data:
                    description = source_data["description"]
                    break

            main_yaml["sources"][experiment] = {
                "description": description or f"Experiment {experiment}",
                "driver": "yaml_file_cat",
                "args": {"path": f"{{{{CATALOG_DIR}}}}/{experiment}.yaml"},
            }

            dump_yaml(main_yaml_path, main_yaml)
            self.logger.info("Updated main.yaml for experiment %s", experiment)

    def _update_catalog_yaml(self, catalog_yaml_path: str, model_experiments: list):
        """Update catalog.yaml with entries for all models (thread-safe).

        Adds or updates entries mapping model names to their main.yaml files.
        Uses SafeFileLock to protect concurrent writes.

        Args:
            catalog_yaml_path: Path to top-level catalog.yaml.
            model_experiments: Iterable of (model, experiment) tuples to index.

        Returns:
            None. Modifies catalog.yaml in-place.
        """

        with SafeFileLock(catalog_yaml_path + ".lock", loglevel=self.loglevel):
            if os.path.exists(catalog_yaml_path):
                catalog_yaml = load_yaml(catalog_yaml_path)
            else:
                catalog_yaml = {"sources": {}}

            # Add each model to catalog.yaml if not already present
            for model, _ in model_experiments:
                if model not in catalog_yaml.get("sources", {}):
                    catalog_yaml.setdefault("sources", {})
                    catalog_yaml["sources"][model] = {
                        "description": f"{model} model",
                        "driver": "yaml_file_cat",
                        "args": {"path": f"{{{{CATALOG_DIR}}}}/catalog/{model}/main.yaml"},
                    }
                    self.logger.debug("Added %s model entry to catalog.yaml", model)

            dump_yaml(catalog_yaml_path, catalog_yaml)
            self.logger.info(
                "Updated catalog.yaml with %d models",
                len(set(m for m, _ in model_experiments)),
            )
