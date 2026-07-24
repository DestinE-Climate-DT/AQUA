"""Intake catalog browsing and inspection helpers for AQUA."""

import intake

from aqua.core.logger import log_configure
from aqua.core.util.util import to_list


class CatalogBrowser:
    """
    Browses, validates, and displays the content of intake catalogs.

    This class does not resolve configuration paths itself - it is built on
    top of a `ConfigPaths` (or backward-compatible `ConfigPath`) instance,
    which is responsible for knowing where catalog and machine files live on
    disk. `CatalogBrowser` is the only place in the codebase that imports
    `intake`.

    Note on coupling: this implementation takes a `paths` object and calls
    back into it (`paths.catalog_available`, `paths.get_catalog_filenames`,
    `paths.catalog`, ...). This mirrors the original `ConfigPath` behavior
    closely, including mutating `paths.catalog` when a specific catalog is
    "delivered". A looser-coupled alternative would have `CatalogBrowser`
    accept a plain dict of {catalog_name: catalog_file_path} instead of a
    live `paths` object - that removes the dependency on `ConfigPaths`
    entirely and makes this class testable with no config file on disk at
    all. Worth considering if `ConfigPaths`'s API keeps growing.
    """

    def __init__(self, paths, loglevel=None):
        """
        Args:
            paths (ConfigPaths): object providing catalog_available,
                get_catalog_filenames(), and a mutable `catalog` attribute.
            loglevel (str | None): if provided, sets up an independent logger
                for this class; otherwise reuses `paths.logger`.
        """
        self.paths = paths
        self.logger = paths.logger if loglevel is None else log_configure(log_level=loglevel, log_name="CatalogBrowser")

    def browse_catalogs(self, model: str, exp: str, source: str):
        """
        Given a triplet of model-exp-source, browse all catalog installed catalogs

        Returns
            a list of catalogs where the triplet is found
            a dictionary with information on wrong triplet
        """
        success = []
        fail = {}

        if self.paths.catalog_available is None:
            return success, fail

        if not all(v is not None for v in [model, exp, source]):
            raise KeyError("Need to defined the triplet model, exp and source")

        for catalog in self.paths.catalog_available:
            self.logger.debug("Browsing catalog %s ...", catalog)
            catalog_file, _ = self.paths.get_catalog_filenames(catalog)
            cat = intake.open_catalog(catalog_file)
            check, level, avail = self.scan_catalog(cat, model=model, exp=exp, source=source)
            if check:
                self.logger.info("%s_%s_%s triplet found in in %s!", model, exp, source, catalog)
                success.append(catalog)
            else:
                fail[catalog] = (
                    f"In catalog {catalog} when looking for {model}_{exp}_{source} "
                    f"triplet I could not find the {level}. Available alternatives are {avail}"
                )
        return success, fail

    def deliver_intake_catalog(self, model, exp, source, catalog=None):
        """
        Given a triplet of model-exp-source (and possibly a specific catalog), browse the catalog
        and check if the triplet can be found

        Returns:
            intake.catalog.Catalog: The intake catalog
            str: The path to the catalog file
            str: The path to the machine file
        """
        matched, failed = self.browse_catalogs(model=model, exp=exp, source=source)
        if not matched:
            for _, value in failed.items():
                self.logger.error(value)
            raise KeyError("Cannot find the triplet in any catalog. Check logger error for hints on possible typos")

        if catalog is not None:
            self.paths.catalog = catalog
        else:
            if len(matched) > 1:
                self.logger.warning("Multiple triplets found in %s, setting %s as the default", matched, matched[0])
            self.paths.catalog = matched[0]

        self.logger.debug("Final catalog to be used is %s", self.paths.catalog)
        self.paths.catalog_file, self.paths.machine_file = self.paths.get_catalog_filenames(self.paths.catalog)
        return intake.open_catalog(self.paths.catalog_file), self.paths.catalog_file, self.paths.machine_file

    @staticmethod
    def scan_catalog(cat, model=None, exp=None, source=None):
        """
        Check if the model, experiment and source are in the catalog.

        Returns:
            status (bool): True if the triplet is found
            level (str): The level at which the triplet is failing
            info (str): The available catalog entries at the level of the triplet
        """
        status = False
        avail = None
        level = None

        if model in cat:
            if exp in cat[model]:
                if source in cat[model][exp]:
                    status = True
                else:
                    level = "source"
                    avail = list(cat[model][exp].keys())
            else:
                level = "exp"
                avail = list(cat[model].keys())
        else:
            level = "model"
            avail = list(cat.keys())

        return status, level, avail

    def show_catalog_content(self, catalog=None, model=None, exp=None, source=None, verbose=True, show_descriptions=False):
        """
        Scan catalog(s) by reading YAML files directly and display the model/exp/source structure.
        Uses intake to handle path resolution automatically.

        Args:
            catalog (str | list | None): Specific catalog(s) to scan. If None, loops over all available catalogs.
            model (str | None): Optional model filter.
            exp (str | None): Optional experiment filter.
            source (str | None): Optional source filter.
            verbose (bool): If True, prints the formatted catalog structure. Defaults to True.
            show_descriptions (bool): If True, also print per-source descriptions.

        Returns:
            dict: Dictionary with catalog names as keys and nested dict structure as values.
        """
        self.logger = log_configure(log_level="info", log_name="ShowCatalog")

        results = {}
        catalogs_to_scan = to_list(catalog) if catalog else self.paths.catalog_available

        if not catalogs_to_scan:
            self.logger.warning("No catalogs available to scan")
            return results

        self.logger.debug("Catalogs to show: %s", catalogs_to_scan)

        for cat_name in catalogs_to_scan:
            try:
                catalog_file, _ = self.paths.get_catalog_filenames(catalog=cat_name)
                cat = intake.open_catalog(catalog_file)
            except (KeyError, FileNotFoundError, Exception) as exc:
                self.logger.warning("Cannot open/scan catalog %s: %s", cat_name, exc)
                continue

            structure = {}
            descriptions = {}  # model -> exp -> {source: description}

            models = [model] if model else list(cat.keys())

            for model_name in models:
                if model_name not in cat:
                    self.logger.warning(
                        "Model %s not found in catalog %s. Available: %s", model_name, cat_name, list(cat.keys())
                    )
                    continue

                model_cat = cat[model_name]
                experiments = [exp] if exp else list(model_cat.keys())

                for exp_name in experiments:
                    if exp_name not in model_cat:
                        self.logger.warning(
                            "Experiment %s not found in model %s. Available: %s", exp_name, model_name, list(model_cat.keys())
                        )
                        continue

                    exp_cat = model_cat[exp_name]
                    sources = list(exp_cat.keys())

                    if source:
                        sources = [s for s in sources if s == source]

                    if not sources:
                        continue

                    if model_name not in structure:
                        structure[model_name] = {}
                    structure[model_name][exp_name] = sources

                    # Pre-fetch descriptions if needed (once per experiment)
                    if show_descriptions:
                        if model_name not in descriptions:
                            descriptions[model_name] = {}
                        descriptions[model_name][exp_name] = self._extract_source_descriptions(exp_cat, sources)

            if not structure:
                continue

            results[cat_name] = structure

            if verbose:
                print(self.format_catalog_structure(structure, cat_name, descriptions if show_descriptions else None))

        return results

    @staticmethod
    def _extract_source_descriptions(exp_cat, sources):
        """Safely extract descriptions for a list of sources from an experiment catalog."""
        try:
            walk_dict = exp_cat.walk()
        except Exception:  # noqa: BLE001
            return {}

        descs = {}
        for source in sources:
            try:
                # Access internal _description as in original implementation
                desc = walk_dict[source]._description  # pylint: disable=W0212
                if desc:
                    descs[source] = desc
            except Exception:  # noqa: BLE001
                pass
        return descs

    @staticmethod
    def format_catalog_structure(structure, catalog_name, descriptions=None):
        """
        Format catalog structure as a nicely aligned tree.

        Args:
            structure: Dictionary with model/exp/source structure
            catalog_name: Name of the catalog
            descriptions: Optional nested dict [model][exp][source] -> description.
                          If provided, prints one source per line with description.
                          If None, prints compact 3-column view.
        """
        lines = [f"\n{'=' * 80}", f"📁 Catalog: {catalog_name}", f"{'=' * 80}"]

        for model_name, experiments in sorted(structure.items()):
            lines.append(f"\n   Model: {model_name}")

            for exp_name, sources in sorted(experiments.items()):
                lines.append(f"     └─ Experiment: {exp_name}")

                if not sources:
                    continue

                sorted_sources = sorted(sources)

                if descriptions:
                    # Detailed view: One source per line with description
                    exp_descs = descriptions.get(model_name, {}).get(exp_name, {})
                    for source_key in sorted_sources:
                        desc = exp_descs.get(source_key, "")
                        lines.append(f"        - {source_key:<25} {desc}")
                else:
                    # Compact view: Group sources in rows of 3
                    for i in range(0, len(sorted_sources), 3):
                        source_group = sorted_sources[i : i + 3]
                        formatted_sources = "  ".join(f"{s:<25}" for s in source_group)
                        lines.append(f"        ├─ {formatted_sources}")

        lines.append(f"{'=' * 80}\n")
        return "\n".join(lines)
