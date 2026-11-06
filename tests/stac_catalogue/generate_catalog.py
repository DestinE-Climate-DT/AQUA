from __future__ import annotations

import json
import os
import re
import subprocess
from datetime import datetime, timedelta, timezone

OUTPUT = os.path.dirname(os.path.realpath(__file__))
MEDIA_TYPE = "application/x-netcdf"
MODEL_DESCRIPTIONS = {
    "ECE4-FAST": "EC-Earth 4 FAST atmosphere, ocean, and sea-ice test datasets.",
    "ERA5": "ERA5 monthly test datasets on the AQUA hpz3 grid.",
    "FESOM": "IFS-FESOM model and derived-result test datasets.",
    "ICON": "ICON low-resolution and HEALPix test datasets.",
    "IFS": "IFS native, regridded, and teleconnection test datasets.",
    "NEMO": "NEMO ocean and derived-result test datasets.",
}


def models_directory() -> str:
    """Find the downloaded fixtures from the generator's checkout."""
    directory = OUTPUT
    while True:
        candidate = os.path.join(directory, "AQUA_tests", "models")
        if os.path.isdir(candidate):
            return candidate
        parent = os.path.dirname(directory)
        if parent == directory:
            break
        directory = parent
    raise FileNotFoundError("AQUA_tests/models is required at the AQUA repository root")


def dump(path: str, value: dict) -> None:
    """Write the JSON atomically, so that a reader never sees a partial file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = os.path.join(os.path.dirname(path), f".{os.path.basename(path)}.{os.getpid()}.tmp")
    with open(tmp, "w") as file:
        file.write(json.dumps(value, indent=2, ensure_ascii=False) + "\n")
    os.replace(tmp, path)


def slug(value: str) -> str:
    value = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return value or "item"


def header(path: str) -> str:
    return subprocess.run(
        ["ncdump", "-h", path],
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def variables(ncdump_header: str) -> list[str]:
    dimension_names = set()
    dimensions = re.search(r"dimensions:\n(.*?)variables:", ncdump_header, re.S)
    if dimensions:
        dimension_names.update(re.findall(r"^\s*([A-Za-z_][\w]*)\s*=", dimensions.group(1), re.M))

    declarations = re.findall(
        r"^\s*(?:byte|char|short|int|int64|float|double|string|u?int\d*)\s+"
        r"(?:\\)?([^\s(]+)(?:\([^)]*\))?\s*;",
        ncdump_header,
        re.M | re.I,
    )
    coordinate_names = {"time", "time_counter", "lat", "latitude", "lon", "longitude", "lev", "level", "cell", "ncells"}
    return sorted(
        name
        for name in declarations
        if name not in dimension_names and name not in coordinate_names and not name.endswith("_bnds")
    )


def parse_base_datetime(raw: str) -> datetime:
    raw = raw.strip().replace("T", " ").replace("Z", "")
    match = re.match(
        r"(\d{4})-(\d{1,2})-(\d{1,2})(?:\s+(\d{1,2})(?::(\d{1,2}))?(?::([\d.]+))?)?",
        raw,
    )
    if not match:
        raise ValueError(raw)
    year, month, day, hour, minute, second = match.groups()
    return datetime(
        int(year), int(month), int(day), int(hour or 0), int(minute or 0), int(float(second or 0)), tzinfo=timezone.utc
    )


def time_extent(path: str, ncdump_header: str) -> tuple[str, str, str]:
    for name in ("time", "time_counter"):
        unit_match = re.search(rf"\b{name}:units\s*=\s*\"(seconds|hours|days) since ([^\"]+)\"", ncdump_header)
        if not unit_match:
            continue
        output = subprocess.run(
            ["ncdump", "-v", name, path],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        data = output.split("data:", 1)[-1]
        values_match = re.search(rf"(?:\\)?{re.escape(name)}\s*=\s*(.*?);", data, re.S)
        if not values_match:
            continue
        values = [float(value) for value in re.findall(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", values_match.group(1))]
        if not values:
            continue
        unit, base_raw = unit_match.groups()
        base = parse_base_datetime(base_raw)
        seconds = {"seconds": 1, "hours": 3600, "days": 86400}[unit]
        start = base + timedelta(seconds=min(values) * seconds)
        end = base + timedelta(seconds=max(values) * seconds)
        return iso(start), iso(end), f"NetCDF coordinate {name}"

    year_match = re.search(r"(?<!\d)(19\d{2}|20\d{2})(?:-(19\d{2}|20\d{2}))?(?!\d)", os.path.basename(path))
    if year_match:
        first = int(year_match.group(1))
        last = int(year_match.group(2) or first)
        return f"{first:04d}-01-01T00:00:00Z", f"{last:04d}-12-31T23:59:59Z", "filename fallback"

    modified = datetime.fromtimestamp(os.path.getmtime(path), tz=timezone.utc)
    stamp = iso(modified)
    return stamp, stamp, "file modification-time fallback"


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def main() -> None:
    models = models_directory()
    os.makedirs(OUTPUT, exist_ok=True)
    files = sorted(
        os.path.join(root, name)
        for root, _, names in os.walk(models)
        for name in names
        if name.endswith(".nc") and not os.path.join(root, name).startswith(OUTPUT + os.sep)
    )
    if not files:
        raise FileNotFoundError(f"No NetCDF files found in {models}")
    grouped: dict[str, list[str]] = {}
    metadata: dict[str, tuple[str, str, str, list[str]]] = {}

    for path in files:
        relative = os.path.relpath(path, models)
        model = relative.split(os.sep)[0]
        grouped.setdefault(model, []).append(path)
        nc_header = header(path)
        start, end, source = time_extent(path, nc_header)
        metadata[path] = start, end, source, variables(nc_header)

    catalog_links = [
        {"rel": "root", "href": "./catalog.json", "type": "application/json"},
        {"rel": "self", "href": "./catalog.json", "type": "application/json"},
    ]

    for model, model_files in sorted(grouped.items()):
        collection_id = slug(model)
        collection_href = f"./collections/{collection_id}.json"
        catalog_links.append(
            {"rel": "child", "href": collection_href, "type": "application/json", "title": f"{model} collection"}
        )
        starts = [metadata[path][0] for path in model_files]
        ends = [metadata[path][1] for path in model_files]
        collection_links = [
            {"rel": "root", "href": "../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            {"rel": "self", "href": f"./{collection_id}.json", "type": "application/json"},
        ]

        seen_ids: set[str] = set()
        for path in model_files:
            relative = os.path.relpath(path, models)
            item_id = slug("-".join(os.path.splitext(relative)[0].split(os.sep)))
            if item_id in seen_ids:
                raise RuntimeError(f"Duplicate item id: {item_id}")
            seen_ids.add(item_id)
            item_href = f"../items/{collection_id}/{item_id}.json"
            collection_links.append(
                {"rel": "item", "href": item_href, "type": "application/geo+json", "title": os.path.basename(path)}
            )
            start, end, source, data_variables = metadata[path]
            item_path = os.path.join(OUTPUT, "items", collection_id, f"{item_id}.json")
            asset_href = os.path.relpath(path, os.path.dirname(item_path)).replace(os.sep, "/")
            item = {
                "type": "Feature",
                "stac_version": "1.0.0",
                "stac_extensions": [],
                "id": item_id,
                "geometry": None,
                "properties": {
                    "datetime": start if start == end else None,
                    **({"start_datetime": start, "end_datetime": end} if start != end else {}),
                    "title": os.path.basename(path),
                    "aqua:model": model,
                    "aqua:relative_path": relative.replace(os.sep, "/"),
                    "aqua:variables": data_variables,
                    "aqua:datetime_source": source,
                },
                "links": [
                    {"rel": "root", "href": "../../catalog.json", "type": "application/json"},
                    {"rel": "parent", "href": f"../../collections/{collection_id}.json", "type": "application/json"},
                    {"rel": "collection", "href": f"../../collections/{collection_id}.json", "type": "application/json"},
                    {"rel": "self", "href": f"./{item_id}.json", "type": "application/geo+json"},
                ],
                "assets": {
                    "data": {
                        "href": asset_href,
                        "title": os.path.basename(path),
                        "description": "Local NetCDF test data; open with xarray.open_dataset(asset.href).",
                        "type": MEDIA_TYPE,
                        "roles": ["data"],
                    }
                },
                "collection": collection_id,
            }
            dump(item_path, item)

        collection = {
            "type": "Collection",
            "stac_version": "1.0.0",
            "stac_extensions": [],
            "id": collection_id,
            "title": f"AQUA {model} test data",
            "description": MODEL_DESCRIPTIONS.get(model, f"AQUA {model} NetCDF test datasets."),
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[-180.0, -90.0, 180.0, 90.0]]},
                "temporal": {"interval": [[min(starts), max(ends)]]},
            },
            "summaries": {
                "aqua:model": [model],
                "assets:type": [MEDIA_TYPE],
            },
            "links": collection_links,
        }
        dump(os.path.join(OUTPUT, "collections", f"{collection_id}.json"), collection)

    catalog = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "stac_extensions": [],
        "id": "aqua-local-netcdf",
        "title": "AQUA local NetCDF test catalog",
        "description": "Example static STAC catalog for local PySTAC and Intake development using AQUA NetCDF fixtures.",
        "links": catalog_links,
    }
    dump(os.path.join(OUTPUT, "catalog.json"), catalog)
    print(f"Wrote {len(grouped)} collections and {len(files)} items to {OUTPUT}")


if __name__ == "__main__":
    main()
