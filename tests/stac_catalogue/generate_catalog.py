from __future__ import annotations

import json
import os
import re
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

OUTPUT = Path(__file__).resolve().parent
MEDIA_TYPE = "application/x-netcdf"
MODEL_DESCRIPTIONS = {
    "ECE4-FAST": "EC-Earth 4 FAST atmosphere, ocean, and sea-ice test datasets.",
    "ERA5": "ERA5 monthly test datasets on the AQUA hpz3 grid.",
    "FESOM": "IFS-FESOM model and derived-result test datasets.",
    "ICON": "ICON low-resolution and HEALPix test datasets.",
    "IFS": "IFS native, regridded, and teleconnection test datasets.",
    "NEMO": "NEMO ocean and derived-result test datasets.",
}


def models_directory() -> Path:
    """Find the downloaded fixtures from the generator's checkout."""
    for directory in (OUTPUT, *OUTPUT.parents):
        candidate = directory / "AQUA_tests" / "models"
        if candidate.is_dir():
            return candidate
    raise FileNotFoundError("AQUA_tests/models is required at the AQUA repository root")


def dump(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")


def slug(value: str) -> str:
    value = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return value or "item"


def header(path: Path) -> str:
    return subprocess.run(
        ["ncdump", "-h", str(path)],
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
        name for name in declarations
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


def time_extent(path: Path, ncdump_header: str) -> tuple[str, str, str]:
    for name in ("time", "time_counter"):
        unit_match = re.search(rf"\b{name}:units\s*=\s*\"(seconds|hours|days) since ([^\"]+)\"", ncdump_header)
        if not unit_match:
            continue
        output = subprocess.run(
            ["ncdump", "-v", name, str(path)],
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

    year_match = re.search(r"(?<!\d)(19\d{2}|20\d{2})(?:-(19\d{2}|20\d{2}))?(?!\d)", path.name)
    if year_match:
        first = int(year_match.group(1))
        last = int(year_match.group(2) or first)
        return f"{first:04d}-01-01T00:00:00Z", f"{last:04d}-12-31T23:59:59Z", "filename fallback"

    modified = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    stamp = iso(modified)
    return stamp, stamp, "file modification-time fallback"


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def main() -> None:
    models = models_directory()
    OUTPUT.mkdir(parents=True, exist_ok=True)
    files = sorted(path for path in models.rglob("*.nc") if OUTPUT not in path.parents)
    if not files:
        raise FileNotFoundError(f"No NetCDF files found in {models}")
    grouped: dict[str, list[Path]] = {}
    metadata: dict[Path, tuple[str, str, str, list[str]]] = {}

    for path in files:
        relative = path.relative_to(models)
        model = relative.parts[0]
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
            relative = path.relative_to(models)
            item_id = slug("-".join(relative.with_suffix("").parts))
            if item_id in seen_ids:
                raise RuntimeError(f"Duplicate item id: {item_id}")
            seen_ids.add(item_id)
            item_href = f"../items/{collection_id}/{item_id}.json"
            collection_links.append(
                {"rel": "item", "href": item_href, "type": "application/geo+json", "title": path.name}
            )
            start, end, source, data_variables = metadata[path]
            item_path = OUTPUT / "items" / collection_id / f"{item_id}.json"
            asset_href = Path(os.path.relpath(path, item_path.parent)).as_posix()
            item = {
                "type": "Feature",
                "stac_version": "1.0.0",
                "stac_extensions": [],
                "id": item_id,
                "geometry": None,
                "properties": {
                    "datetime": start if start == end else None,
                    **({"start_datetime": start, "end_datetime": end} if start != end else {}),
                    "title": path.name,
                    "aqua:model": model,
                    "aqua:relative_path": relative.as_posix(),
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
                        "title": path.name,
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
        dump(OUTPUT / "collections" / f"{collection_id}.json", collection)

    catalog = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "stac_extensions": [],
        "id": "aqua-local-netcdf",
        "title": "AQUA local NetCDF test catalog",
        "description": "Example static STAC catalog for local PySTAC and Intake development using AQUA NetCDF fixtures.",
        "links": catalog_links,
    }
    dump(OUTPUT / "catalog.json", catalog)
    print(f"Wrote {len(grouped)} collections and {len(files)} items to {OUTPUT}")


if __name__ == "__main__":
    main()
