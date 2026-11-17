"""Markdown summary of the package changes between two conda-lock explicit lock files.

Direct dependencies (from environment.yml and pyproject.toml) are listed first,
transitive ones are folded in a <details> block, major version changes are flagged.

Usage:
    python lock_diff.py OLD_LOCK NEW_LOCK [--env environment.yml] [--pyproject pyproject.toml]
"""

import argparse
import re
import tomllib

import yaml

# conda: .../<name>-<version>-<build>.conda#md5 or .tar.bz2#md5
CONDA_RE = re.compile(r"/([^/]+)-([^-/]+)-([^-/]+)\.(?:conda|tar\.bz2)#")
# pip: "# pip <name> @ <url>/<file>#sha256=..."
PIP_RE = re.compile(r"^# pip (\S+) @ \S*/([^/#]+)#")


def normalize(name):
    """PEP 503 name normalization, so conda and pip names compare equal."""
    return re.sub(r"[-_.]+", "-", name).lower()


def parse_lock(path):
    """Return {name: version} for the conda and pip packages of an explicit lock file."""
    packages = {}
    with open(path) as f:
        for line in f:
            if match := CONDA_RE.search(line):
                packages[normalize(match[1])] = match[2]
            elif match := PIP_RE.match(line):
                filename = match[2]
                if filename.endswith(".whl"):  # <name>-<version>-<tags>.whl
                    version = filename.split("-")[1]
                else:  # sdist: <name>-<version>.tar.gz
                    version = re.sub(r"\.(tar\.gz|zip)$", "", filename).rsplit("-", 1)[-1]
                packages[normalize(match[1])] = version
    return packages


def direct_dependencies(env_file, pyproject_file):
    """Names of the packages requested explicitly in environment.yml and pyproject.toml (all extras)."""
    with open(env_file) as f:
        requirements = [dep for dep in yaml.safe_load(f)["dependencies"] if isinstance(dep, str)]
    with open(pyproject_file, "rb") as f:
        project = tomllib.load(f)["project"]
    requirements += project.get("dependencies", [])
    for extra in project.get("optional-dependencies", {}).values():
        requirements += extra
    names = {normalize(re.split(r"[<>=!~\[ ;@]", req, maxsplit=1)[0]) for req in requirements}
    names.discard(normalize(project["name"]))  # self-referencing extras, e.g. aqua-core[fdb] in aqua-core
    return names


def summarize(old, new, direct):
    """Markdown summary of the differences between two {name: version} dicts."""
    rows = {"direct": [], "transitive": []}
    for name in sorted(old.keys() | new.keys()):
        before, after = old.get(name), new.get(name)
        if before == after:
            continue
        if before is None:
            change = f"added `{after}`"
        elif after is None:
            change = f"removed `{before}`"
        else:
            change = f"`{before}` → `{after}`"
            # leading number only: tzdata 2026.6 -> 2026e is not a major change
            if re.match(r"\d*", before)[0] != re.match(r"\d*", after)[0]:
                name += " ⚠️"
        rows["direct" if name.split()[0] in direct else "transitive"].append(f"| {name} | {change} |")

    lines = ["## Lock file changes", ""]
    if not rows["direct"] and not rows["transitive"]:
        return "\n".join(lines + ["No package changed."])
    header = ["| Package | Change |", "|---|---|"]
    lines.append(
        f"{len(rows['direct'])} direct and {len(rows['transitive'])} transitive dependencies changed. "
        "⚠️ = major version change."
    )
    if rows["direct"]:
        lines += ["", "### Direct dependencies", "", *header, *rows["direct"]]
    if rows["transitive"]:
        lines += [
            "",
            f"<details><summary>Transitive dependencies ({len(rows['transitive'])})</summary>",
            "",
            *header,
            *rows["transitive"],
            "",
            "</details>",
        ]
    return "\n".join(lines)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("old_lock")
    parser.add_argument("new_lock")
    parser.add_argument("--env", default="environment.yml")
    parser.add_argument("--pyproject", default="pyproject.toml")
    args = parser.parse_args()
    print(summarize(parse_lock(args.old_lock), parse_lock(args.new_lock), direct_dependencies(args.env, args.pyproject)))
