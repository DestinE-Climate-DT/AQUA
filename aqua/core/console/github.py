"""GitHub API helpers for AQUA's console."""

import os
from collections import Counter, defaultdict
from contextlib import ExitStack, contextmanager, nullcontext
from functools import wraps
from pathlib import PurePosixPath
from threading import Lock
from unittest.mock import patch
from urllib.parse import quote

import requests
from urllib3.connectionpool import HTTPConnectionPool


class GitHubAPIRequestTracker:
    """Track GitHub API quota headers while a catalog operation runs.

    Entering the context temporarily wraps urllib3 and, when installed, aiohttp
    response handling. Only responses from ``api.github.com`` with a
    ``X-RateLimit-Resource`` of ``core`` or ``graphql`` are counted; this keeps
    unrelated HTTP traffic and GitHub's separate quota resources out of the
    report. Call :meth:`stage` around each operation to attribute responses to
    that phase. Exiting restores the original library methods and logs one
    summary with per-stage details.

    The wrappers are process-wide while active, so the class lock prevents
    overlapping tracker contexts. Keep each context scoped to one synchronous
    command operation; do not nest trackers or use them as concurrent request
    attribution boundaries.
    """

    _active_lock = Lock()

    def __init__(self, logger, label):
        self.logger = logger
        self.label = label
        self._api_counts = Counter()
        self._api_headers = defaultdict(list)
        self._latest_api_headers = {}
        self._counts_lock = Lock()
        self._stage = "unclassified"
        self._patches = ExitStack()

    def __enter__(self):
        if not self._active_lock.acquire(blocking=False):
            raise RuntimeError("A GitHub request tracker is already active in this process")

        installed = False
        try:
            self._install_requests_tracker()
            self._install_aiohttp_tracker()
            installed = True
            return self
        finally:
            if not installed:
                try:
                    self._patches.close()
                finally:
                    self._active_lock.release()

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            try:
                self._patches.close()
            finally:
                self._log_summary()
        finally:
            self._active_lock.release()

    @contextmanager
    def stage(self, name):
        """Attribute requests made inside the context to a named stage."""
        previous_stage = self._stage
        self._stage = name
        try:
            yield
        finally:
            self._stage = previous_stage

    def _record_api_response(self, host, headers):
        if str(host).lower() != "api.github.com" or not headers:
            return

        resource = headers.get("X-RateLimit-Resource")
        if resource not in {"core", "graphql"}:
            return

        observation = {}
        for key in ("used", "remaining", "limit", "cost"):
            value = headers.get(f"X-RateLimit-{key.title()}")
            try:
                observation[key] = int(value) if value is not None else None
            except ValueError:
                observation[key] = None
        with self._counts_lock:
            self._api_counts[(resource, self._stage)] += 1
            self._api_headers[(resource, self._stage)].append(observation)
            self._latest_api_headers[resource] = observation

    def _install_requests_tracker(self):
        original_make_request = HTTPConnectionPool._make_request

        @wraps(original_make_request)
        def tracked_make_request(pool, conn, method, url, *args, **kwargs):
            response = original_make_request(pool, conn, method, url, *args, **kwargs)
            self._record_api_response(pool.host, response.headers)
            return response

        self._patches.enter_context(patch.object(HTTPConnectionPool, "_make_request", tracked_make_request))

    def _install_aiohttp_tracker(self):
        try:
            from aiohttp.client_reqrep import ClientResponse
        except ModuleNotFoundError as exc:
            if exc.name not in {"aiohttp", "aiohttp.client_reqrep"}:
                raise
            return

        original_start = ClientResponse.start

        @wraps(original_start)
        async def tracked_start(response, *args, **kwargs):
            result = await original_start(response, *args, **kwargs)
            self._record_api_response(response.url.host, response.headers)
            return result

        self._patches.enter_context(patch.object(ClientResponse, "start", tracked_start))

    def _log_summary(self):
        with self._counts_lock:
            counts = sorted(self._api_counts.items())
            observations = {key: list(values) for key, values in self._api_headers.items()}
            latest_by_resource = dict(self._latest_api_headers)
        for resource in ("core", "graphql"):
            resource_counts = [(stage, count) for (item_resource, stage), count in counts if item_resource == resource]
            total = sum(count for _, count in resource_counts)
            latest = latest_by_resource.get(resource)
            if latest is not None or resource == "core":
                api_label = "REST core API calls" if resource == "core" else "GraphQL requests"
                usage_label = "used" if resource == "core" else "points used"
                self.logger.info(
                    "GitHub %s for %s: %d; latest %s=%s remaining=%s limit=%s",
                    api_label,
                    self.label,
                    total,
                    usage_label,
                    latest["used"] if latest and latest["used"] is not None else "unknown",
                    latest["remaining"] if latest and latest["remaining"] is not None else "unknown",
                    latest["limit"] if latest and latest["limit"] is not None else "unknown",
                )
            for stage, count in resource_counts:
                headers = observations[(resource, stage)]
                used = [value["used"] for value in headers if value["used"] is not None]
                remaining = [value["remaining"] for value in headers if value["remaining"] is not None]
                limits = sorted({value["limit"] for value in headers if value["limit"] is not None})
                costs = sorted({value["cost"] for value in headers if value["cost"] is not None})
                self.logger.debug(
                    "GitHub %s API details for %s: stage=%s calls=%d used_range=%s..%s "
                    "remaining_range=%s..%s limit=%s cost=%s",
                    resource,
                    self.label,
                    stage,
                    count,
                    min(used) if used else "unknown",
                    max(used) if used else "unknown",
                    min(remaining) if remaining else "unknown",
                    max(remaining) if remaining else "unknown",
                    ",".join(str(limit) for limit in limits) if limits else "unknown",
                    ",".join(str(cost) for cost in costs) if costs else "unknown",
                )


class GitHubCatalogFetcher:
    """Fetch a selected catalog in a small number of authenticated API calls.

    The fetcher first reads repository metadata to resolve its default branch,
    then makes one recursive Git-tree request to list paths and blob object IDs.
    It filters that listing to ``catalogs/<name>/`` and retrieves those blobs
    with aliases in one GraphQL query. This avoids directory-by-directory
    discovery and one request per file. The tree and GraphQL calls consume
    different GitHub quota resources and are tracked separately.

    Use this fetcher only when both a GitHub username and token are available;
    unauthenticated callers should retain the fsspec path. A truncated tree,
    GraphQL errors, missing or non-text blobs, or unsafe relative paths must
    fail rather than silently install incomplete data. Keep the per-blob alias
    order aligned with ``blobs`` in :meth:`fetch_catalog`, since the response
    fields are mapped back to tree paths by index.
    """

    api_url = "https://api.github.com"

    def __init__(self, owner, repo, username, token, request_tracker=None):
        self.owner = owner
        self.repo = repo
        self.username = username
        self.token = token
        self.request_tracker = request_tracker
        self.headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    def fetch_catalog(self, catalog, dest_dir):
        """Download the selected catalog and return all catalog names in the repo."""
        with self._stage("repository default branch"):
            repository_response = requests.get(
                f"{self.api_url}/repos/{self.owner}/{self.repo}",
                headers=self.headers,
                auth=(self.username, self.token),
                timeout=30,
            )
            repository_response.raise_for_status()
            default_branch = repository_response.json()["default_branch"]

        with self._stage("recursive Git tree discovery"):
            tree_response = requests.get(
                f"{self.api_url}/repos/{self.owner}/{self.repo}/git/trees/{quote(default_branch, safe='')}",
                params={"recursive": "1"},
                headers=self.headers,
                auth=(self.username, self.token),
                timeout=30,
            )
            tree_response.raise_for_status()
            tree = tree_response.json()

        if tree.get("truncated"):
            raise RuntimeError("GitHub returned a truncated repository tree; cannot safely fetch this catalog.")

        entries = tree.get("tree", [])
        available_catalogs = sorted(
            {
                path.split("/", 2)[1]
                for entry in entries
                if entry.get("type") == "tree"
                and (path := entry.get("path", "")).startswith("catalogs/")
                and len(path.split("/", 2)) > 1
            }
        )
        catalog_prefix = f"catalogs/{catalog}/"
        blobs = [
            entry for entry in entries if entry.get("type") == "blob" and entry.get("path", "").startswith(catalog_prefix)
        ]
        if catalog not in available_catalogs:
            return available_catalogs
        if not blobs:
            raise RuntimeError(f"GitHub tree contains catalog {catalog!r} but no files to download.")

        query, variables = self._build_blob_query(blobs)
        with self._stage("GraphQL catalog blob batch"):
            graphql_response = requests.post(
                f"{self.api_url}/graphql",
                json={"query": query, "variables": variables},
                headers={**self.headers, "Authorization": f"Bearer {self.token}"},
                timeout=60,
            )
            graphql_response.raise_for_status()
            payload = graphql_response.json()

        if payload.get("errors"):
            messages = "; ".join(error.get("message", "Unknown GraphQL error") for error in payload["errors"])
            raise RuntimeError(f"GitHub GraphQL catalog batch failed: {messages}")

        repository_data = payload.get("data", {}).get("repository")
        if not isinstance(repository_data, dict):
            raise RuntimeError("GitHub GraphQL response did not contain repository data.")

        files = []
        for index, entry in enumerate(blobs):
            blob = repository_data.get(f"blob{index}")
            if not isinstance(blob, dict) or blob.get("__typename") != "Blob":
                raise RuntimeError(f"GitHub GraphQL response omitted catalog blob {entry['path']!r}.")
            if blob.get("isBinary") or blob.get("isTruncated") or blob.get("text") is None:
                raise RuntimeError(f"GitHub GraphQL cannot return complete text for catalog file {entry['path']!r}.")
            relative_path = self._safe_relative_path(entry["path"], catalog_prefix)
            files.append((relative_path, blob["text"]))

        for relative_path, content in files:
            path = os.path.join(dest_dir, *relative_path.parts)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8", newline="") as file:
                file.write(content)

        return available_catalogs

    def _build_blob_query(self, blobs):
        """Create one GraphQL query with a field alias for every selected blob."""
        declarations = ["$owner: String!", "$name: String!"]
        fields = []
        variables = {"owner": self.owner, "name": self.repo}
        for index, blob in enumerate(blobs):
            variable = f"oid{index}"
            declarations.append(f"${variable}: GitObjectID!")
            fields.append(
                f"blob{index}: object(oid: ${variable}) {{ __typename ... on Blob {{ text isBinary isTruncated }} }}"
            )
            variables[variable] = blob["sha"]
        query = (
            f"query CatalogBlobs({', '.join(declarations)}) "
            "{ repository(owner: $owner, name: $name) { " + " ".join(fields) + " } }"
        )
        return query, variables

    @staticmethod
    def _safe_relative_path(path, prefix):
        if not path.startswith(prefix) or "\\" in path:
            raise RuntimeError(f"GitHub returned an unsafe catalog path: {path!r}.")
        relative_path = PurePosixPath(path[len(prefix) :])
        if (
            not relative_path.parts
            or relative_path.is_absolute()
            or any(part in {"", ".", ".."} for part in relative_path.parts)
        ):
            raise RuntimeError(f"GitHub returned an unsafe catalog path: {path!r}.")
        return relative_path

    def _stage(self, name):
        return self.request_tracker.stage(name) if self.request_tracker else nullcontext()
