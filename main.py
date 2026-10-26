"""
CMS Hospitals Data Pipeline
Discover CMS Provider Data Catalog datasets tagged with the Hospitals theme,
download changed CSV datasets concurrently, normalize headers to snake_case,
and record successful runs in metadata.json.

Compatible with regular Windows and Linux Python installations.
"""

import csv
import json
import logging
import os
import re
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests

BASE_URL = "https://data.cms.gov/provider-data/api/1"
SEARCH_URL = f"{BASE_URL}/search"
METASTORE_URL = f"{BASE_URL}/metastore/schemas/dataset/items"
THEME = "Hospitals"
ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
LOG_DIR = ROOT / "logs"
STATE_FILE = ROOT / "metadata.json"
LOG_FILE = LOG_DIR / "pipeline.log"
MAX_WORKERS = 5
TIMEOUT_SECONDS = 90
USER_AGENT = "cms-hospitals-data-pipeline/1.0 (educational project)"

DATA_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def get_json(url, params=None):
    """GET JSON with a small retry loop."""
    last_error = None
    for attempt in range(3):
        try:
            response = requests.get(
                url,
                params=params,
                timeout=TIMEOUT_SECONDS,
                headers={"User-Agent": USER_AGENT},
            )
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            if attempt < 2:
                time.sleep(2 ** attempt)
    raise RuntimeError(f"Could not retrieve JSON from {url}: {last_error}")


def slugify(value):
    """Make a safe, readable filename."""
    value = str(value or "dataset").strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = value.strip("_")
    return value[:100] or "dataset"


def snake_case(column_name):
    """Convert mixed-case headers, spaces, punctuation, and symbols to snake_case."""
    name = str(column_name).strip()
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    name = name.lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    return name.strip("_")


def load_state():
    if not STATE_FILE.exists():
        return {"datasets": {}}
    try:
        with STATE_FILE.open("r", encoding="utf-8") as file:
            state = json.load(file)
        if not isinstance(state, dict):
            raise ValueError("Metadata root is not an object")
        state.setdefault("datasets", {})
        return state
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        # Do not silently overwrite a corrupt state file.
        raise RuntimeError(
            f"Cannot read {STATE_FILE}. Back it up and repair it before running again."
        ) from exc


def save_state(state):
    """Atomically replace metadata so interrupted writes don't corrupt the state."""
    temporary = STATE_FILE.with_suffix(".tmp")
    with temporary.open("w", encoding="utf-8") as file:
        json.dump(state, file, indent=2, ensure_ascii=False)
        file.write("\n")
    temporary.replace(STATE_FILE)


def extract_results(payload):
    """Support common response shapes returned by catalog search endpoints."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("results", "data", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
    return []


def discover_hospital_datasets():
    """
    Search the Hospitals theme first. If the search response has an unexpected
    shape, fall back to the metastore catalog and filter its theme metadata.
    """
    try:
        payload = get_json(
            SEARCH_URL,
            params={"page-size": 100, "theme": THEME},
        )
        results = extract_results(payload)
        ids = set()
        for item in results:
            if not isinstance(item, dict):
                continue
            identifier = item.get("identifier") or item.get("id")
            if identifier:
                ids.add(str(identifier))
        if ids:
            logger.info("Found %s dataset IDs through the Hospitals search.", len(ids))
            return sorted(ids)
        logger.warning("Search response contained no recognizable dataset IDs.")
    except Exception as exc:
        logger.warning("Hospitals search failed; trying the metastore: %s", exc)

    payload = get_json(METASTORE_URL)
    items = extract_results(payload)
    if not items and isinstance(payload, list):
        items = payload

    ids = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        themes = item.get("theme", [])
        if isinstance(themes, str):
            themes = [themes]
        if any(str(theme).strip().casefold() == THEME.casefold() for theme in themes):
            identifier = item.get("identifier") or item.get("id")
            if identifier:
                ids.add(str(identifier))

    if not ids:
        raise RuntimeError(
            "No Hospitals datasets were found. Check the CMS API response format."
        )
    logger.info("Found %s dataset IDs through the metastore.", len(ids))
    return sorted(ids)


def get_dataset_metadata(dataset_id):
    url = f"{METASTORE_URL}/{dataset_id}"
    payload = get_json(url)
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected metadata response for {dataset_id}")
    return payload


def get_download_url(metadata):
    """Find a CSV download URL from a dataset's distribution metadata."""
    distributions = metadata.get("distribution", [])
    if isinstance(distributions, dict):
        distributions = [distributions]

    candidates = []
    for distribution in distributions:
        if not isinstance(distribution, dict):
            continue
        url = (
            distribution.get("downloadURL")
            or distribution.get("downloadUrl")
            or distribution.get("accessURL")
            or distribution.get("accessUrl")
        )
        media_type = str(distribution.get("mediaType", "")).lower()
        format_name = str(distribution.get("format", "")).lower()
        if url:
            candidates.append((str(url), media_type, format_name))

    for url, media_type, format_name in candidates:
        if ".csv" in url.lower() or "csv" in media_type or "csv" in format_name:
            return url
    if candidates:
        return candidates[0][0]
    return None


def dataset_modified(metadata):
    """Return the source's modification value as a string, if available."""
    for key in ("modified", "metadata_modified", "lastModified", "last_modified"):
        value = metadata.get(key)
        if value:
            return str(value)
    return None


def download_and_normalize(dataset_id, metadata, previous_record):
    """Download one changed dataset and atomically write a normalized CSV."""
    title = metadata.get("title") or metadata.get("name") or dataset_id
    modified = dataset_modified(metadata)
    download_url = get_download_url(metadata)

    if not download_url:
        logger.warning("SKIPPED | %s | no download URL found", title)
        return None

    # Only skip when the catalog provides a modification timestamp and it matches.
    # If it doesn't provide one, download again rather than risk missing a change.
    if (
        modified
        and previous_record
        and previous_record.get("modified") == modified
        and previous_record.get("download_url") == download_url
    ):
        logger.info("UNCHANGED | %s", title)
        return {"status": "unchanged", "dataset_id": dataset_id}

    filename = f"{slugify(title)}_{slugify(dataset_id)}.csv"
    destination = DATA_DIR / filename

    try:
        with requests.get(
            download_url,
            stream=True,
            timeout=TIMEOUT_SECONDS,
            headers={"User-Agent": USER_AGENT},
        ) as response:
            response.raise_for_status()
            with tempfile.NamedTemporaryFile(
                mode="wb", delete=False, dir=DATA_DIR, suffix=".download"
            ) as temp_file:
                temp_path = Path(temp_file.name)
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        temp_file.write(chunk)

        # Rewrite CSV to a temporary output while changing only the header row.
        normalized_temp = destination.with_suffix(".csv.tmp")
        with temp_path.open("r", encoding="utf-8-sig", newline="") as source:
            reader = csv.reader(source)
            try:
                original_headers = next(reader)
            except StopIteration:
                raise ValueError("Downloaded CSV is empty")

            normalized_headers = [snake_case(header) for header in original_headers]
            with normalized_temp.open("w", encoding="utf-8", newline="") as target:
                writer = csv.writer(target)
                writer.writerow(normalized_headers)
                writer.writerows(reader)

        normalized_temp.replace(destination)
        temp_path.unlink(missing_ok=True)

        return {
            "status": "downloaded",
            "dataset_id": dataset_id,
            "title": title,
            "modified": modified,
            "download_url": download_url,
            "filename": filename,
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
            "columns": normalized_headers,
            "row_count": max(0, sum(1 for _ in destination.open(
                "r", encoding="utf-8", newline=""
            )) - 1),
        }
    except Exception:
        # Clean up temporary files; keep prior metadata unchanged so the next run retries.
        try:
            if "temp_path" in locals():
                temp_path.unlink(missing_ok=True)
        except OSError:
            pass
        try:
            if "normalized_temp" in locals():
                normalized_temp.unlink(missing_ok=True)
        except OSError:
            pass
        logger.exception("FAILED | %s (%s)", title, dataset_id)
        return {"status": "failed", "dataset_id": dataset_id, "title": title}


def main():
    started_at = datetime.now(timezone.utc).isoformat()
    logger.info("Starting CMS Hospitals pipeline.")
    state = load_state()
    state.setdefault("datasets", {})
    dataset_ids = discover_hospital_datasets()

    # Fetch metadata first, then process datasets concurrently.
    work = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(get_dataset_metadata, dataset_id): dataset_id
            for dataset_id in dataset_ids
        }
        for future in as_completed(futures):
            dataset_id = futures[future]
            try:
                metadata = future.result()
                work.append((dataset_id, metadata))
            except Exception as exc:
                logger.error("Metadata fetch failed for %s: %s", dataset_id, exc)

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for dataset_id, metadata in work:
            previous = state["datasets"].get(dataset_id)
            futures[executor.submit(
                download_and_normalize, dataset_id, metadata, previous
            )] = dataset_id

        for future in as_completed(futures):
            result = future.result()
            if not result:
                continue
            results.append(result)
            if result["status"] == "downloaded":
                state["datasets"][result["dataset_id"]] = {
                    "title": result["title"],
                    "modified": result["modified"],
                    "download_url": result["download_url"],
                    "filename": result["filename"],
                    "downloaded_at": result["downloaded_at"],
                    "columns": result["columns"],
                    "row_count": result["row_count"],
                }

    # Save only successful downloads; unchanged records remain as they were.
    state["last_run_started_at"] = started_at
    state["last_run_completed_at"] = datetime.now(timezone.utc).isoformat()
    state["last_run_summary"] = {
        "discovered": len(dataset_ids),
        "metadata_retrieved": len(work),
        "downloaded": sum(r["status"] == "downloaded" for r in results),
        "unchanged": sum(r["status"] == "unchanged" for r in results),
        "failed": sum(r["status"] == "failed" for r in results),
    }
    save_state(state)
    logger.info("Run summary: %s", json.dumps(state["last_run_summary"]))
    logger.info("Pipeline finished.")


if __name__ == "__main__":
    main()
