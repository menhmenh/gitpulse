"""
GitPulse Manifest Specification and Data Quality Validator
==========================================================
Enforces ingestion boundaries, file completeness, cryptographic checksums,
and record-level quality assertions before any downstream table merge.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union


class ManifestValidationError(Exception):
    """Base exception for all manifest and data contract validation errors."""
    pass


class MissingPageError(ManifestValidationError):
    """Raised when a page file listed in the manifest cannot be found."""
    pass


class ChecksumMismatchError(ManifestValidationError):
    """Raised when a file's computed SHA-256 does not match the manifest checksum."""
    pass


class InvalidJsonError(ManifestValidationError):
    """Raised when a raw page file contains malformed or unparseable JSON."""
    pass


class MissingRequiredKeysError(ManifestValidationError):
    """Raised when a record in a page is missing required primary or audit keys."""
    pass


class RecordCountMismatchError(ManifestValidationError):
    """Raised when the number of records found does not match the manifest's declared count."""
    pass


class InvalidTimestampError(ManifestValidationError):
    """Raised when an essential date/timestamp field cannot be parsed into UTC ISO format."""
    pass


# Mandatory fields by resource type for schema enforcement before merge
REQUIRED_FIELDS = {
    "workflow_runs": {"id", "created_at", "status"},
    "repository": {"id", "name"},
    "commits": {"sha"},
    "issues": {"id", "created_at", "state"},
    "pull_requests": {"id", "created_at", "state"},
    "releases": {"id", "published_at"},
    "contributors": {"id"},
    "deployments": {"id", "created_at"},
}


@dataclass
class PageInfo:
    page_index: int
    filename: str
    record_count: int
    checksum_sha256: str
    s3_key: Optional[str] = None


@dataclass
class IngestionManifest:
    repo: str
    resource_type: str
    run_ts: str
    since: Optional[str]
    until: Optional[str]
    total_records: int
    pages: List[PageInfo]
    manifest_version: str = "1.0"
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "manifest_version": self.manifest_version,
            "repo": self.repo,
            "resource_type": self.resource_type,
            "run_ts": self.run_ts,
            "since": self.since,
            "until": self.until,
            "total_records": self.total_records,
            "page_count": len(self.pages),
            "pages": [
                {
                    "page_index": p.page_index,
                    "filename": p.filename,
                    "record_count": p.record_count,
                    "checksum_sha256": p.checksum_sha256,
                    "s3_key": p.s3_key,
                }
                for p in self.pages
            ],
            "created_at": self.created_at,
        }


def compute_sha256(content: Union[bytes, str]) -> str:
    """Computes standard SHA-256 hex digest for byte or string content."""
    if isinstance(content, str):
        content = content.encode("utf-8")
    return hashlib.sha256(content).hexdigest()


def create_manifest(
    repo: str,
    resource_type: str,
    run_ts: str,
    since: Optional[str],
    until: Optional[str],
    pages_data: List[Union[Dict[str, Any], List[Any]]],
    s3_key_prefix: Optional[str] = None,
    page_checksums: Optional[List[str]] = None,
) -> IngestionManifest:
    """
    Creates an IngestionManifest from raw in-memory page structures.
    Calculates checksums and record counts for each page.
    """
    envelope_key = "workflow_runs" if resource_type == "workflow_runs" else None
    pages: List[PageInfo] = []
    total_records = 0

    for idx, page in enumerate(pages_data):
        if page_checksums and idx < len(page_checksums):
            checksum = page_checksums[idx]
        else:
            page_bytes = json.dumps(page, indent=2, sort_keys=True).encode("utf-8")
            checksum = compute_sha256(page_bytes)

        if envelope_key and isinstance(page, dict):
            items = page.get(envelope_key, [])
        elif isinstance(page, list):
            items = page
        else:
            items = [page]

        rec_count = len(items)
        total_records += rec_count
        filename = f"page_{idx}.json"
        s3_key = f"{s3_key_prefix}{filename}" if s3_key_prefix else None

        pages.append(
            PageInfo(
                page_index=idx,
                filename=filename,
                record_count=rec_count,
                checksum_sha256=checksum,
                s3_key=s3_key,
            )
        )

    return IngestionManifest(
        repo=repo,
        resource_type=resource_type,
        run_ts=run_ts,
        since=since,
        until=until,
        total_records=total_records,
        pages=pages,
    )


def save_manifest(manifest: Union[IngestionManifest, Dict[str, Any]], filepath: Union[str, Path]) -> Path:
    """Writes a manifest to disk as a formatted JSON document."""
    path = Path(filepath)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = manifest.to_dict() if isinstance(manifest, IngestionManifest) else manifest
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)
    return path


def load_manifest(source: Union[str, Path, Dict[str, Any]]) -> Dict[str, Any]:
    """Loads a manifest from a file path or returns dictionary directly."""
    if isinstance(source, dict):
        return source
    path = Path(source)
    if not path.is_file():
        raise FileNotFoundError(f"Manifest file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def validate_manifest(
    manifest_source: Union[str, Path, Dict[str, Any]],
    base_dir: Optional[Union[str, Path]] = None,
    enforce_required_fields: bool = True,
) -> Dict[str, Any]:
    """
    Validates manifest integrity, file checksums, JSON structure, and record contracts.
    
    Checks:
    1. All listed pages exist in base_dir.
    2. Checksum of each page matches SHA-256 in manifest.
    3. JSON content is well-formed.
    4. Mandatory fields exist in all records (e.g. id, created_at, status).
    5. Timestamps are valid parseable dates.
    6. Total record count equals sum of page records.

    Raises specific ManifestValidationError if any check fails.
    Returns audit summary dict on success.
    """
    if isinstance(manifest_source, (str, Path)):
        manifest_path = Path(manifest_source)
        manifest = load_manifest(manifest_path)
        if base_dir is None:
            base_dir = manifest_path.parent
    else:
        manifest = manifest_source
        if base_dir is None:
            raise ValueError("base_dir must be provided when manifest_source is a dict")

    base_path = Path(base_dir)
    resource_type = manifest.get("resource_type", "workflow_runs")
    repo = manifest.get("repo", "unknown")
    pages = manifest.get("pages", [])
    expected_total_records = manifest.get("total_records", 0)

    envelope_key = "workflow_runs" if resource_type == "workflow_runs" else None
    required_keys = REQUIRED_FIELDS.get(resource_type, set()) if enforce_required_fields else set()

    verified_records = 0
    pages_inspected = 0

    # 1. Phase 1: Verify all page files exist before inspecting content
    for page_info in pages:
        filename = page_info.get("filename", f"page_{page_info.get('page_index', 0)}.json")
        page_file = base_path / filename
        if not page_file.is_file():
            raise MissingPageError(f"Page file '{filename}' listed in manifest does not exist in {base_path}")

    # 2. Phase 2: Checksum, JSON validity, record counts, and schema contracts
    for page_info in pages:
        filename = page_info.get("filename", f"page_{page_info.get('page_index', 0)}.json")
        expected_checksum = page_info.get("checksum_sha256")
        expected_page_count = page_info.get("record_count")
        page_file = base_path / filename

        try:
            file_bytes = page_file.read_bytes()
        except Exception as e:
            raise ManifestValidationError(f"Failed to read page file '{filename}': {e}")

        actual_checksum = compute_sha256(file_bytes)
        if expected_checksum and actual_checksum.lower() != expected_checksum.lower():
            raise ChecksumMismatchError(
                f"Checksum mismatch for '{filename}': expected {expected_checksum}, got {actual_checksum}"
            )

        # 2. JSON well-formedness verification
        try:
            data = json.loads(file_bytes.decode("utf-8"))
        except Exception as e:
            raise InvalidJsonError(f"Malformed JSON in page file '{filename}': {e}")

        # 3. Extract records and verify record count
        if envelope_key and isinstance(data, dict):
            items = data.get(envelope_key, [])
            if not isinstance(items, list):
                raise InvalidJsonError(f"Envelope key '{envelope_key}' in '{filename}' is not a list")
        elif isinstance(data, list):
            items = data
        else:
            items = [data]

        if expected_page_count is not None and len(items) != expected_page_count:
            raise RecordCountMismatchError(
                f"Record count mismatch in '{filename}': manifest declared {expected_page_count}, found {len(items)}"
            )

        # 4. Mandatory fields and timestamp verification
        for item_idx, item in enumerate(items):
            if not isinstance(item, dict):
                raise InvalidJsonError(f"Record #{item_idx} in '{filename}' is not a JSON object")

            missing = required_keys - set(item.keys())
            if missing:
                raise MissingRequiredKeysError(
                    f"Record #{item_idx} in '{filename}' (ID: {item.get('id', 'N/A')}) is missing required keys: {missing}"
                )

            # Timestamp check
            if "created_at" in item and item["created_at"] is not None:
                ts_str = str(item["created_at"]).replace("Z", "+00:00")
                try:
                    datetime.fromisoformat(ts_str)
                except Exception:
                    raise InvalidTimestampError(
                        f"Record #{item_idx} in '{filename}' has invalid ISO timestamp: {item['created_at']}"
                    )

        verified_records += len(items)
        pages_inspected += 1

    if expected_total_records is not None and verified_records != expected_total_records:
        raise RecordCountMismatchError(
            f"Total records mismatch: manifest declared {expected_total_records}, total verified was {verified_records}"
        )

    return {
        "valid": True,
        "repo": repo,
        "resource_type": resource_type,
        "run_ts": manifest.get("run_ts"),
        "pages_verified": pages_inspected,
        "records_verified": verified_records,
        "checksums_verified": True,
    }


def validate_batch(
    manifest_sources: List[Union[str, Path, Dict[str, Any]]],
    base_dirs: Optional[List[Union[str, Path]]] = None,
) -> List[Dict[str, Any]]:
    """
    Atomically validates a batch of manifests across multiple repositories or runs.
    If ANY manifest fails, an exception is raised immediately, stopping the entire batch.
    """
    results = []
    for idx, source in enumerate(manifest_sources):
        b_dir = base_dirs[idx] if base_dirs and idx < len(base_dirs) else None
        report = validate_manifest(source, base_dir=b_dir)
        results.append(report)
    return results
