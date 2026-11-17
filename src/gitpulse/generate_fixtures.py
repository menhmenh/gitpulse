"""
Generates synthetic data fixtures and manifests for GitPulse offline verification.
Creates 2 repositories across 3 ingestion batches, plus negative corruption test cases.
"""

import copy
import json
from pathlib import Path
from typing import Any, Dict, List

from gitpulse.manifest import create_manifest, save_manifest, compute_sha256


REPO_SPARK = "apache/spark"
REPO_ICEBERG = "apache/iceberg"

FIXTURES_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "fixtures"


def _make_run(
    run_id: int,
    name: str,
    workflow_id: int,
    created_at: str,
    updated_at: str,
    status: str,
    conclusion: Any,
    run_attempt: int = 1,
    event: str = "push",
    head_branch: str = "main",
) -> Dict[str, Any]:
    return {
        "id": run_id,
        "name": name,
        "workflow_id": workflow_id,
        "created_at": created_at,
        "updated_at": updated_at,
        "status": status,
        "conclusion": conclusion,
        "run_attempt": run_attempt,
        "event": event,
        "head_branch": head_branch,
    }


def build_batch_1() -> Dict[str, List[Dict[str, Any]]]:
    """
    Batch 1: Initial baseline.
    - apache/spark: runs 101 (completed), 102 (in_progress), 103 (failed)
    - apache/iceberg: runs 101 (completed - identical ID to spark!), 201 (completed)
    """
    spark_runs = [
        _make_run(101, "CI Build", 1, "2026-10-01T10:00:00Z", "2026-10-01T10:15:00Z", "completed", "success"),
        _make_run(102, "Integration Tests", 2, "2026-10-01T11:00:00Z", "2026-10-01T11:05:00Z", "in_progress", None),
        _make_run(103, "Lint", 3, "2026-10-01T12:00:00Z", "2026-10-01T12:10:00Z", "completed", "failure"),
    ]

    iceberg_runs = [
        # Note: Same ID 101 as spark run, tests multi-repo key isolation
        _make_run(101, "Release Build", 10, "2026-10-01T09:00:00Z", "2026-10-01T09:20:00Z", "completed", "success"),
        _make_run(201, "CI Build", 10, "2026-10-01T14:00:00Z", "2026-10-01T14:30:00Z", "completed", "success"),
    ]

    return {REPO_SPARK: spark_runs, REPO_ICEBERG: iceberg_runs}


def build_batch_2() -> Dict[str, List[Dict[str, Any]]]:
    """
    Batch 2: Updates and Late-Arriving Data.
    - apache/spark:
        * Run 102 updated: in_progress -> completed (success) with later updated_at
        * Run 99: Late-arriving run belonging to earlier date 2026-09-30 (completed failure)
        * Run 104: New run on 2026-10-02 (completed failure)
    - apache/iceberg:
        * Run 202: New run on 2026-10-02 (completed success)
    """
    spark_runs = [
        _make_run(102, "Integration Tests", 2, "2026-10-01T11:00:00Z", "2026-10-01T11:45:00Z", "completed", "success"),
        _make_run(99, "Nightly Security Scan", 4, "2026-09-30T23:50:00Z", "2026-10-02T01:00:00Z", "completed", "failure"),
        _make_run(104, "CI Build", 1, "2026-10-02T08:00:00Z", "2026-10-02T08:10:00Z", "completed", "failure"),
    ]

    iceberg_runs = [
        _make_run(202, "CI Build", 10, "2026-10-02T09:00:00Z", "2026-10-02T09:25:00Z", "completed", "success"),
    ]

    return {REPO_SPARK: spark_runs, REPO_ICEBERG: iceberg_runs}


def build_batch_3_replay() -> Dict[str, List[Dict[str, Any]]]:
    """
    Batch 3: Idempotency Replay & State Protection.
    - Exactly identical items as Batch 2 (must not create duplicate records on replay).
    - Includes an attempted replay of OLD state for Run 102 (status: in_progress, older updated_at)
      to prove old state does NOT overwrite newer state.
    """
    batch2 = build_batch_2()
    replay = copy.deepcopy(batch2)
    # Append stale attempt for Run 102
    replay[REPO_SPARK].append(
        _make_run(102, "Integration Tests", 2, "2026-10-01T11:00:00Z", "2026-10-01T11:05:00Z", "in_progress", None)
    )
    return replay


def write_batch_files(
    batch_name: str,
    batch_data: Dict[str, List[Dict[str, Any]]],
    run_ts: str,
    since: str,
    until: str,
    base_dir: Path,
) -> Dict[str, Any]:
    batch_dir = base_dir / batch_name
    batch_dir.mkdir(parents=True, exist_ok=True)
    batch_manifest = {
        "batch_name": batch_name,
        "run_ts": run_ts,
        "repos": [],
    }

    for repo, runs in batch_data.items():
        safe_repo = repo.replace("/", "_")
        repo_dir = batch_dir / safe_repo / f"run_ts={run_ts}"
        repo_dir.mkdir(parents=True, exist_ok=True)

        # Write page_0.json wrapped in envelope with deterministic indent
        page_content = {"total_count": len(runs), "workflow_runs": runs}
        page_bytes = json.dumps(page_content, indent=2, sort_keys=True).encode("utf-8")
        page_file = repo_dir / "page_0.json"
        with open(page_file, "wb") as f:
            f.write(page_bytes)

        page_checksum = compute_sha256(page_bytes)

        manifest = create_manifest(
            repo=repo,
            resource_type="workflow_runs",
            run_ts=run_ts,
            since=since,
            until=until,
            pages_data=[page_content],
            s3_key_prefix=f"raw/github/workflow_runs/{safe_repo}/run_ts={run_ts}/",
            page_checksums=[page_checksum],
        )
        manifest_path = repo_dir / "manifest.json"
        save_manifest(manifest, manifest_path)

        batch_manifest["repos"].append({
            "repo": repo,
            "manifest_path": str(manifest_path.relative_to(base_dir)).replace("\\", "/"),
            "run_dir": str(repo_dir.relative_to(base_dir)).replace("\\", "/"),
            "record_count": len(runs),
        })

    with open(batch_dir / "batch_manifest.json", "w", encoding="utf-8") as f:
        json.dump(batch_manifest, f, indent=2)

    return batch_manifest


def generate_all_fixtures(output_dir: Path = FIXTURES_DIR):
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Batch 1
    write_batch_files(
        batch_name="batch_1",
        batch_data=build_batch_1(),
        run_ts="20261001T150000Z",
        since="2026-10-01T00:00:00Z",
        until="2026-10-01T15:00:00Z",
        base_dir=output_dir,
    )

    # 2. Batch 2
    write_batch_files(
        batch_name="batch_2",
        batch_data=build_batch_2(),
        run_ts="20261002T120000Z",
        since="2026-10-01T15:00:00Z",
        until="2026-10-02T12:00:00Z",
        base_dir=output_dir,
    )

    # 3. Batch 3 Replay
    write_batch_files(
        batch_name="batch_3_replay",
        batch_data=build_batch_3_replay(),
        run_ts="20261002T120000Z",
        since="2026-10-01T15:00:00Z",
        until="2026-10-02T12:00:00Z",
        base_dir=output_dir,
    )

    # 4. Generate Corrupted Scenarios for Contract Testing
    corr_dir = output_dir / "corruptions"
    corr_dir.mkdir(parents=True, exist_ok=True)

    # 4.1 Missing Page
    missing_dir = corr_dir / "missing_page"
    missing_dir.mkdir(parents=True, exist_ok=True)
    page_0_content = {"total_count": 5, "workflow_runs": []}
    page_0_bytes = json.dumps(page_0_content, indent=2, sort_keys=True).encode("utf-8")
    with open(missing_dir / "page_0.json", "wb") as f:
        f.write(page_0_bytes)
    m_manifest = {
        "manifest_version": "1.0",
        "repo": "apache/spark",
        "resource_type": "workflow_runs",
        "run_ts": "20261001T000000Z",
        "total_records": 10,
        "pages": [
            {"page_index": 0, "filename": "page_0.json", "record_count": 5, "checksum_sha256": compute_sha256(page_0_bytes)},
            {"page_index": 1, "filename": "page_1.json", "record_count": 5, "checksum_sha256": "fake"}
        ],
    }
    with open(missing_dir / "manifest.json", "w", encoding="utf-8") as f:
        json.dump(m_manifest, f, indent=2)

    # 4.2 Checksum Mismatch
    chk_dir = corr_dir / "checksum_mismatch"
    chk_dir.mkdir(parents=True, exist_ok=True)
    chk_content = json.dumps({"total_count": 1, "workflow_runs": [_make_run(1, "Test", 1, "2026-10-01T00:00:00Z", "2026-10-01T00:00:00Z", "completed", "success")]})
    with open(chk_dir / "page_0.json", "w", encoding="utf-8") as f:
        f.write(chk_content)
    with open(chk_dir / "manifest.json", "w", encoding="utf-8") as f:
        json.dump({
            "manifest_version": "1.0",
            "repo": "apache/spark",
            "resource_type": "workflow_runs",
            "run_ts": "20261001T000000Z",
            "total_records": 1,
            "pages": [{"page_index": 0, "filename": "page_0.json", "record_count": 1, "checksum_sha256": "0000000000000000000000000000000000000000000000000000000000000000"}],
        }, f, indent=2)

    # 4.3 Malformed JSON
    bad_json_dir = corr_dir / "malformed_json"
    bad_json_dir.mkdir(parents=True, exist_ok=True)
    with open(bad_json_dir / "page_0.json", "w", encoding="utf-8") as f:
        f.write('{"total_count": 1, "workflow_runs": [ broken json syntax')
    bad_chk = compute_sha256('{"total_count": 1, "workflow_runs": [ broken json syntax')
    with open(bad_json_dir / "manifest.json", "w", encoding="utf-8") as f:
        json.dump({
            "manifest_version": "1.0",
            "repo": "apache/spark",
            "resource_type": "workflow_runs",
            "run_ts": "20261001T000000Z",
            "total_records": 1,
            "pages": [{"page_index": 0, "filename": "page_0.json", "record_count": 1, "checksum_sha256": bad_chk}],
        }, f, indent=2)

    # 4.4 Missing Mandatory Keys
    miss_key_dir = corr_dir / "missing_key"
    miss_key_dir.mkdir(parents=True, exist_ok=True)
    # Record has no 'id' and no 'status'
    bad_rec = {"name": "No ID Run", "created_at": "2026-10-01T00:00:00Z"}
    bad_rec_page = {"total_count": 1, "workflow_runs": [bad_rec]}
    bad_rec_bytes = json.dumps(bad_rec_page, sort_keys=True).encode("utf-8")
    with open(miss_key_dir / "page_0.json", "wb") as f:
        f.write(bad_rec_bytes)
    with open(miss_key_dir / "manifest.json", "w", encoding="utf-8") as f:
        json.dump({
            "manifest_version": "1.0",
            "repo": "apache/spark",
            "resource_type": "workflow_runs",
            "run_ts": "20261001T000000Z",
            "total_records": 1,
            "pages": [{"page_index": 0, "filename": "page_0.json", "record_count": 1, "checksum_sha256": compute_sha256(bad_rec_bytes)}],
        }, f, indent=2)

    # 4.5 Invalid Timestamp
    inv_ts_dir = corr_dir / "invalid_timestamp"
    inv_ts_dir.mkdir(parents=True, exist_ok=True)
    bad_ts_rec = {"id": 999, "status": "completed", "created_at": "not-a-valid-date-time"}
    bad_ts_page = {"total_count": 1, "workflow_runs": [bad_ts_rec]}
    bad_ts_bytes = json.dumps(bad_ts_page, sort_keys=True).encode("utf-8")
    with open(inv_ts_dir / "page_0.json", "wb") as f:
        f.write(bad_ts_bytes)
    with open(inv_ts_dir / "manifest.json", "w", encoding="utf-8") as f:
        json.dump({
            "manifest_version": "1.0",
            "repo": "apache/spark",
            "resource_type": "workflow_runs",
            "run_ts": "20261001T000000Z",
            "total_records": 1,
            "pages": [{"page_index": 0, "filename": "page_0.json", "record_count": 1, "checksum_sha256": compute_sha256(bad_ts_bytes)}],
        }, f, indent=2)

    # Write expected_results.json for test assertions and verification
    expected = {
        "batch_1": {
            "spark_records": 3,
            "iceberg_records": 2,
            "total_unique_records": 5,
            "spark_run_102_status": "in_progress",
            "iceberg_has_run_101": True,
            "spark_has_run_101": True,
        },
        "batch_2": {
            "spark_records": 5,
            "iceberg_records": 3,
            "total_unique_records": 8,
            "spark_run_102_status": "completed",
            "spark_run_99_partition_dt": "2026-09-30",
            "spark_run_104_partition_dt": "2026-10-02",
        },
        "batch_3_replay": {
            "total_unique_records": 8,
            "spark_run_102_status": "completed",  # Stale in_progress was rejected
        },
    }
    with open(output_dir / "expected_results.json", "w", encoding="utf-8") as f:
        json.dump(expected, f, indent=2)

    print(f"Fixtures successfully generated under {output_dir}")


if __name__ == "__main__":
    generate_all_fixtures()
