"""
GitPulse Universal CLI Entrypoint
=================================
Unified CLI for both local (Docker/Hadoop catalog) and cloud (AWS Glue Spark / Glue Catalog).
Returns standard exit codes:
  0: Success
  1: Data contract / manifest / checksum validation failure
  2: Pipeline execution or Spark runtime error
"""

import argparse
import json
import os
from pathlib import Path
import sys
import time
from typing import List

from gitpulse.analytics import (
    create_reliability_view,
    query_7_day_failure_trend,
    query_least_stable_workflows,
    query_snapshot_time_travel_comparison,
)
from gitpulse.manifest import ManifestValidationError
from gitpulse.pipeline import IcebergPipeline
from gitpulse.spark_session import get_spark_session


def parse_args(args: List[str] = None):
    parser = argparse.ArgumentParser(description="GitPulse Apache Iceberg Pipeline CLI")
    parser.add_argument(
        "--manifest",
        type=str,
        help="Path to manifest.json or batch_manifest.json to ingest",
    )
    parser.add_argument(
        "--catalog",
        type=str,
        default="local",
        help="Spark catalog name (default: 'local')",
    )
    parser.add_argument(
        "--catalog-type",
        type=str,
        choices=["hadoop", "glue"],
        default="hadoop",
        help="Catalog backend type: 'hadoop' for local disk/volume, 'glue' for AWS Glue Catalog",
    )
    parser.add_argument(
        "--warehouse",
        type=str,
        default=None,
        help="Warehouse path (directory on disk or s3:// bucket URI)",
    )
    parser.add_argument(
        "--database",
        type=str,
        default="gitpulse",
        help="Target database name (default: 'gitpulse')",
    )
    parser.add_argument(
        "--action",
        type=str,
        choices=["ingest", "queries", "time-travel", "migrate", "demo"],
        default="ingest",
        help="Action to perform: 'ingest', 'queries', 'time-travel', 'migrate', or 'demo'",
    )
    return parser.parse_args(args)


def run_cli(args: List[str] = None) -> int:
    parsed = parse_args(args)

    print(json.dumps({
        "event": "cli_start",
        "action": parsed.action,
        "catalog": parsed.catalog,
        "catalog_type": parsed.catalog_type,
        "manifest": parsed.manifest,
    }))

    try:
        # Initialize Spark session
        spark = get_spark_session(
            catalog_name=parsed.catalog,
            warehouse_path=parsed.warehouse,
            catalog_type=parsed.catalog_type,
        )

        pipeline = IcebergPipeline(
            spark=spark,
            catalog=parsed.catalog,
            database=parsed.database,
        )

        if parsed.action == "ingest":
            if not parsed.manifest:
                print("Error: --manifest argument is required for 'ingest' action", file=sys.stderr)
                return 1

            manifest_path = Path(parsed.manifest)
            if not manifest_path.is_file():
                print(f"Error: Manifest file not found: {manifest_path}", file=sys.stderr)
                return 1

            # Check if this is a batch manifest or a single manifest
            with open(manifest_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            if "repos" in data and isinstance(data["repos"], list):
                # Batch manifest
                base_dir = manifest_path.parent
                for item in data["repos"]:
                    single_m_path = base_dir.parent / item["manifest_path"]
                    print(f"Processing batch item for repo: {item['repo']} -> {single_m_path}")
                    report = pipeline.run_batch_manifest(single_m_path)
                    print(json.dumps(report, indent=2))
            else:
                report = pipeline.run_batch_manifest(manifest_path)
                print(json.dumps(report, indent=2))

            return 0

        elif parsed.action == "migrate":
            pipeline.apply_schema_migration_add_head_branch()
            print("Successfully verified/applied schema migration (head_branch nullable column)")
            return 0

        elif parsed.action == "queries":
            create_reliability_view(spark, table_name=pipeline.full_table_name)
            print("\n=== 1. 7-Day Failure Trend ===")
            trend_df = query_7_day_failure_trend(spark)
            trend_df.show(truncate=False)

            print("\n=== 2. Least Stable / Flakiest Workflows ===")
            flaky_df = query_least_stable_workflows(spark)
            flaky_df.show(truncate=False)
            return 0

        elif parsed.action == "time-travel":
            comparison = query_snapshot_time_travel_comparison(spark, table_name=pipeline.full_table_name)
            print(json.dumps(comparison, indent=2, default=str))
            return 0

        elif parsed.action == "demo":
            # Run the complete demonstration workflow: Batch 1 -> Snapshot 1 -> Batch 2 -> Snapshot 2 -> Queries -> Replay
            fixtures_root = Path(__file__).resolve().parent.parent.parent / "data" / "fixtures"
            batch_1_manifest = fixtures_root / "batch_1" / "batch_manifest.json"
            batch_2_manifest = fixtures_root / "batch_2" / "batch_manifest.json"
            batch_3_manifest = fixtures_root / "batch_3_replay" / "batch_manifest.json"

            print("\n>>> STEP 1: Ingesting Batch 1 (Initial Baseline)...")
            run_cli(["--manifest", str(batch_1_manifest), "--action", "ingest", "--catalog", parsed.catalog])
            snap_1 = pipeline.get_current_snapshot_id()
            print(f"Committed Snapshot 1: {snap_1}")

            print("\n>>> STEP 2: Ingesting Batch 2 (Updates & Late-Arriving Records)...")
            run_cli(["--manifest", str(batch_2_manifest), "--action", "ingest", "--catalog", parsed.catalog])
            snap_2 = pipeline.get_current_snapshot_id()
            print(f"Committed Snapshot 2: {snap_2}")

            print("\n>>> STEP 3: Executing Iceberg Time-Travel Comparison Query...")
            comparison = query_snapshot_time_travel_comparison(
                spark,
                table_name=pipeline.full_table_name,
                snapshot_before=snap_1,
                snapshot_after=snap_2,
            )
            print(json.dumps(comparison, indent=2, default=str))

            print("\n>>> STEP 4: Ingesting Batch 3 (Replay Idempotency Verification)...")
            run_cli(["--manifest", str(batch_3_manifest), "--action", "ingest", "--catalog", parsed.catalog])
            count_after_replay = spark.table(pipeline.full_table_name).count()
            print(f"Record count after replay: {count_after_replay} (Must be identical to Batch 2: 8)")

            print("\n>>> STEP 5: Running Analytical Reliability Queries...")
            create_reliability_view(spark, table_name=pipeline.full_table_name)
            query_7_day_failure_trend(spark).show(truncate=False)
            query_least_stable_workflows(spark).show(truncate=False)

            print("\n>>> DEMO COMPLETED SUCCESSFULLY!")
            return 0

    except ManifestValidationError as e:
        print(f"Data Contract / Manifest Validation Error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Pipeline Execution Error: {e}", file=sys.stderr)
        return 2


def main():
    sys.exit(run_cli(sys.argv[1:]))


if __name__ == "__main__":
    main()
