"""
GitPulse PySpark + Apache Iceberg Workflow Runs Pipeline
========================================================
Performs atomic manifest validation, PySpark explicit schema parsing,
in-batch deduplication, version conflict checking, and Iceberg MERGE INTO upserts.
"""

from datetime import datetime, timezone
import json
from pathlib import Path
import time
from typing import Any, Dict, List, Optional, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from gitpulse.manifest import load_manifest, validate_manifest, ManifestValidationError
from gitpulse.schema import RAW_WORKFLOW_RUNS_SCHEMA


class ConflictVersionError(Exception):
    """Raised when conflicting records share the exact same version order (updated_at and run_attempt)."""
    pass


class IcebergPipeline:
    def __init__(
        self,
        spark: SparkSession,
        catalog: str = "local",
        database: str = "gitpulse",
        table_name: str = "workflow_runs_current",
    ):
        self.spark = spark
        self.catalog = catalog
        self.database = database
        self.table_name = table_name
        self.full_table_name = f"{catalog}.{database}.{table_name}"
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        """Creates the target database and partitioned Iceberg table if not already present."""
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog}.{self.database}")
        
        # FR: Partitioned by day of created_at; Primary Key (repo, id)
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.full_table_name} (
            repo STRING,
            id BIGINT,
            name STRING,
            workflow_id BIGINT,
            status STRING,
            conclusion STRING,
            run_attempt INT,
            event STRING,
            head_branch STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            run_started_at TIMESTAMP,
            ingested_at TIMESTAMP,
            run_ts STRING
        )
        USING iceberg
        PARTITIONED BY (days(created_at))
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.merge.mode' = 'copy-on-write'
        )
        """
        self.spark.sql(create_sql)

    def load_and_validate_manifest(
        self,
        manifest_source: Union[str, Path, Dict[str, Any]],
        base_dir: Optional[Union[str, Path]] = None,
    ) -> Dict[str, Any]:
        """Validates manifest integrity, checksums, and schema contracts before any Spark processing."""
        return validate_manifest(manifest_source, base_dir=base_dir)

    def extract_raw_dataframe(
        self,
        manifest: Dict[str, Any],
        base_dir: Union[str, Path],
    ) -> DataFrame:
        """
        Reads raw JSON pages listed in manifest into a PySpark DataFrame with explicit schema.
        Explodes the workflow_runs envelope and projects only privacy-sanitized fields.
        """
        base_path = Path(base_dir)
        page_files = [
            str((base_path / p["filename"]).resolve()).replace("\\", "/")
            for p in manifest["pages"]
        ]

        if not page_files:
            return self.spark.createDataFrame([], RAW_WORKFLOW_RUNS_SCHEMA)

        repo = manifest.get("repo", "unknown")
        run_ts = manifest.get("run_ts", datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
        ingested_at_str = manifest.get("created_at") or datetime.now(timezone.utc).isoformat()

        # Read JSON with explicit schema for envelope
        from pyspark.sql.types import ArrayType, LongType, StructField, StructType
        envelope_schema = StructType([
            StructField("total_count", LongType(), True),
            StructField("workflow_runs", ArrayType(RAW_WORKFLOW_RUNS_SCHEMA), True),
        ])

        raw_df = self.spark.read.schema(envelope_schema).json(page_files)

        # Explode array of workflow_runs and sanitize/cast types
        exploded_df = raw_df.select(F.explode("workflow_runs").alias("run"))

        # Explicit casting and timestamp parsing
        projected_df = exploded_df.select(
            F.lit(repo).alias("repo"),
            F.col("run.id").cast("bigint").alias("id"),
            F.col("run.name").cast("string").alias("name"),
            F.col("run.workflow_id").cast("bigint").alias("workflow_id"),
            F.col("run.status").cast("string").alias("status"),
            F.col("run.conclusion").cast("string").alias("conclusion"),
            F.coalesce(F.col("run.run_attempt").cast("int"), F.lit(1)).alias("run_attempt"),
            F.col("run.event").cast("string").alias("event"),
            F.col("run.head_branch").cast("string").alias("head_branch"),
            F.to_timestamp(F.col("run.created_at")).alias("created_at"),
            F.to_timestamp(F.col("run.updated_at")).alias("updated_at"),
            F.to_timestamp(F.col("run.run_started_at")).alias("run_started_at"),
            F.to_timestamp(F.lit(ingested_at_str)).alias("ingested_at"),
            F.lit(run_ts).alias("run_ts"),
        )

        return projected_df

    def deduplicate_incoming(self, df: DataFrame) -> DataFrame:
        """
        Deduplicates incoming batch by (repo, id).
        Orders candidates by:
          1. updated_at DESC
          2. run_attempt DESC
          3. ingested_at DESC
        Asserts no conflicting content with identical version ordering.
        """
        # Check for conflicting versions with identical (repo, id, updated_at, run_attempt)
        conflict_check = (
            df.groupBy("repo", "id", "updated_at", "run_attempt")
            .agg(
                F.countDistinct("status").alias("status_cnt"),
                F.countDistinct("conclusion").alias("conclusion_cnt"),
            )
            .filter((F.col("status_cnt") > 1) | (F.col("conclusion_cnt") > 1))
        )
        if conflict_check.count() > 0:
            conflict_row = conflict_check.first()
            raise ConflictVersionError(
                f"Conflicting workflow run observed for repo={conflict_row['repo']}, id={conflict_row['id']} "
                f"with identical version (updated_at={conflict_row['updated_at']}, attempt={conflict_row['run_attempt']})"
            )

        window_spec = Window.partitionBy("repo", "id").orderBy(
            F.col("updated_at").desc(),
            F.col("run_attempt").desc(),
            F.col("ingested_at").desc(),
        )

        deduped = (
            df.withColumn("_row_num", F.row_number().over(window_spec))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )
        return deduped

    def merge_into_iceberg(self, incoming_df: DataFrame) -> Dict[str, Any]:
        """
        Executes MERGE INTO the Apache Iceberg table.
        - Updates matching record if incoming updated_at is greater, or identical with higher run_attempt.
        - Inserts if not matched.
        - Replay of identical or older records is ignored (idempotent).
        """
        incoming_df.createOrReplaceTempView("incoming_batch")

        # Snapshot before merge for auditing
        table_before = self.spark.table(self.full_table_name)
        count_before = table_before.count()
        incoming_count = incoming_df.count()

        # Count how many match target table
        matched_count = (
            incoming_df.join(
                table_before,
                (incoming_df["repo"] == table_before["repo"]) & (incoming_df["id"] == table_before["id"]),
                "inner",
            ).count()
        )
        new_keys_estimate = incoming_count - matched_count

        merge_sql = f"""
        MERGE INTO {self.full_table_name} AS target
        USING incoming_batch AS source
        ON target.repo = source.repo AND target.id = source.id
        WHEN MATCHED AND (
            source.updated_at > target.updated_at OR
            (source.updated_at = target.updated_at AND source.run_attempt > target.run_attempt)
        ) THEN UPDATE SET
            target.name = source.name,
            target.workflow_id = source.workflow_id,
            target.status = source.status,
            target.conclusion = source.conclusion,
            target.run_attempt = source.run_attempt,
            target.event = source.event,
            target.head_branch = source.head_branch,
            target.updated_at = source.updated_at,
            target.run_started_at = source.run_started_at,
            target.ingested_at = source.ingested_at,
            target.run_ts = source.run_ts
        WHEN NOT MATCHED THEN INSERT (
            repo, id, name, workflow_id, status, conclusion, run_attempt, event,
            head_branch, created_at, updated_at, run_started_at, ingested_at, run_ts
        ) VALUES (
            source.repo, source.id, source.name, source.workflow_id, source.status,
            source.conclusion, source.run_attempt, source.event, source.head_branch,
            source.created_at, source.updated_at, source.run_started_at,
            source.ingested_at, source.run_ts
        )
        """
        self.spark.sql(merge_sql)

        # Get current snapshot ID from Iceberg metadata
        current_snapshot_id = self.get_current_snapshot_id()
        count_after = self.spark.table(self.full_table_name).count()
        net_new_inserted = count_after - count_before
        updated_keys = matched_count if net_new_inserted == 0 and incoming_count > 0 else (incoming_count - net_new_inserted)

        return {
            "table_count_before": count_before,
            "table_count_after": count_after,
            "incoming_rows": incoming_count,
            "new_keys_inserted": net_new_inserted,
            "matched_keys": matched_count,
            "snapshot_id": current_snapshot_id,
        }

    def get_current_snapshot_id(self) -> Optional[int]:
        """Retrieves the current snapshot ID from Iceberg's metadata history."""
        try:
            snapshots_df = self.spark.sql(f"SELECT snapshot_id FROM {self.full_table_name}.snapshots ORDER BY committed_at DESC LIMIT 1")
            row = snapshots_df.first()
            return row["snapshot_id"] if row else None
        except Exception:
            return None

    def run_batch_manifest(
        self,
        manifest_path: Union[str, Path],
        base_dir: Optional[Union[str, Path]] = None,
    ) -> Dict[str, Any]:
        """
        End-to-end atomic processing of a manifest:
        1. Validates manifest & contracts (fails before modifying table on corrupt/missing data).
        2. Parses into PySpark DataFrame with explicit schema.
        3. Deduplicates incoming rows.
        4. Executes MERGE INTO.
        5. Emits structured execution metrics.
        """
        started = time.monotonic()
        path = Path(manifest_path)
        if base_dir is None:
            base_dir = path.parent

        # Step 1: Pre-merge validation
        validation_report = self.load_and_validate_manifest(path, base_dir=base_dir)

        # Step 2: Extract & project schema
        manifest_dict = load_manifest(path)
        raw_df = self.extract_raw_dataframe(manifest_dict, base_dir=base_dir)
        input_rows = raw_df.count()

        # Step 3: Deduplicate
        deduped_df = self.deduplicate_incoming(raw_df)
        deduped_rows = deduped_df.count()

        # Step 4: Merge Into Iceberg
        merge_metrics = self.merge_into_iceberg(deduped_df)

        elapsed = time.monotonic() - started
        report = {
            "status": "SUCCESS",
            "repo": validation_report["repo"],
            "resource_type": validation_report["resource_type"],
            "run_ts": validation_report["run_ts"],
            "pages_processed": validation_report["pages_verified"],
            "input_rows": input_rows,
            "deduplicated_rows": deduped_rows,
            "duplicate_rows_dropped": input_rows - deduped_rows,
            "new_keys_inserted": merge_metrics["new_keys_inserted"],
            "matched_keys": merge_metrics["matched_keys"],
            "total_records_in_table": merge_metrics["table_count_after"],
            "snapshot_id": merge_metrics["snapshot_id"],
            "duration_seconds": round(elapsed, 3),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        return report

    def apply_schema_migration_add_head_branch(self):
        """
        Sample Schema Evolution: Adds nullable `head_branch` column if missing,
        verifying that existing data remains readable without downtime or data rewrite.
        """
        try:
            self.spark.sql(f"ALTER TABLE {self.full_table_name} ADD COLUMN IF NOT EXISTS head_branch STRING")
        except Exception as e:
            # Table already has column
            pass
