"""
Integration Tests for Apache Iceberg PySpark Pipeline
=====================================================
Validates end-to-end Iceberg table creation, MERGE INTO updates,
late-arriving records, snapshot time-travel queries, and schema evolution.
Runs within Docker (Glue 5.0 / Spark 3.5.4) or in environments with Java 17+.
"""

import os
from pathlib import Path
import shutil
import unittest

FIXTURES_DIR = Path(__file__).resolve().parent.parent / "data" / "fixtures"
TEMP_WAREHOUSE = Path(__file__).resolve().parent.parent / "data" / "test_warehouse"


def is_spark_available():
    import subprocess
    try:
        proc = subprocess.run(["java", "-version"], capture_output=True, text=True, timeout=3)
        version_output = proc.stderr or proc.stdout
        # Java 8 (1.8.) does not support Spark 4 / Java 17 bytecode (class version 61)
        if "1.8." in version_output or "1.7." in version_output:
            return False
        if any(v in version_output for v in ("17.", "21.", "22.", "23.", "24.")):
            return True
        return False
    except Exception:
        return False


SPARK_OK = is_spark_available()


@unittest.skipIf(not SPARK_OK, "Spark runtime with Java 17+ not available in local host; runs inside Docker")
class IcebergPipelineIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from gitpulse.spark_session import get_spark_session
        from gitpulse.pipeline import IcebergPipeline

        if TEMP_WAREHOUSE.exists():
            shutil.rmtree(TEMP_WAREHOUSE)

        cls.spark = get_spark_session(
            catalog_name="local",
            warehouse_path=str(TEMP_WAREHOUSE).replace("\\", "/"),
            catalog_type="hadoop",
            app_name="GitPulse-Iceberg-IntegrationTest",
        )
        cls.pipeline = IcebergPipeline(cls.spark, catalog="local", database="test_db")

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "spark"):
            cls.spark.stop()
        if TEMP_WAREHOUSE.exists():
            shutil.rmtree(TEMP_WAREHOUSE)

    def test_end_to_end_iceberg_lifecycle(self):
        """Tests Batch 1 -> Snapshot 1 -> Batch 2 -> Snapshot 2 -> Time-Travel -> Replay."""
        from gitpulse.analytics import (
            create_reliability_view,
            query_7_day_failure_trend,
            query_least_stable_workflows,
            query_snapshot_time_travel_comparison,
        )

        b1_spark = FIXTURES_DIR / "batch_1" / "apache_spark" / "run_ts=20261001T150000Z" / "manifest.json"
        b1_iceberg = FIXTURES_DIR / "batch_1" / "apache_iceberg" / "run_ts=20261001T150000Z" / "manifest.json"

        # 1. Ingest Batch 1
        rep1 = self.pipeline.run_batch_manifest(b1_spark)
        self.assertEqual(rep1["new_keys_inserted"], 3)
        rep2 = self.pipeline.run_batch_manifest(b1_iceberg)
        self.assertEqual(rep2["new_keys_inserted"], 2)

        snap_1 = self.pipeline.get_current_snapshot_id()
        self.assertIsNotNone(snap_1)

        # 2. Ingest Batch 2 (Updates & Late-arriving)
        b2_spark = FIXTURES_DIR / "batch_2" / "apache_spark" / "run_ts=20261002T120000Z" / "manifest.json"
        b2_iceberg = FIXTURES_DIR / "batch_2" / "apache_iceberg" / "run_ts=20261002T120000Z" / "manifest.json"

        self.pipeline.run_batch_manifest(b2_spark)
        self.pipeline.run_batch_manifest(b2_iceberg)

        snap_2 = self.pipeline.get_current_snapshot_id()
        self.assertIsNotNone(snap_2)
        self.assertNotEqual(snap_1, snap_2)

        # 3. Snapshot Time-Travel Comparison
        comparison = query_snapshot_time_travel_comparison(
            self.spark,
            table_name=self.pipeline.full_table_name,
            snapshot_before=snap_1,
            snapshot_after=snap_2,
        )
        self.assertIn("delta", comparison)
        self.assertGreater(comparison["delta"]["new_runs_count"], 0)

        # 4. Ingest Batch 3 (Replay) -> verify Idempotency
        count_before_replay = self.spark.table(self.pipeline.full_table_name).count()
        b3_spark = FIXTURES_DIR / "batch_3_replay" / "apache_spark" / "run_ts=20261002T120000Z" / "manifest.json"
        self.pipeline.run_batch_manifest(b3_spark)
        count_after_replay = self.spark.table(self.pipeline.full_table_name).count()
        self.assertEqual(count_before_replay, count_after_replay)

        # 5. Schema Migration
        self.pipeline.apply_schema_migration_add_head_branch()
        cols = self.spark.table(self.pipeline.full_table_name).columns
        self.assertIn("head_branch", cols)

        # 6. Analytical Views
        view = create_reliability_view(self.spark, table_name=self.pipeline.full_table_name)
        trend = query_7_day_failure_trend(self.spark, view_name=view)
        self.assertGreater(trend.count(), 0)

        flaky = query_least_stable_workflows(self.spark, view_name=view)
        self.assertGreater(flaky.count(), 0)


if __name__ == "__main__":
    unittest.main()
