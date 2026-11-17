"""
Offline Logic and Contract Tests for GitPulse Pipeline
=======================================================
Verifies deduplication, version conflict detection, multi-repo isolation,
idempotency, and analytical SQL view calculations against synthetic fixtures.
Executes 100% offline without requiring Java runtime.
"""

import json
from pathlib import Path
import unittest

import duckdb

from gitpulse.manifest import load_manifest, validate_manifest
from gitpulse.pipeline import ConflictVersionError

FIXTURES_DIR = Path(__file__).resolve().parent.parent / "data" / "fixtures"


class PipelineLogicTests(unittest.TestCase):
    def setUp(self):
        self.con = duckdb.connect(database=":memory:")
        # Create staging table matching Iceberg schema
        self.con.execute("""
        CREATE TABLE workflow_runs_current (
            repo VARCHAR,
            id BIGINT,
            name VARCHAR,
            workflow_id BIGINT,
            status VARCHAR,
            conclusion VARCHAR,
            run_attempt INT,
            event VARCHAR,
            head_branch VARCHAR,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            run_started_at TIMESTAMP,
            ingested_at TIMESTAMP,
            run_ts VARCHAR
        )
        """)

    def tearDown(self):
        self.con.close()

    def _load_batch_records(self, batch_name: str):
        batch_dir = FIXTURES_DIR / batch_name
        records = []
        for repo_dir in batch_dir.iterdir():
            if not repo_dir.is_dir():
                continue
            for run_dir in repo_dir.glob("run_ts=*"):
                manifest_path = run_dir / "manifest.json"
                manifest = load_manifest(manifest_path)
                validate_manifest(manifest, base_dir=run_dir)
                for page_info in manifest["pages"]:
                    page_file = run_dir / page_info["filename"]
                    with open(page_file, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    for item in data.get("workflow_runs", []):
                        records.append({
                            "repo": manifest["repo"],
                            "id": item["id"],
                            "name": item.get("name"),
                            "workflow_id": item.get("workflow_id"),
                            "status": item["status"],
                            "conclusion": item.get("conclusion"),
                            "run_attempt": item.get("run_attempt", 1),
                            "event": item.get("event", "push"),
                            "head_branch": item.get("head_branch", "main"),
                            "created_at": item["created_at"].replace("Z", ""),
                            "updated_at": item["updated_at"].replace("Z", ""),
                            "run_started_at": item.get("run_started_at", item["created_at"]).replace("Z", ""),
                            "ingested_at": manifest.get("created_at", "2026-10-01T00:00:00").replace("Z", ""),
                            "run_ts": manifest["run_ts"],
                        })
        return records

    def _merge_records(self, records):
        if not records:
            return

        import pandas as pd
        records_df = pd.DataFrame(records)
        self.con.register("incoming_raw", records_df)

        conflict = self.con.execute("""
            SELECT repo, id, updated_at, run_attempt,
                   COUNT(DISTINCT status) AS status_cnt,
                   COUNT(DISTINCT conclusion) AS conclusion_cnt
            FROM incoming_raw
            GROUP BY repo, id, updated_at, run_attempt
            HAVING status_cnt > 1 OR conclusion_cnt > 1
        """).fetchall()

        if conflict:
            raise ConflictVersionError(f"Conflicting version detected: {conflict}")

        # In-batch dedup using window function (updated_at DESC, run_attempt DESC, ingested_at DESC)
        self.con.execute("""
            CREATE OR REPLACE TEMP TABLE incoming_deduped AS
            SELECT 
                repo,
                id,
                name,
                workflow_id,
                status,
                conclusion,
                run_attempt,
                event,
                head_branch,
                created_at::TIMESTAMP AS created_at,
                updated_at::TIMESTAMP AS updated_at,
                run_started_at::TIMESTAMP AS run_started_at,
                ingested_at::TIMESTAMP AS ingested_at,
                run_ts
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY repo, id
                           ORDER BY updated_at::TIMESTAMP DESC, run_attempt DESC, ingested_at::TIMESTAMP DESC
                       ) AS rn
                FROM incoming_raw
            ) WHERE rn = 1
        """)

        # Execute MERGE INTO (upsert)
        self.con.execute("""
            MERGE INTO workflow_runs_current AS target
            USING incoming_deduped AS source
            ON target.repo = source.repo AND target.id = source.id
            WHEN MATCHED AND (
                source.updated_at > target.updated_at OR
                (source.updated_at = target.updated_at AND source.run_attempt > target.run_attempt)
            ) THEN UPDATE SET
                name = source.name,
                workflow_id = source.workflow_id,
                status = source.status,
                conclusion = source.conclusion,
                run_attempt = source.run_attempt,
                event = source.event,
                head_branch = source.head_branch,
                updated_at = source.updated_at,
                run_started_at = source.run_started_at,
                ingested_at = source.ingested_at,
                run_ts = source.run_ts
            WHEN NOT MATCHED THEN INSERT (
                repo, id, name, workflow_id, status, conclusion, run_attempt, event,
                head_branch, created_at, updated_at, run_started_at, ingested_at, run_ts
            ) VALUES (
                source.repo, source.id, source.name, source.workflow_id, source.status,
                source.conclusion, source.run_attempt, source.event, source.head_branch,
                source.created_at, source.updated_at, source.run_started_at,
                source.ingested_at, source.run_ts
            )
        """)

    def test_condition_1_multi_repo_isolation_preserves_same_id_in_both_repos(self):
        """Condition 1: Same ID (101) in both apache/spark and apache/iceberg must both be retained."""
        batch_1_records = self._load_batch_records("batch_1")
        self._merge_records(batch_1_records)

        res = self.con.execute("""
            SELECT repo, id, status, conclusion
            FROM workflow_runs_current
            WHERE id = 101
            ORDER BY repo
        """).fetchall()

        self.assertEqual(len(res), 2, "Expected both repositories to retain run ID 101")
        self.assertEqual(res[0][0], "apache/iceberg")
        self.assertEqual(res[1][0], "apache/spark")

    def test_condition_2_and_3_in_progress_updates_to_completed_and_stale_replay_rejected(self):
        """Condition 3: in_progress changes to completed; replaying old state does not revert data."""
        # 1. Ingest Batch 1
        self._merge_records(self._load_batch_records("batch_1"))
        run_102_b1 = self.con.execute("SELECT status, conclusion FROM workflow_runs_current WHERE repo = 'apache/spark' AND id = 102").fetchone()
        self.assertEqual(run_102_b1[0], "in_progress")

        # 2. Ingest Batch 2 (Run 102 updated to completed / success)
        self._merge_records(self._load_batch_records("batch_2"))
        run_102_b2 = self.con.execute("SELECT status, conclusion FROM workflow_runs_current WHERE repo = 'apache/spark' AND id = 102").fetchone()
        self.assertEqual(run_102_b2[0], "completed")
        self.assertEqual(run_102_b2[1], "success")

        # 3. Ingest Batch 3 (Replay with stale attempt of in_progress for Run 102)
        self._merge_records(self._load_batch_records("batch_3_replay"))
        run_102_b3 = self.con.execute("SELECT status, conclusion FROM workflow_runs_current WHERE repo = 'apache/spark' AND id = 102").fetchone()
        self.assertEqual(run_102_b3[0], "completed", "Stale in_progress must NOT overwrite completed state")
        self.assertEqual(run_102_b3[1], "success")

    def test_condition_4_late_arriving_records_route_to_correct_historical_date(self):
        """Condition 4: Late-arriving run 99 (created_at 2026-09-30) routes to 2026-09-30."""
        self._merge_records(self._load_batch_records("batch_1"))
        self._merge_records(self._load_batch_records("batch_2"))

        run_99 = self.con.execute("""
            SELECT id, CAST(created_at AS DATE) AS dt, status
            FROM workflow_runs_current
            WHERE id = 99
        """).fetchone()

        self.assertIsNotNone(run_99)
        self.assertEqual(str(run_99[1]), "2026-09-30")

    def test_condition_2_replay_idempotency_does_not_duplicate_records(self):
        """Condition 2: Replaying same manifest does not increase table record count."""
        self._merge_records(self._load_batch_records("batch_1"))
        self._merge_records(self._load_batch_records("batch_2"))
        count_after_b2 = self.con.execute("SELECT COUNT(*) FROM workflow_runs_current").fetchone()[0]

        # Replay Batch 2 again
        self._merge_records(self._load_batch_records("batch_2"))
        count_after_replay = self.con.execute("SELECT COUNT(*) FROM workflow_runs_current").fetchone()[0]

        self.assertEqual(count_after_b2, count_after_replay)
        self.assertEqual(count_after_replay, 8)

    def test_condition_8_sql_reliability_view_and_zero_division_safety(self):
        """Condition 8: SQL view handles zero completed runs safely without division-by-zero error."""
        self._merge_records(self._load_batch_records("batch_1"))

        # Create analytical view
        self.con.execute("""
            CREATE OR REPLACE TEMP VIEW v_workflow_run_reliability AS
            SELECT
                repo,
                name AS workflow_name,
                CAST(created_at AS DATE) AS created_date,
                COUNT(*) AS total_runs,
                COUNT(CASE WHEN status = 'completed' THEN 1 END) AS completed_runs,
                COUNT(CASE WHEN conclusion = 'failure' THEN 1 END) AS failure_count,
                ROUND(
                    CAST(COUNT(CASE WHEN conclusion = 'success' THEN 1 END) AS DOUBLE) /
                    NULLIF(COUNT(CASE WHEN status = 'completed' THEN 1 END), 0) * 100.0,
                    2
                ) AS success_rate_pct,
                ROUND(
                    AVG(
                        CASE
                            WHEN status = 'completed' AND updated_at >= created_at
                            THEN CAST(epoch(updated_at) - epoch(created_at) AS DOUBLE) / 60.0
                            ELSE NULL
                        END
                    ),
                    2
                ) AS avg_latency_to_completion_minutes
            FROM workflow_runs_current
            GROUP BY repo, name, CAST(created_at AS DATE)
        """)

        # Integration Tests has status = 'in_progress', completed_runs = 0
        in_progress_row = self.con.execute("""
            SELECT total_runs, completed_runs, success_rate_pct
            FROM v_workflow_run_reliability
            WHERE workflow_name = 'Integration Tests'
        """).fetchone()

        self.assertEqual(in_progress_row[0], 1)
        self.assertEqual(in_progress_row[1], 0)
        self.assertIsNone(in_progress_row[2], "success_rate_pct must be NULL (zero division safe) when completed_runs = 0")


if __name__ == "__main__":
    unittest.main()
