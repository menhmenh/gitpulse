"""Regression tests for Glue transform fixes: multi-repo deduplication and deterministic partition date fallback."""
import unittest
import pandas as pd
from datetime import datetime, timezone

from terraform.glue_scripts.transform_and_dora import dedupe, partition_date, _extract_run_date


class TransformRegressionTests(unittest.TestCase):
    def test_dedupe_preserves_same_id_across_different_repos(self):
        """Regression test: Identical contributor ID or commit SHA across different repos must NOT be dropped."""
        data = [
            {"_repo": "owner/repo-a", "id": 101, "login": "alice", "_run_ts": "20260918T000000Z"},
            {"_repo": "owner/repo-b", "id": 101, "login": "alice", "_run_ts": "20260918T000000Z"},
            # Duplicate within repo-a with older run_ts:
            {"_repo": "owner/repo-a", "id": 101, "login": "alice_old", "_run_ts": "20260917T000000Z"},
        ]
        df = pd.DataFrame(data)
        deduped = dedupe(df, "contributors")

        self.assertEqual(len(deduped), 2)
        repos = set(deduped["_repo"].tolist())
        self.assertEqual(repos, {"owner/repo-a", "owner/repo-b"})

        repo_a_record = deduped[deduped["_repo"] == "owner/repo-a"].iloc[0]
        self.assertEqual(repo_a_record["login"], "alice")
        self.assertEqual(repo_a_record["_run_ts"], "20260918T000000Z")

    def test_dedupe_works_when_repo_column_is_named_repo(self):
        """Dedupe works if dataframe column is 'repo' instead of '_repo'."""
        data = [
            {"repo": "owner/repo-a", "sha": "c0ffee", "_run_ts": "20260918T000000Z"},
            {"repo": "owner/repo-b", "sha": "c0ffee", "_run_ts": "20260918T000000Z"},
        ]
        df = pd.DataFrame(data)
        deduped = dedupe(df, "commits")
        self.assertEqual(len(deduped), 2)

    def test_extract_run_date_parses_iso_compact_format(self):
        """Verifies extracting YYYY-MM-DD from run_ts string."""
        self.assertEqual(_extract_run_date("20260918T123045Z"), "2026-09-18")
        self.assertEqual(_extract_run_date("20260925T110000Z_sim_fail"), "2026-09-25")

    def test_partition_date_falls_back_to_run_ts_date_not_today(self):
        """Regression test: Missing business date falls back to ingestion date (_run_ts) for deterministic replays."""
        data = [
            {"_repo": "owner/repo-a", "id": 101, "_run_ts": "20260918T000000Z"},
            {"_repo": "owner/repo-b", "id": 102, "_run_ts": "20260919T000000Z"},
        ]
        df = pd.DataFrame(data)
        # contributors has date_field = None
        result = partition_date(df, "contributors")
        self.assertIn("dt", result.columns)
        self.assertEqual(result.loc[result["id"] == 101, "dt"].values[0], "2026-09-18")
        self.assertEqual(result.loc[result["id"] == 102, "dt"].values[0], "2026-09-19")

    def test_partition_date_uses_business_date_when_valid(self):
        """Valid business date takes precedence over ingestion date."""
        data = [
            {"_repo": "owner/repo-a", "id": 1, "created_at": "2026-08-15T10:00:00Z", "_run_ts": "20260918T000000Z"},
            {"_repo": "owner/repo-a", "id": 2, "created_at": None, "_run_ts": "20260918T000000Z"},
        ]
        df = pd.DataFrame(data)
        result = partition_date(df, "issues")
        self.assertEqual(result.loc[result["id"] == 1, "dt"].values[0], "2026-08-15")
        self.assertEqual(result.loc[result["id"] == 2, "dt"].values[0], "2026-09-18")


if __name__ == "__main__":
    unittest.main()
