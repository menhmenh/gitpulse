"""
Unit and Contract Tests for GitPulse Manifest and Fixtures Validation
====================================================================
Verifies that the manifest module correctly enforces SHA-256 checksums,
page completeness, well-formed JSON, mandatory schema keys, and atomic batch checks.
"""

import json
from pathlib import Path
import unittest

from gitpulse.manifest import (
    validate_manifest,
    validate_batch,
    load_manifest,
    compute_sha256,
    MissingPageError,
    ChecksumMismatchError,
    InvalidJsonError,
    MissingRequiredKeysError,
    RecordCountMismatchError,
    InvalidTimestampError,
)

FIXTURES_DIR = Path(__file__).resolve().parent.parent / "data" / "fixtures"


class ManifestValidationTests(unittest.TestCase):
    def test_batch_1_manifests_are_valid(self):
        """Batch 1 fixtures for both apache/spark and apache/iceberg pass validation."""
        batch_1_dir = FIXTURES_DIR / "batch_1"
        spark_manifest = batch_1_dir / "apache_spark" / "run_ts=20261001T150000Z" / "manifest.json"
        iceberg_manifest = batch_1_dir / "apache_iceberg" / "run_ts=20261001T150000Z" / "manifest.json"

        spark_res = validate_manifest(spark_manifest)
        self.assertTrue(spark_res["valid"])
        self.assertEqual(spark_res["records_verified"], 3)
        self.assertEqual(spark_res["repo"], "apache/spark")

        iceberg_res = validate_manifest(iceberg_manifest)
        self.assertTrue(iceberg_res["valid"])
        self.assertEqual(iceberg_res["records_verified"], 2)
        self.assertEqual(iceberg_res["repo"], "apache/iceberg")

    def test_batch_2_and_3_manifests_are_valid(self):
        """Batch 2 and Batch 3 fixtures pass validation."""
        b2_spark = FIXTURES_DIR / "batch_2" / "apache_spark" / "run_ts=20261002T120000Z" / "manifest.json"
        res2 = validate_manifest(b2_spark)
        self.assertTrue(res2["valid"])
        self.assertEqual(res2["records_verified"], 3)

        b3_spark = FIXTURES_DIR / "batch_3_replay" / "apache_spark" / "run_ts=20261002T120000Z" / "manifest.json"
        res3 = validate_manifest(b3_spark)
        self.assertTrue(res3["valid"])
        self.assertEqual(res3["records_verified"], 4)

    def test_validate_batch_validates_multiple_manifests_atomically(self):
        """validate_batch succeeds when all manifests in the batch are valid."""
        batch_1_dir = FIXTURES_DIR / "batch_1"
        spark_m = batch_1_dir / "apache_spark" / "run_ts=20261001T150000Z" / "manifest.json"
        iceberg_m = batch_1_dir / "apache_iceberg" / "run_ts=20261001T150000Z" / "manifest.json"

        results = validate_batch([spark_m, iceberg_m])
        self.assertEqual(len(results), 2)
        self.assertTrue(all(r["valid"] for r in results))

    def test_missing_page_raises_missing_page_error(self):
        """Condition 5: Missing page file raises MissingPageError and aborts."""
        bad_manifest = FIXTURES_DIR / "corruptions" / "missing_page" / "manifest.json"
        with self.assertRaises(MissingPageError) as ctx:
            validate_manifest(bad_manifest)
        self.assertIn("page_1.json", str(ctx.exception))

    def test_checksum_mismatch_raises_checksum_mismatch_error(self):
        """Condition 5: Altered page content with mismatching checksum raises ChecksumMismatchError."""
        bad_manifest = FIXTURES_DIR / "corruptions" / "checksum_mismatch" / "manifest.json"
        with self.assertRaises(ChecksumMismatchError) as ctx:
            validate_manifest(bad_manifest)
        self.assertIn("Checksum mismatch", str(ctx.exception))

    def test_malformed_json_raises_invalid_json_error(self):
        """Condition 5: Corrupt JSON syntax in raw page raises InvalidJsonError."""
        bad_manifest = FIXTURES_DIR / "corruptions" / "malformed_json" / "manifest.json"
        with self.assertRaises(InvalidJsonError) as ctx:
            validate_manifest(bad_manifest)
        self.assertIn("Malformed JSON", str(ctx.exception))

    def test_missing_required_keys_raises_missing_keys_error(self):
        """Condition 5: Record missing mandatory 'id' or 'status' raises MissingRequiredKeysError."""
        bad_manifest = FIXTURES_DIR / "corruptions" / "missing_key" / "manifest.json"
        with self.assertRaises(MissingRequiredKeysError) as ctx:
            validate_manifest(bad_manifest)
        self.assertIn("missing required keys", str(ctx.exception))

    def test_invalid_timestamp_raises_invalid_timestamp_error(self):
        """Condition 5: Record with invalid ISO timestamp raises InvalidTimestampError."""
        bad_manifest = FIXTURES_DIR / "corruptions" / "invalid_timestamp" / "manifest.json"
        with self.assertRaises(InvalidTimestampError) as ctx:
            validate_manifest(bad_manifest)
        self.assertIn("invalid ISO timestamp", str(ctx.exception))

    def test_record_count_mismatch_raises_error(self):
        """Mismatched record count in manifest vs actual page raises RecordCountMismatchError."""
        manifest_data = {
            "manifest_version": "1.0",
            "repo": "apache/spark",
            "resource_type": "workflow_runs",
            "total_records": 999,  # Declared 999, but page has 3
            "pages": [
                {
                    "page_index": 0,
                    "filename": "page_0.json",
                    "record_count": 999,
                    "checksum_sha256": None,
                }
            ],
        }
        base_dir = FIXTURES_DIR / "batch_1" / "apache_spark" / "run_ts=20261001T150000Z"
        with self.assertRaises(RecordCountMismatchError):
            validate_manifest(manifest_data, base_dir=base_dir)


if __name__ == "__main__":
    unittest.main()
