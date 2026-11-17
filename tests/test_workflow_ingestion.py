"""Offline regression tests for the workflow_runs DLQ timeout fix."""
import importlib.util
import json
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlparse


SOURCE = Path(__file__).resolve().parents[1] / "terraform/lambda_src/handler.py"


class WorkflowIngestionTests(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location("workflow_ingestion", SOURCE)
        self.handler = importlib.util.module_from_spec(spec)
        with patch.dict(os.environ, {"RAW_BUCKET": "test-raw", "GITHUB_SECRET_ARN": "test-secret"}), \
                patch.dict(sys.modules, {"boto3": MagicMock()}):
            spec.loader.exec_module(self.handler)
        self.handler._token_cache = "test-token"
        self.since = "2026-09-17T00:00:41.703479+00:00"
        self.until = "2026-09-18T00:00:41+00:00"

    def run_item(self, identifier, created):
        return {"id": identifier, "created_at": created}

    def test_original_dlq_message_uses_encoded_bounded_window_and_writes_envelope(self):
        payload = {"repo": "vercel/next.js", "resource_type": "workflow_runs",
                   "since": self.since, "run_ts": "20260918T000041Z"}
        response = {"total_count": 1, "workflow_runs": [
            self.run_item(1, "2026-09-17T01:00:00Z")]}
        with patch.object(self.handler, "call_github", return_value=(response, "")) as api:
            self.assertEqual(self.handler.handler({"Records": [{"body": json.dumps(payload)}]}, None),
                             {"status": "ok"})
        url = api.call_args.args[0]
        query = parse_qs(urlparse(url).query)
        self.assertEqual(query["created"], ["2026-09-17T00:00:41Z..2026-09-18T00:00:41Z"])
        self.assertEqual(query["per_page"], ["100"])
        stored = self.handler._s3.put_object.call_args.kwargs
        self.assertEqual(json.loads(stored["Body"]), response)
        self.assertIn("run_ts=20260918T000041Z/page_0.json", stored["Key"])

    def test_pagination_keeps_exact_boundaries_without_assuming_item_order(self):
        next_url = "https://api.github.com/repos/vercel/next.js/actions/runs?page=2&created=window"
        first = {"total_count": 4, "workflow_runs": [
            self.run_item(0, "2026-09-17T00:00:41Z"),
            self.run_item(1, "2026-09-17T00:00:42Z")]}
        second = {"total_count": 4, "workflow_runs": [
            self.run_item(2, "2026-09-18T00:00:41Z"),
            self.run_item(3, "2026-09-18T00:00:42Z")]}
        with patch.object(self.handler, "call_github", side_effect=[
                (first, f'<{next_url}>; rel="next"'), (second, "")]) as api:
            pages = self.handler.fetch_pages("vercel/next.js", "workflow_runs", self.since,
                                             "test-token", until=self.until)
        self.assertEqual([item["id"] for page in pages for item in page["workflow_runs"]], [1, 2])
        self.assertEqual(api.call_args.args[0], next_url)
        self.assertEqual(pages[0]["total_count"], 4)

    def test_splits_over_limit_windows_without_overlap_or_missing_seconds(self):
        windows = []

        def api(url, token):
            window = parse_qs(urlparse(url).query)["created"][0]
            windows.append(window)
            if len(windows) == 1:
                return {"total_count": 1001, "workflow_runs": []}, ""
            created = window.split("..")[0]
            return {"total_count": 1, "workflow_runs": [self.run_item(len(windows), created)]}, ""

        with patch.object(self.handler, "call_github", side_effect=api):
            pages = self.handler.fetch_pages("vercel/next.js", "workflow_runs",
                                             "2026-09-17T00:00:00Z", "test-token",
                                             until="2026-09-17T00:00:03Z")
        self.assertEqual(windows, [
            "2026-09-17T00:00:00Z..2026-09-17T00:00:03Z",
            "2026-09-17T00:00:00Z..2026-09-17T00:00:01Z",
            "2026-09-17T00:00:02Z..2026-09-17T00:00:03Z"])
        self.assertEqual(len(pages), 2)

    def test_unsplittable_over_limit_window_fails_before_any_s3_write(self):
        payload = {"repo": "vercel/next.js", "resource_type": "workflow_runs",
                   "since": "2026-09-18T00:00:41Z", "run_ts": "20260918T000041Z"}
        with patch.object(self.handler, "call_github", return_value=(
                {"total_count": 1001, "workflow_runs": []}, "")):
            with self.assertRaisesRegex(RuntimeError, "More than 1,000"):
                self.handler.handler({"Records": [{"body": json.dumps(payload)}]}, None)
        self.handler._s3.put_object.assert_not_called()

    def test_empty_window_response_preserves_envelope(self):
        with patch.object(self.handler, "call_github", return_value=(
                {"total_count": 0, "workflow_runs": []}, "")):
            pages = self.handler.fetch_pages("vercel/next.js", "workflow_runs", self.since,
                                             "test-token", until=self.until)
        self.assertEqual(pages, [{"total_count": 0, "workflow_runs": []}])

    def test_github_failure_propagates_without_writing_s3(self):
        payload = {"repo": "vercel/next.js", "resource_type": "workflow_runs",
                   "since": self.since, "run_ts": "20260918T000041Z"}
        with patch.object(self.handler, "call_github", side_effect=RuntimeError("GitHub unavailable")):
            with self.assertRaisesRegex(RuntimeError, "GitHub unavailable"):
                self.handler.handler({"Records": [{"body": json.dumps(payload)}]}, None)
        self.handler._s3.put_object.assert_not_called()

    def test_other_resource_since_keeps_positive_timezone_offset(self):
        since = "2026-09-17T07:00:41+07:00"
        with patch.object(self.handler, "call_github", return_value=([], "")) as api:
            self.handler.fetch_pages("vercel/next.js", "commits", since, "test-token")
        self.assertEqual(parse_qs(urlparse(api.call_args.args[0]).query)["since"], [since])

    def test_ingestion_with_write_manifest_writes_both_page_and_valid_manifest(self):
        payload = {
            "repo": "vercel/next.js",
            "resource_type": "workflow_runs",
            "since": self.since,
            "until": self.until,
            "run_ts": "20260918T000041Z",
            "write_manifest": True,
        }
        response = {"total_count": 1, "workflow_runs": [self.run_item(10, "2026-09-17T01:00:00Z")]}
        with patch.object(self.handler, "call_github", return_value=(response, "")):
            res = self.handler.handler({"Records": [{"body": json.dumps(payload)}]}, None)
        self.assertEqual(res, {"status": "ok"})

        calls = self.handler._s3.put_object.call_args_list
        self.assertEqual(len(calls), 2)
        # Call 1: page_0.json
        self.assertIn("page_0.json", calls[0].kwargs["Key"])
        # Call 2: manifest.json
        manifest_call = calls[1].kwargs
        self.assertIn("manifest.json", manifest_call["Key"])
        manifest = json.loads(manifest_call["Body"])
        self.assertEqual(manifest["total_records"], 1)
        self.assertEqual(manifest["repo"], "vercel/next.js")
        self.assertEqual(manifest["resource_type"], "workflow_runs")
        self.assertEqual(len(manifest["pages"]), 1)
        self.assertEqual(manifest["pages"][0]["filename"], "page_0.json")

    def test_ingestion_with_explicit_until_window_uses_exact_until(self):
        explicit_until = "2026-09-17T12:00:00Z"
        payload = {
            "repo": "vercel/next.js",
            "resource_type": "workflow_runs",
            "since": "2026-09-17T00:00:00Z",
            "until": explicit_until,
            "run_ts": "20260918T000041Z",
        }
        response = {"total_count": 0, "workflow_runs": []}
        with patch.object(self.handler, "call_github", return_value=(response, "")) as api:
            self.handler.handler({"Records": [{"body": json.dumps(payload)}]}, None)
        url = api.call_args.args[0]
        query = parse_qs(urlparse(url).query)
        self.assertEqual(query["created"], ["2026-09-17T00:00:00Z..2026-09-17T12:00:00Z"])


if __name__ == "__main__":
    unittest.main()
