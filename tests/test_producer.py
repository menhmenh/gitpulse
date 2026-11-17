"""Unit tests for the enhanced Producer Lambda handler."""
import importlib.util
import json
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock, patch

SOURCE = Path(__file__).resolve().parents[1] / "terraform/producer_src/handler.py"


class ProducerHandlerTests(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location("producer_handler", SOURCE)
        self.handler = importlib.util.module_from_spec(spec)
        with patch.dict(os.environ, {
            "JOB_QUEUE_URL": "https://sqs.mock.queue",
            "GITHUB_REPOS": "apache/spark,apache/iceberg",
            "LOOKBACK_HOURS": "24",
        }), patch.dict(sys.modules, {"boto3": MagicMock()}):
            spec.loader.exec_module(self.handler)
        self.mock_sqs = MagicMock()
        self.handler._sqs = self.mock_sqs

    def test_default_producer_enqueues_8_resources_for_all_repos(self):
        """Standard scheduled run enqueues 2 repos * 8 resources = 16 messages without until or write_manifest."""
        res = self.handler.handler({}, None)
        self.assertEqual(res["messages_enqueued"], 16)
        self.assertEqual(self.mock_sqs.send_message.call_count, 16)

        # Check first enqueued message
        first_call = self.mock_sqs.send_message.call_args_list[0].kwargs
        body = json.loads(first_call["MessageBody"])
        self.assertEqual(body["repo"], "apache/spark")
        self.assertEqual(body["resource_type"], "repository")
        self.assertIn("since", body)
        self.assertIn("run_ts", body)
        self.assertNotIn("until", body)
        self.assertNotIn("write_manifest", body)

    def test_targeted_workflow_runs_with_window_and_manifest(self):
        """Allows targeting only workflow_runs with fixed since, until, and manifest generation."""
        custom_event = {
            "resource_types": ["workflow_runs"],
            "repos": ["apache/spark"],
            "since": "2026-10-01T00:00:00Z",
            "until": "2026-10-01T12:00:00Z",
            "write_manifest": True,
            "run_ts": "20261001T120000Z",
        }
        res = self.handler.handler(custom_event, None)
        self.assertEqual(res["messages_enqueued"], 1)
        self.assertEqual(self.mock_sqs.send_message.call_count, 1)

        body = json.loads(self.mock_sqs.send_message.call_args.kwargs["MessageBody"])
        self.assertEqual(body["repo"], "apache/spark")
        self.assertEqual(body["resource_type"], "workflow_runs")
        self.assertEqual(body["since"], "2026-10-01T00:00:00Z")
        self.assertEqual(body["until"], "2026-10-01T12:00:00Z")
        self.assertEqual(body["run_ts"], "20261001T120000Z")
        self.assertTrue(body["write_manifest"])


if __name__ == "__main__":
    unittest.main()
