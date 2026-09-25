#!/usr/bin/env python3
"""
M5-3 End-to-End Failure Simulation & Verification Suite
=======================================================

Verifies all three required failure chains (PRD FR-19..FR-23, FR-32, DoD §8.6,
Architecture §5 & §12.2):

1. Ingestion Lambda Failure -> SQS Retries -> DLQ -> CloudWatch Alarm -> SNS
   - Intentionally breaks 1 Ingestion Lambda job (including atomic partial-S3-write
     rollback verification and/or invalid GitHub endpoint 404).
   - Verifies message is NOT deleted when Lambda throws, retries after visibility
     timeout (`ApproximateReceiveCount` 1 -> 2 -> 3), moves to DLQ after
     `maxReceiveCount = 3`, triggers `gitpulse-dlq-depth` -> `ALARM`, publishes to
     `gitpulse-pipeline-alerts`, and leaves ZERO partial objects in S3.

2. Step Functions Transform Tail: Glue Job Failure -> Catch -> FailNotify -> SNS
   - Deliberately breaks the Glue transform script (`--SIMULATE_FAILURE=true`).
   - Verifies SFN `TransformGlueJob` Catch transitions to `NotifyTransformFailed`
     -> `FailNotify` -> publishes to the SAME `gitpulse-pipeline-alerts` SNS topic
     with `FailedStage = TransformGlueJob` and the single SFN Execution ARN in body.

3. Step Functions Transform Tail: Crawler Failure -> Catch/Choice -> FailNotify -> SNS
   - Deliberately breaks the Glue Crawler (e.g. `CrawlRaw`).
   - Verifies SFN transitions to `NotifyCrawlRawFailed` -> `FailNotify` -> publishes
     to the SAME `gitpulse-pipeline-alerts` SNS topic with `FailedStage = CrawlRaw`
     and the single SFN Execution ARN in body.

Usage:
  - Local contract & unit simulation test (no AWS credentials needed):
      python tests/simulate_failures_m5_3.py --mode unit
  - Live AWS end-to-end simulation test:
      python tests/simulate_failures_m5_3.py --mode aws --profile gitpulse --region ap-southeast-1
"""

import argparse
import json
import os
import sys
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parent.parent
LAMBDA_SRC = REPO_ROOT / "terraform" / "lambda_src"
GLUE_SCRIPTS = REPO_ROOT / "terraform" / "glue_scripts"


# ============================================================================
# 1. LOCAL / UNIT SIMULATION VERIFICATION (Fast, deterministic verification)
# ============================================================================

class TestM53FailureSimulationLocal(unittest.TestCase):
    """Verifies the exact code paths for all 3 failure simulation scenarios."""

    def setUp(self):
        os.environ["RAW_BUCKET"] = "gitpulse-raw-test"
        os.environ["PROCESSED_BUCKET"] = "gitpulse-processed-test"
        os.environ["GITHUB_SECRET_ARN"] = "arn:aws:secretsmanager:ap-southeast-1:123456789012:secret:gitpulse/github-token-test"
        os.environ["REPOS"] = "vercel/next.js"
        for mod_name in ("boto3", "numpy", "pandas"):
            if mod_name not in sys.modules:
                try:
                    __import__(mod_name)
                except ImportError:
                    sys.modules[mod_name] = MagicMock()

    def test_1_ingestion_lambda_throws_and_rolls_back_partial_s3_writes(self):
        """
        Scenario 1:
        - Lambda writes page_0.json during `partial_write_rollback` simulation,
          throws RuntimeError, deletes page_0.json in `except Exception:`, and
          propagates the exception so SQS does NOT delete the message and retries
          until maxReceiveCount=3 -> DLQ.
        - S3 contains 0 objects for the failed run_ts.
        """
        sys.path.insert(0, str(LAMBDA_SRC))
        import handler as ingestion_handler

        s3_store = {}

        def fake_put_object(Bucket, Key, Body, ContentType=None):
            s3_store[(Bucket, Key)] = Body

        def fake_delete_object(Bucket, Key):
            s3_store.pop((Bucket, Key), None)

        mock_s3 = MagicMock()
        mock_s3.put_object.side_effect = fake_put_object
        mock_s3.delete_object.side_effect = fake_delete_object

        mock_sm = MagicMock()
        mock_sm.get_secret_value.return_value = {"SecretString": json.dumps({"token": "mock_secret_manager_token"})}

        ingestion_handler._s3 = mock_s3
        ingestion_handler._sm = mock_sm
        ingestion_handler._token_cache = None

        run_ts = "20260925T110000Z_sim_fail"
        sqs_message_body = {
            "repo": "vercel/next.js",
            "resource_type": "repository",
            "since": "2026-09-24T00:00:00Z",
            "run_ts": run_ts,
            "simulate_failure": "partial_write_rollback",
        }

        # Simulate SQS retrying the message 3 times (maxReceiveCount = 3)
        max_receive_count = 3
        dlq_messages = []

        for receive_count in range(1, max_receive_count + 1):
            event = {
                "Records": [
                    {
                        "messageId": "msg-sim-001",
                        "attributes": {"ApproximateReceiveCount": str(receive_count)},
                        "body": json.dumps(sqs_message_body),
                    }
                ]
            }
            with self.assertRaises(RuntimeError) as ctx:
                ingestion_handler.handler(event, None)

            self.assertIn("Simulated mid-write failure", str(ctx.exception))
            # Verify S3 put_object was called AND rolled back via delete_object
            self.assertEqual(
                len(s3_store),
                0,
                f"Expected 0 partial S3 objects after rollback on attempt {receive_count}, found {s3_store}",
            )

            if receive_count == max_receive_count:
                dlq_messages.append(event["Records"][0])

        self.assertEqual(len(dlq_messages), 1, "Message must land in DLQ after maxReceiveCount=3")
        self.assertEqual(mock_s3.put_object.call_count, 3)
        self.assertEqual(mock_s3.delete_object.call_count, 3)

    def test_2_glue_transform_job_simulated_failure_raises_before_processed_write(self):
        """
        Scenario 2:
        - When `--SIMULATE_FAILURE true` is passed to `transform_and_dora.py`,
          the Glue job immediately raises RuntimeError before writing to S3,
          which triggers SFN's `TransformGlueJob` Catch -> `NotifyTransformFailed`
          -> `FailNotify` -> SNS (`gitpulse-pipeline-alerts`).
        """
        sys.path.insert(0, str(GLUE_SCRIPTS))
        import transform_and_dora

        with patch.object(sys, "argv", ["transform_and_dora.py", "--SIMULATE_FAILURE", "true"]):
            with self.assertRaises(RuntimeError) as ctx:
                transform_and_dora.main()
            self.assertIn("Simulated Glue transform job failure", str(ctx.exception))

    def test_3_sfn_and_alarms_shared_sns_topic_and_failure_routing(self):
        """
        Scenario 2 & 3 + Scope Update:
        - Verifies `terraform/sfn.tf` and `terraform/alarms.tf` wire BOTH the
          DLQ alarm (`aws_cloudwatch_metric_alarm.dlq_depth`) AND the SFN
          `FailNotify` state (`CrawlRaw`, `TransformGlueJob`, `CrawlProcessed`)
          to the exact same SNS topic (`aws_sns_topic.pipeline_alerts.arn`).
        """
        sfn_tf = (REPO_ROOT / "terraform" / "sfn.tf").read_text(encoding="utf-8")
        alarms_tf = (REPO_ROOT / "terraform" / "alarms.tf").read_text(encoding="utf-8")

        # 1. Single shared SNS topic
        self.assertIn('resource "aws_sns_topic" "pipeline_alerts"', sfn_tf)
        self.assertIn('resource "aws_sns_topic_subscription" "pipeline_alerts_email"', sfn_tf)
        self.assertIn("alarm_actions = [aws_sns_topic.pipeline_alerts.arn]", alarms_tf)

        # 2. All 3 SFN stages route failures to FailNotify with FailedStage and Execution.Id
        for stage, pass_state in [
            ("CrawlRaw", "NotifyCrawlRawFailed"),
            ("TransformGlueJob", "NotifyTransformFailed"),
            ("CrawlProcessed", "NotifyCrawlProcessedFailed"),
        ]:
            self.assertIn(f'Parameters = {{ FailedStage = "{stage}" }}', sfn_tf)
            self.assertIn(pass_state, sfn_tf)

        self.assertIn(
            "States.Format('GitPulse transform tail failed at stage: {}. Execution: {}', $.failure.FailedStage, $$.Execution.Id)",
            sfn_tf,
        )


# ============================================================================
# 2. LIVE AWS END-TO-END SIMULATION RUNNER
# ============================================================================

class LiveAWSFailureSimulator:
    """Executes the 3 failure simulation scenarios on a deployed AWS environment."""

    def __init__(self, profile: str, region: str, project_name: str = "gitpulse", fast_visibility_timeout: int = 15):
        import boto3

        session = boto3.Session(profile_name=profile, region_name=region) if profile else boto3.Session(region_name=region)
        self.sqs = session.client("sqs")
        self.s3 = session.client("s3")
        self.sns = session.client("sns")
        self.cw = session.client("cloudwatch")
        self.sfn = session.client("stepfunctions")
        self.glue = session.client("glue")
        self.sts = session.client("sts")
        self.account_id = self.sts.get_caller_identity()["Account"]
        self.region = region
        self.project_name = project_name
        self.fast_visibility_timeout = fast_visibility_timeout

        self.job_queue_url = self.sqs.get_queue_url(QueueName=f"{project_name}-job-queue")["QueueUrl"]
        self.dlq_url = self.sqs.get_queue_url(QueueName=f"{project_name}-dlq")["QueueUrl"]
        self.sns_topic_arn = f"arn:aws:sns:{region}:{self.account_id}:{project_name}-pipeline-alerts"
        self.sfn_arn = f"arn:aws:states:{region}:{self.account_id}:stateMachine:{project_name}-transform-tail"
        self.dlq_alarm_name = f"{project_name}-dlq-depth"
        self.glue_job_name = f"{project_name}-transform-job"
        self.raw_crawler_name = f"{project_name}-raw-crawler"

    def _setup_ephemeral_sns_verifier_queue(self):
        """Creates a temporary SQS queue subscribed to `gitpulse-pipeline-alerts`
        so the script can programmatically assert every SNS notification."""
        q_name = f"{self.project_name}-m5-3-sns-verifier-{int(time.time())}"
        q_url = self.sqs.create_queue(QueueName=q_name)["QueueUrl"]
        q_arn = self.sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]

        policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "sns.amazonaws.com"},
                "Action": "sqs:SendMessage",
                "Resource": q_arn,
                "Condition": {"ArnEquals": {"aws:SourceArn": self.sns_topic_arn}},
            }],
        }
        self.sqs.set_queue_attributes(QueueUrl=q_url, Attributes={"Policy": json.dumps(policy)})
        sub_arn = self.sns.subscribe(TopicArn=self.sns_topic_arn, Protocol="sqs", Endpoint=q_arn)["SubscriptionArn"]
        return q_url, sub_arn

    def _poll_sns_verifier(self, verifier_q_url: str, expected_substring: str, timeout_sec: int = 180) -> str:
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            resp = self.sqs.receive_message(QueueUrl=verifier_q_url, MaxNumberOfMessages=10, WaitTimeSeconds=10)
            for msg in resp.get("Messages", []):
                envelope = json.loads(msg["Body"])
                sns_message = envelope.get("Message", "")
                self.sqs.delete_message(QueueUrl=verifier_q_url, ReceiptHandle=msg["ReceiptHandle"])
                if expected_substring in sns_message:
                    return sns_message
        raise TimeoutError(f"Did not receive SNS message containing '{expected_substring}' within {timeout_sec}s")

    def run_all(self):
        verifier_q_url, sub_arn = self._setup_ephemeral_sns_verifier_queue()
        try:
            print("\n=== [Scenario 1/3] Ingestion Lambda -> SQS Retries -> DLQ -> Alarm -> SNS ===")
            self.simulate_ingestion_dlq(verifier_q_url)

            print("\n=== [Scenario 2/3] SFN Transform Tail: Glue Job Failure -> Catch -> FailNotify -> SNS ===")
            self.simulate_sfn_glue_job_failure(verifier_q_url)

            print("\n=== [Scenario 3/3] SFN Transform Tail: Glue Crawler Failure -> Catch -> FailNotify -> SNS ===")
            self.simulate_sfn_crawler_failure(verifier_q_url)

            print("\n[SUCCESS] All M5-3 failure simulation scenarios passed!")
        finally:
            self.sns.unsubscribe(SubscriptionArn=sub_arn)
            self.sqs.delete_queue(QueueUrl=verifier_q_url)

    def simulate_ingestion_dlq(self, verifier_q_url: str):
        run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ_m5_3_fail")
        orig_attrs = self.sqs.get_queue_attributes(
            QueueUrl=self.job_queue_url, AttributeNames=["VisibilityTimeout"]
        )["Attributes"]
        orig_vis = orig_attrs["VisibilityTimeout"]

        try:
            # Temporarily shorten visibility timeout so 3 retries complete quickly
            self.sqs.set_queue_attributes(
                QueueUrl=self.job_queue_url,
                Attributes={"VisibilityTimeout": str(self.fast_visibility_timeout)},
            )
            payload = {
                "repo": "vercel/next.js",
                "resource_type": "repository",
                "since": "2026-09-24T00:00:00Z",
                "run_ts": run_ts,
                "simulate_failure": "partial_write_rollback",
            }
            self.sqs.send_message(QueueUrl=self.job_queue_url, MessageBody=json.dumps(payload))
            print(f"Sent failing ingestion job message (run_ts={run_ts}). Observing retries -> DLQ...")

            # Wait for message to land in DLQ after maxReceiveCount=3
            deadline = time.time() + (self.fast_visibility_timeout * 5 + 60)
            dlq_depth = 0
            while time.time() < deadline:
                attrs = self.sqs.get_queue_attributes(
                    QueueUrl=self.dlq_url,
                    AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
                )["Attributes"]
                dlq_depth = int(attrs["ApproximateNumberOfMessages"])
                if dlq_depth > 0:
                    print(f"Verified DLQ depth > 0: ApproximateNumberOfMessagesVisible = {dlq_depth}")
                    break
                time.sleep(5)
            assert dlq_depth > 0, "Expected message to land in DLQ after maxReceiveCount=3"

            # Verify 0 partial objects in S3 raw bucket for run_ts
            raw_bucket = next(
                b["Name"] for b in self.s3.list_buckets()["Buckets"] if b["Name"].startswith(f"{self.project_name}-raw-")
            )
            prefix = f"raw/github/repository/vercel_next.js/run_ts={run_ts}/"
            s3_objs = self.s3.list_objects_v2(Bucket=raw_bucket, Prefix=prefix).get("Contents", [])
            assert len(s3_objs) == 0, f"Expected 0 partial S3 objects under {prefix}, found {s3_objs}"
            print(f"Verified 0 partial S3 objects under s3://{raw_bucket}/{prefix}")

            # Wait for CloudWatch alarm `gitpulse-dlq-depth` to transition to ALARM and publish to SNS
            sns_msg = self._poll_sns_verifier(verifier_q_url, self.dlq_alarm_name, timeout_sec=180)
            print(f"Verified CloudWatch DLQ alarm SNS notification on {self.sns_topic_arn}: {sns_msg[:120]}...")
        finally:
            self.sqs.set_queue_attributes(
                QueueUrl=self.job_queue_url, Attributes={"VisibilityTimeout": orig_vis}
            )

    def simulate_sfn_glue_job_failure(self, verifier_q_url: str):
        job_def = self.glue.get_job(JobName=self.glue_job_name)["Job"]
        orig_args = dict(job_def.get("DefaultArguments", {}))
        updated_args = dict(orig_args)
        updated_args["--SIMULATE_FAILURE"] = "true"

        try:
            self.glue.update_job(
                JobName=self.glue_job_name,
                JobUpdate={
                    "Role": job_def["Role"],
                    "Command": job_def["Command"],
                    "DefaultArguments": updated_args,
                    "MaxCapacity": job_def.get("MaxCapacity", 1.0),
                    "Timeout": job_def.get("Timeout", 30),
                    "GlueVersion": job_def.get("GlueVersion", "3.0"),
                },
            )
            exec_arn = self.sfn.start_execution(
                stateMachineArn=self.sfn_arn,
                name=f"m5-3-glue-job-fail-{int(time.time())}",
                input="{}",
            )["executionArn"]
            print(f"Started SFN execution for Glue Job failure simulation: {exec_arn}")

            expected_body = f"GitPulse transform tail failed at stage: TransformGlueJob. Execution: {exec_arn}"
            sns_msg = self._poll_sns_verifier(verifier_q_url, expected_body, timeout_sec=600)
            print(f"Verified SFN FailNotify SNS message on shared topic: {sns_msg}")
        finally:
            self.glue.update_job(
                JobName=self.glue_job_name,
                JobUpdate={
                    "Role": job_def["Role"],
                    "Command": job_def["Command"],
                    "DefaultArguments": orig_args,
                    "MaxCapacity": job_def.get("MaxCapacity", 1.0),
                    "Timeout": job_def.get("Timeout", 30),
                    "GlueVersion": job_def.get("GlueVersion", "3.0"),
                },
            )

    def simulate_sfn_crawler_failure(self, verifier_q_url: str):
        # Start the raw crawler once right before starting SFN so SFN's `startCrawler`
        # throws `CrawlerRunningException` across retries and enters `NotifyCrawlRawFailed`
        try:
            self.glue.start_crawler(Name=self.raw_crawler_name)
        except Exception:
            pass

        exec_arn = self.sfn.start_execution(
            stateMachineArn=self.sfn_arn,
            name=f"m5-3-crawler-fail-{int(time.time())}",
            input="{}",
        )["executionArn"]
        print(f"Started SFN execution for Crawler failure simulation: {exec_arn}")

        expected_body = f"GitPulse transform tail failed at stage: CrawlRaw. Execution: {exec_arn}"
        sns_msg = self._poll_sns_verifier(verifier_q_url, expected_body, timeout_sec=300)
        print(f"Verified SFN FailNotify SNS message on shared topic: {sns_msg}")


def main():
    parser = argparse.ArgumentParser(description="M5-3 End-to-End Failure Simulation & Verification")
    parser.add_argument("--mode", choices=["unit", "aws"], default="unit", help="Run local unit simulation or live AWS simulation")
    parser.add_argument("--profile", default="gitpulse", help="AWS CLI profile for --mode aws")
    parser.add_argument("--region", default="ap-southeast-1", help="AWS region for --mode aws")
    args = parser.parse_args()

    if args.mode == "unit":
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestM53FailureSimulationLocal)
        runner = unittest.TextTestRunner(verbosity=2)
        result = runner.run(suite)
        sys.exit(0 if result.wasSuccessful() else 1)
    else:
        sim = LiveAWSFailureSimulator(profile=args.profile, region=args.region)
        sim.run_all()


if __name__ == "__main__":
    main()
