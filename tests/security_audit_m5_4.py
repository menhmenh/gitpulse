#!/usr/bin/env python3
"""
M5-4 Final IAM & Security Audit Verification Suite
==================================================

Verifies all Acceptance Criteria & Scope Updates for M5-4 (PRD FR-24, FR-25,
NFR-5, DoD §8.7 & §8.8, Architecture §6 & §12.6):

1. All IAM roles follow least-privilege:
   - Zero stray `*` in IAM role policy `Action` lists (`iam.tf`, `glue.tf`, `sfn.tf`).
   - `aws_iam_role.sfn_transform_tail` has ONLY `glue:StartCrawler`, `glue:GetCrawler`,
     `glue:StartJobRun`, `glue:GetJobRun`, `sns:Publish` on the single topic (plus KMS
     `kms:Decrypt`, `kms:GenerateDataKey` on the CMK for the encrypted SNS topic),
     with zero stray `*` in actions or resources.
   - `aws_iam_role.github_ingestion_lambda` S3 write policy (`s3:PutObject`, `s3:DeleteObject`)
     covers all 8 resource types including `deployments` and `workflow_runs` via
     `local.github_resource_types` without over-scoping S3 write paths.

2. Zero hardcoded credentials / tokens in code + Terraform:
   - Scanning all source/Terraform files for GitHub token prefixes returns 0 results.

3. GitHub token is accessed ONLY through Secrets Manager at runtime (confirmed via logs):
   - `get_github_token()` fetches from `secretsmanager` via `GITHUB_SECRET_ARN` and emits
     structured CloudWatch audit log `{"event": "github_token_loaded", "source": "secretsmanager"}`
     without leaking the secret value.

4. Athena results bucket (`athena_results`) is encrypted with KMS:
   - Verified at both S3 bucket level (`aws:kms` with `aws_kms_key.data_lake.arn`) and
     Athena Workgroup level (`SSE_KMS` with `enforce_workgroup_configuration = true`).
"""

import io
import json
import os
import re
import sys
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import MagicMock

REPO_ROOT = Path(__file__).resolve().parent.parent
TERRAFORM_DIR = REPO_ROOT / "terraform"
LAMBDA_SRC = TERRAFORM_DIR / "lambda_src"


class TestM54SecurityAudit(unittest.TestCase):
    def test_1_no_hardcoded_github_tokens_across_repo(self):
        """AC 2: grep for GitHub token prefixes returns 0 results in code + Terraform."""
        # Construct prefixes dynamically so this test file itself never matches grep -r
        token_prefixes = [
            "gh" + "p_",
            "gh" + "o_",
            "gh" + "s_",
            "gh" + "r_",
            "github_" + "pat_",
        ]
        matches = []
        for path in REPO_ROOT.rglob("*"):
            if not path.is_file():
                continue
            if any(part in path.parts for part in (".git", ".terraform", ".build", "__pycache__")):
                continue
            try:
                content = path.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue
            for prefix in token_prefixes:
                if prefix in content:
                    matches.append((str(path.relative_to(REPO_ROOT)), prefix))

        self.assertEqual(matches, [], f"Found hardcoded GitHub token prefix matches: {matches}")

    def test_2_no_stray_wildcards_in_iam_role_policy_actions(self):
        """AC 1: All IAM role policies (`iam.tf`, `glue.tf`, `sfn.tf`) have no stray `*` in Action."""
        for tf_file in ("iam.tf", "glue.tf", "sfn.tf"):
            content = (TERRAFORM_DIR / tf_file).read_text(encoding="utf-8")
            # Find all Action = [...] or Action = "..." inside aws_iam_role_policy blocks
            wildcard_actions = re.findall(r'Action\s*=\s*\[[^\]]*\*[^\]]*\]|Action\s*=\s*"[^"]*\*[^"]*"', content)
            self.assertEqual(
                wildcard_actions,
                [],
                f"Found stray wildcard `*` in Action inside {tf_file}: {wildcard_actions}",
            )

    def test_3_sfn_transform_tail_role_least_privilege(self):
        """Scope Update: `aws_iam_role.sfn_transform_tail` has only the 4 Glue actions + sns:Publish
        on the one topic (+ KMS Decrypt/GenerateDataKey for encrypted topic) and zero `*`."""
        sfn_tf = (TERRAFORM_DIR / "sfn.tf").read_text(encoding="utf-8")

        self.assertIn('"glue:StartCrawler", "glue:GetCrawler"', sfn_tf)
        self.assertIn("Resource = [aws_glue_crawler.raw.arn, aws_glue_crawler.processed.arn]", sfn_tf)
        self.assertIn('"glue:StartJobRun", "glue:GetJobRun"', sfn_tf)
        self.assertIn("Resource = aws_glue_job.transform.arn", sfn_tf)
        self.assertIn('Action   = ["sns:Publish"]', sfn_tf)
        self.assertIn("Resource = aws_sns_topic.pipeline_alerts.arn", sfn_tf)

        # Ensure no `*` exists anywhere in the sfn_transform_tail IAM policy blocks
        sfn_policies = re.findall(
            r'resource\s+"aws_iam_role_policy"\s+"sfn_transform_tail_[^"]+"\s+\{.*?\n\}',
            sfn_tf,
            flags=re.DOTALL,
        )
        self.assertEqual(len(sfn_policies), 3, "Expected 3 scoped policies on sfn_transform_tail role")
        for block in sfn_policies:
            self.assertNotIn("*", block, f"Found stray `*` in sfn_transform_tail policy:\n{block}")

    def test_4_ingestion_lambda_s3_write_covers_deployments_and_workflow_runs_without_overscoping(self):
        """Scope Update: Ingestion Lambda S3 policy covers `deployments` and `workflow_runs`
        via `local.github_resource_types` without over-scoping S3 write paths."""
        iam_tf = (TERRAFORM_DIR / "iam.tf").read_text(encoding="utf-8")
        glue_tf = (TERRAFORM_DIR / "glue.tf").read_text(encoding="utf-8")

        for rt in (
            "repository", "commits", "issues", "pull_requests",
            "releases", "contributors", "deployments", "workflow_runs",
        ):
            self.assertIn(f'"{rt}"', glue_tf)

        self.assertIn(
            'for rt in local.github_resource_types :\n        "${aws_s3_bucket.this["raw"].arn}/raw/github/${rt}/*"',
            iam_tf,
        )
        self.assertIn("Resource = aws_secretsmanager_secret.github_token.arn", iam_tf)

    def test_5_github_token_accessed_only_via_secrets_manager_and_logged_at_runtime(self):
        """AC 3: GitHub token is retrieved from Secrets Manager at runtime and confirmed via structured log."""
        os.environ["RAW_BUCKET"] = "gitpulse-raw-test"
        os.environ["GITHUB_SECRET_ARN"] = "arn:aws:secretsmanager:ap-southeast-1:123456789012:secret:gitpulse/github-token"

        if "boto3" not in sys.modules:
            sys.modules["boto3"] = MagicMock()

        sys.path.insert(0, str(LAMBDA_SRC))
        import handler as ingestion_handler

        mock_sm = MagicMock()
        secret_val = "runtime_secret_value_not_logged"
        mock_sm.get_secret_value.return_value = {"SecretString": json.dumps({"token": secret_val})}
        ingestion_handler._sm = mock_sm
        ingestion_handler._token_cache = None

        buf = io.StringIO()
        with redirect_stdout(buf):
            token = ingestion_handler.get_github_token()

        self.assertEqual(token, secret_val)
        log_output = buf.getvalue().strip()
        log_record = json.loads(log_output)
        self.assertEqual(log_record["event"], "github_token_loaded")
        self.assertEqual(log_record["source"], "secretsmanager")
        self.assertEqual(log_record["secret_arn"], os.environ["GITHUB_SECRET_ARN"])
        self.assertNotIn(secret_val, log_output, "Secret token value must NEVER appear in logs")

    def test_6_athena_results_bucket_encrypted_with_kms(self):
        """AC 4: The Athena results bucket is encrypted with KMS at both S3 bucket and Athena workgroup levels."""
        s3_tf = (TERRAFORM_DIR / "s3.tf").read_text(encoding="utf-8")
        athena_tf = (TERRAFORM_DIR / "athena.tf").read_text(encoding="utf-8")

        self.assertIn("athena_results", s3_tf)
        self.assertIn('sse_algorithm     = "aws:kms"', s3_tf)
        self.assertIn("kms_master_key_id = aws_kms_key.data_lake.arn", s3_tf)
        self.assertIn('encryption_option = "SSE_KMS"', athena_tf)
        self.assertIn("kms_key_arn       = aws_kms_key.data_lake.arn", athena_tf)
        self.assertIn("enforce_workgroup_configuration    = true", athena_tf)


if __name__ == "__main__":
    unittest.main(verbosity=2)
