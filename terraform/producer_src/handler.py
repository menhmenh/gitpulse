import json
import os
from datetime import datetime, timedelta, timezone

import boto3

QUEUE_URL = os.environ["JOB_QUEUE_URL"]
REPOS = [r.strip() for r in os.environ["GITHUB_REPOS"].split(",") if r.strip()]
LOOKBACK_HOURS = int(os.environ.get("LOOKBACK_HOURS", "24"))

# FR-1: the 6 resource types the system must collect
RESOURCE_TYPES = [
    "repository",
    "commits",
    "issues",
    "pull_requests",
    "releases",
    "contributors",
]

_sqs = boto3.client("sqs")


def handler(event, context):
    """FR-3: enumerate (repo, resource_type) pairs and enqueue one SQS
    message per unit of work. Never calls the GitHub API."""
    since = (datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)).isoformat()
    # One run_ts per batch shared by every message so all resulting raw
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    sent = 0
    for repo in REPOS:
        for resource_type in RESOURCE_TYPES:
            message = {
                "repo": repo,
                "resource_type": resource_type,
                "since": since,
                "run_ts": run_ts,
            }
            _sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(message))
            sent += 1

    # Structured log for verification 
    print(json.dumps({
        "event": "producer_run_complete",
        "run_ts": run_ts,
        "repos": len(REPOS),
        "resource_types": len(RESOURCE_TYPES),
        "messages_enqueued": sent,
        "github_api_calls": 0,
    }))

    return {"messages_enqueued": sent, "run_ts": run_ts}