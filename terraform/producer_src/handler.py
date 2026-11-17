import json
import os
from datetime import datetime, timedelta, timezone

import boto3

QUEUE_URL = os.environ["JOB_QUEUE_URL"]
REPOS = [r.strip() for r in os.environ["GITHUB_REPOS"].split(",") if r.strip()]
LOOKBACK_HOURS = int(os.environ.get("LOOKBACK_HOURS", "24"))

# FR-1 (revised, PRD §11.3): the 8 resource types the system must collect
RESOURCE_TYPES = [
    "repository",
    "commits",
    "issues",
    "pull_requests",
    "releases",
    "contributors",
    "deployments",
    "workflow_runs",
]

_sqs = boto3.client("sqs")


def handler(event, context):
    """FR-3: enumerate (repo, resource_type) pairs and enqueue one SQS
    message per unit of work. Never calls the GitHub API.
    Supports optional event overrides for since, until, resource_types, and repos
    for backfilling or targeted workflow_runs execution."""
    event = event or {}

    event_repos = event.get("repos")
    if isinstance(event_repos, list):
        target_repos = event_repos
    elif isinstance(event_repos, str) and event_repos.strip():
        target_repos = [r.strip() for r in event_repos.split(",") if r.strip()]
    else:
        target_repos = REPOS

    event_resources = event.get("resource_types")
    if isinstance(event_resources, list):
        target_resources = event_resources
    elif isinstance(event_resources, str) and event_resources.strip():
        target_resources = [r.strip() for r in event_resources.split(",") if r.strip()]
    else:
        target_resources = RESOURCE_TYPES

    since = event.get("since") or (datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)).isoformat()
    until = event.get("until")
    run_ts = event.get("run_ts") or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    write_manifest = event.get("write_manifest", False)

    sent = 0
    for repo in target_repos:
        for resource_type in target_resources:
            message = {
                "repo": repo,
                "resource_type": resource_type,
                "since": since,
                "run_ts": run_ts,
            }
            if until:
                message["until"] = until
            if write_manifest:
                message["write_manifest"] = True

            _sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(message))
            sent += 1

    # Structured log for verification (FR-9 / AC: no GitHub API calls made)
    print(json.dumps({
        "event": "producer_run_complete",
        "run_ts": run_ts,
        "repos": len(target_repos),
        "resource_types": len(target_resources),
        "messages_enqueued": sent,
        "github_api_calls": 0,
        "since": since,
        "until": until,
    }))

    return {"messages_enqueued": sent, "run_ts": run_ts}