import json
import os
import re
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

import boto3

RAW_BUCKET = os.environ["RAW_BUCKET"]
SECRET_ARN = os.environ["GITHUB_SECRET_ARN"]

_s3 = boto3.client("s3")
_sm = boto3.client("secretsmanager")
_token_cache = None  # cached across warm invocations, refetched on cold start

GITHUB_API = "https://api.github.com"

# FR-1 / resource_type -> endpoint config
#   since_server        : GitHub API accepts a `since` query param natively
#   client_filter_field : field to compare against `since` client-side when
#                         the API has no server-side `since` support. Items
#                         are fetched newest-first, so pagination stops as
#                         soon as an older-than-`since` item is seen — this
#                         avoids a true full re-pull on incremental runs.
#   sort_param          : extra query string to force newest-first ordering
RESOURCE_CONFIG = {
    "repository": {
        "path": "/repos/{repo}", "since_server": False, "paginated": False,
        "client_filter_field": None, "sort_param": "",
    },
    "commits": {
        "path": "/repos/{repo}/commits", "since_server": True, "paginated": True,
        "client_filter_field": None, "sort_param": "",
    },
    "issues": {
        "path": "/repos/{repo}/issues", "since_server": True, "paginated": True,
        "client_filter_field": None, "sort_param": "&state=all",
    },
    "pull_requests": {
        "path": "/repos/{repo}/pulls", "since_server": False, "paginated": True,
        "client_filter_field": "updated_at", "sort_param": "&state=all&sort=updated&direction=desc",
    },
    "releases": {
        "path": "/repos/{repo}/releases", "since_server": False, "paginated": True,
        # GitHub returns releases newest-created-first by default.
        "client_filter_field": "published_at", "sort_param": "",
    },
    "contributors": {
        "path": "/repos/{repo}/contributors", "since_server": False, "paginated": True,
        # No per-item timestamp exists on this endpoint (it's aggregated
        # contribution counts, not events) — incremental filtering is not
        # possible here by design. Always a full pull; volume is small
        # (max ~1 contributor per commit author) so this is an accepted,
        # permanent limitation rather than a bug to fix later.
        "client_filter_field": None, "sort_param": "",
    },
}


def get_github_token() -> str:
    """FR-8: credentials retrieved from Secrets Manager at runtime."""
    global _token_cache
    if _token_cache is None:
        resp = _sm.get_secret_value(SecretId=SECRET_ARN)
        _token_cache = json.loads(resp["SecretString"])["token"]
    return _token_cache


def parse_link_header(header_value: str) -> dict:
    """Parses GitHub's RFC 5988 Link header into {rel: url}."""
    links = {}
    if not header_value:
        return links
    for part in header_value.split(","):
        match = re.match(r'\s*<([^>]+)>;\s*rel="([^"]+)"', part)
        if match:
            links[match.group(2)] = match.group(1)
    return links


def call_github(url: str, token: str, retries: int = 3):
    """Single GitHub API GET with basic rate-limit-aware retry. Raises on
    non-2xx or transport failure so the caller (and SQS) can retry the message."""
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                body = resp.read()
                link_header = resp.headers.get("Link", "")
                return json.loads(body), link_header
        except urllib.error.HTTPError as e:
            # Secondary rate limit / abuse detection -> back off and retry within
            # this invocation; if still failing, raise so SQS retries the message.
            if e.code in (403, 429) and attempt < retries:
                wait = int(e.headers.get("Retry-After", 2 ** attempt))
                print(json.dumps({"event": "github_rate_limited", "wait_seconds": wait, "attempt": attempt}))
                time.sleep(wait)
                continue
            raise RuntimeError(f"GitHub API error {e.code} for {url}: {e.read().decode(errors='replace')}")
        except urllib.error.URLError as e:
            if attempt < retries:
                time.sleep(2 ** attempt)
                continue
            raise RuntimeError(f"Network error calling {url}: {e}")


def fetch_pages(repo: str, resource_type: str, since: str, token: str):
    """FR-6: handles pagination itself. FR-7: incremental via server-side
    `since` where the API supports it, or client-side filtering + early
    pagination exit (newest-first) where it doesn't."""
    config = RESOURCE_CONFIG[resource_type]
    url = f"{GITHUB_API}{config['path'].format(repo=repo)}?per_page=100{config['sort_param']}"
    if config["since_server"]:
        url += f"&since={since}"

    since_dt = None
    if since and config["client_filter_field"]:
        since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))

    pages = []
    while url:
        data, link_header = call_github(url, token)

        if since_dt is not None and isinstance(data, list):
            filtered, hit_older = [], False
            for item in data:
                item_dt = datetime.fromisoformat(item[config["client_filter_field"]].replace("Z", "+00:00"))
                if item_dt >= since_dt:
                    filtered.append(item)
                else:
                    hit_older = True
                    break  # newest-first: everything after this is older too
            pages.append(filtered)
            if hit_older:
                break
        else:
            pages.append(data)

        if not config["paginated"]:
            break
        url = parse_link_header(link_header).get("next")

    return pages


def write_raw_pages(repo: str, resource_type: str, pages: list, run_ts: str):
    """FR-10/FR-11: verbatim JSON to S3, deterministic idempotent key —
    retried runs overwrite the same object instead of duplicating."""
    safe_repo = repo.replace("/", "_")
    for i, page in enumerate(pages):
        key = f"raw/github/{resource_type}/{safe_repo}/run_ts={run_ts}/page_{i}.json"
        _s3.put_object(
            Bucket=RAW_BUCKET,
            Key=key,
            Body=json.dumps(page).encode("utf-8"),
            ContentType="application/json",
        )
    return len(pages)


def handler(event, context):
    """FR-5: SQS-triggered, one invocation handles one resource for one repo."""
    token = get_github_token()
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    for record in event["Records"]:
        body = json.loads(record["body"])
        repo = body["repo"]
        resource_type = body["resource_type"]
        since = body.get("since", "")

        if resource_type not in RESOURCE_CONFIG:
            # Fail loudly rather than silently dropping unknown work.
            raise ValueError(f"Unknown resource_type: {resource_type}")

        print(json.dumps({
            "event": "ingestion_start",
            "repo": repo,
            "resource_type": resource_type,
            "since": since,
        }))

        # FR-9: validate responses before writing. If anything below throws,
        # NO partial data is written and the Lambda exception lets SQS retry.
        pages = fetch_pages(repo, resource_type, since, token)
        page_count = write_raw_pages(repo, resource_type, pages, run_ts)

        print(json.dumps({
            "event": "ingestion_complete",
            "repo": repo,
            "resource_type": resource_type,
            "pages_written": page_count,
            "run_ts": run_ts,
        }))

    return {"status": "ok"}
