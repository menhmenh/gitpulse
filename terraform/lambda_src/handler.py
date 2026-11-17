import hashlib
import json
import os
import re
import time
import urllib.request
import urllib.error
from urllib.parse import urlencode
from datetime import datetime, timedelta, timezone

import boto3

RAW_BUCKET = os.environ["RAW_BUCKET"]
SECRET_ARN = os.environ["GITHUB_SECRET_ARN"]

_s3 = boto3.client("s3")
_sm = boto3.client("secretsmanager")
_token_cache = None  # cached across warm invocations, refetched on cold start

GITHUB_API = "https://api.github.com"

RESOURCE_CONFIG = {
    "repository": {
        "path": "/repos/{repo}", "since_server": False, "paginated": False,
        "client_filter_field": None, "sort_param": "", "envelope_key": None,
    },
    "commits": {
        "path": "/repos/{repo}/commits", "since_server": True, "paginated": True,
        "client_filter_field": None, "sort_param": "", "envelope_key": None,
    },
    "issues": {
        "path": "/repos/{repo}/issues", "since_server": True, "paginated": True,
        "client_filter_field": None, "sort_param": "&state=all", "envelope_key": None,
    },
    "pull_requests": {
        "path": "/repos/{repo}/pulls", "since_server": False, "paginated": True,
        "client_filter_field": "updated_at", "sort_param": "&state=all&sort=updated&direction=desc",
        "envelope_key": None,
    },
    "releases": {
        "path": "/repos/{repo}/releases", "since_server": False, "paginated": True,
        # GitHub returns releases newest-created-first by default.
        "client_filter_field": "published_at", "sort_param": "", "envelope_key": None,
    },
    "contributors": {
        "path": "/repos/{repo}/contributors", "since_server": False, "paginated": True,
        "client_filter_field": None, "sort_param": "", "envelope_key": None,
    },
    "deployments": {
        "path": "/repos/{repo}/deployments", "since_server": False, "paginated": True,
        "client_filter_field": "created_at", "sort_param": "", "envelope_key": None,
    },
    "workflow_runs": {
        "path": "/repos/{repo}/actions/runs", "since_server": False, "paginated": True,
        "client_filter_field": "created_at", "sort_param": "", "envelope_key": "workflow_runs",
    },
}


def get_items(data, config: dict) -> list:
    """Extracts the actual item list from a page, handling both bare-list
    responses and envelope-wrapped ones (e.g. workflow_runs)."""
    if config.get("envelope_key"):
        return data.get(config["envelope_key"], []) or []
    return data if isinstance(data, list) else [data]


def get_github_token() -> str:
    """FR-8: credentials retrieved from Secrets Manager at runtime."""
    global _token_cache
    if _token_cache is None:
        resp = _sm.get_secret_value(SecretId=SECRET_ARN)
        _token_cache = json.loads(resp["SecretString"])["token"]
        print(json.dumps({
            "event": "github_token_loaded",
            "source": "secretsmanager",
            "secret_arn": SECRET_ARN,
        }))
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


def fetch_workflow_pages(repo: str, since: str, until: str, token: str):
    """Bound Actions searches to the original batch and split GitHub's 1,000-result cap."""
    since_dt = datetime.fromisoformat(since.replace("Z", "+00:00")).astimezone(timezone.utc)
    until_dt = datetime.fromisoformat(until.replace("Z", "+00:00")).astimezone(timezone.utc)
    if since_dt > until_dt:
        raise ValueError("Workflow run window starts after its end")

    def fetch_window(start, end):
        created = f"{start.strftime('%Y-%m-%dT%H:%M:%SZ')}..{end.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        url = f"{GITHUB_API}/repos/{repo}/actions/runs?{urlencode({'per_page': 100, 'created': created})}"
        pages = []
        while url:
            started = time.monotonic()
            print(json.dumps({"event": "github_page_start", "repo": repo,
                              "resource_type": "workflow_runs", "url": url}))
            data, link_header = call_github(url, token)
            print(json.dumps({"event": "github_page_complete", "repo": repo,
                              "resource_type": "workflow_runs", "url": url,
                              "elapsed_seconds": round(time.monotonic() - started, 3),
                              "record_count": len(data["workflow_runs"])}))
            if data["total_count"] > 1000:
                seconds = int((end - start).total_seconds())
                if seconds == 0:
                    raise RuntimeError(f"More than 1,000 workflow runs in one second for {repo}: {created}")
                midpoint = start + timedelta(seconds=seconds // 2)
                return fetch_window(start, midpoint) + fetch_window(midpoint + timedelta(seconds=1), end)

            page = dict(data)
            # API timestamps have second precision; retain the exact since boundary.
            # Filter every item, without relying on the API's ordering.
            page["workflow_runs"] = [
                item for item in data["workflow_runs"]
                if since_dt <= datetime.fromisoformat(item["created_at"].replace("Z", "+00:00")) <= until_dt
            ]
            pages.append(page)
            url = parse_link_header(link_header).get("next")
        return pages

    return fetch_window(since_dt.replace(microsecond=0), until_dt.replace(microsecond=0))


def fetch_pages(repo: str, resource_type: str, since: str, token: str, until: str = None):
    """FR-6: handles pagination itself. FR-7: incremental via server-side
    `since` where the API supports it, or client-side filtering + early
    pagination exit (newest-first) where it doesn't."""
    if resource_type == "workflow_runs" and since:
        return fetch_workflow_pages(repo, since, until or datetime.now(timezone.utc).isoformat(), token)

    config = RESOURCE_CONFIG[resource_type]
    url = f"{GITHUB_API}{config['path'].format(repo=repo)}?per_page=100{config['sort_param']}"
    if config["since_server"]:
        url += f"&{urlencode({'since': since})}"

    since_dt = None
    if since and config["client_filter_field"]:
        since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))

    pages = []
    while url:
        data, link_header = call_github(url, token)

        if since_dt is not None:
            items = get_items(data, config)
            filtered, hit_older = [], False
            for item in items:
                item_dt = datetime.fromisoformat(item[config["client_filter_field"]].replace("Z", "+00:00"))
                if item_dt >= since_dt:
                    filtered.append(item)
                else:
                    hit_older = True
                    break  # newest-first: everything after this is older too

            if config.get("envelope_key"):
                page_out = dict(data)  # preserve other envelope fields (e.g. total_count)
                page_out[config["envelope_key"]] = filtered
            else:
                page_out = filtered
            pages.append(page_out)
            if hit_older:
                break
        else:
            pages.append(data)

        if not config["paginated"]:
            break
        url = parse_link_header(link_header).get("next")

    return pages


def write_raw_pages(
    repo: str,
    resource_type: str,
    pages: list,
    run_ts: str,
    simulate_mid_write_failure: bool = False,
    since: str = "",
    until: str = "",
    write_manifest: bool = False,
):
    safe_repo = repo.replace("/", "_")
    prefix = f"raw/github/{resource_type}/{safe_repo}/run_ts={run_ts}/"
    written_keys = []
    page_metadata = []
    total_records = 0
    config = RESOURCE_CONFIG[resource_type]

    try:
        for i, page in enumerate(pages):
            key = f"{prefix}page_{i}.json"
            body_bytes = json.dumps(page).encode("utf-8")
            checksum = hashlib.sha256(body_bytes).hexdigest()
            items = get_items(page, config)
            page_rec_count = len(items)
            total_records += page_rec_count

            _s3.put_object(
                Bucket=RAW_BUCKET,
                Key=key,
                Body=body_bytes,
                ContentType="application/json",
            )
            written_keys.append(key)
            page_metadata.append({
                "page_index": i,
                "s3_key": key,
                "filename": f"page_{i}.json",
                "record_count": page_rec_count,
                "checksum_sha256": checksum,
            })
            if simulate_mid_write_failure:
                raise RuntimeError(f"Simulated mid-write failure after writing {key} (testing atomic rollback)")

        if write_manifest:
            manifest = {
                "manifest_version": "1.0",
                "repo": repo,
                "resource_type": resource_type,
                "run_ts": run_ts,
                "since": since,
                "until": until,
                "total_records": total_records,
                "page_count": len(pages),
                "pages": page_metadata,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            manifest_key = f"{prefix}manifest.json"
            _s3.put_object(
                Bucket=RAW_BUCKET,
                Key=manifest_key,
                Body=json.dumps(manifest, indent=2).encode("utf-8"),
                ContentType="application/json",
            )
            written_keys.append(manifest_key)

    except Exception:
        for key in written_keys:
            try:
                _s3.delete_object(Bucket=RAW_BUCKET, Key=key)
            except Exception as cleanup_err:
                # Rollback itself failed — log loudly so this is visible in
                # CloudWatch even though the original exception still wins.
                print(json.dumps({
                    "event": "rollback_cleanup_failed",
                    "key": key,
                    "error": str(cleanup_err),
                }))
        raise
    return len([k for k in written_keys if not k.endswith("manifest.json")])


def handler(event, context):
    """FR-5: SQS-triggered, one invocation handles one resource for one repo."""
    token = get_github_token()

    for record in event["Records"]:
        body = json.loads(record["body"])
        repo = body["repo"]
        resource_type = body["resource_type"]
        since = body.get("since", "")
        # run_ts comes from the Producer batch, NOT generated here — this is
        # what makes a retried message overwrite the same S3 key (FR-11)
        # instead of drifting to a new run_ts on every retry attempt.
        run_ts = body["run_ts"]
        until = body.get("until")
        write_manifest = body.get("write_manifest", False)
        simulate_failure = body.get("simulate_failure", False)

        if resource_type not in RESOURCE_CONFIG:
            # Fail loudly rather than silently dropping unknown work.
            raise ValueError(f"Unknown resource_type: {resource_type}")

        print(json.dumps({
            "event": "ingestion_start",
            "repo": repo,
            "resource_type": resource_type,
            "since": since,
            "until": until,
            "run_ts": run_ts,
            "simulate_failure": simulate_failure,
            "write_manifest": write_manifest,
        }))

        if simulate_failure == "partial_write_rollback":
            # Write page_0.json to S3 and immediately fail mid-write to prove
            # that write_raw_pages rolls back all written keys leaving 0 partial data.
            write_raw_pages(
                repo,
                resource_type,
                pages=[{"simulated_partial_page": 0}],
                run_ts=run_ts,
                simulate_mid_write_failure=True,
            )

        # FR-9: validate responses before writing. If anything below throws,
        # NO partial data is written and the Lambda exception lets SQS retry.
        if resource_type == "workflow_runs" and since:
            # If until was not specified, use run_ts to keep window deterministic
            effective_until = until or datetime.strptime(run_ts[:16], "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc).isoformat()
            pages = fetch_pages(repo, resource_type, since, token, until=effective_until)
        else:
            pages = fetch_pages(repo, resource_type, since, token, until=until)

        page_count = write_raw_pages(
            repo,
            resource_type,
            pages,
            run_ts,
            since=since,
            until=until or "",
            write_manifest=write_manifest,
        )
        record_count = sum(len(get_items(p, RESOURCE_CONFIG[resource_type])) for p in pages)

        print(json.dumps({
            "event": "ingestion_complete",
            "repo": repo,
            "resource_type": resource_type,
            "pages_written": page_count,
            "record_count": record_count,  # compare across runs to verify incremental delta
            "run_ts": run_ts,
            "manifest_written": write_manifest,
        }))

    return {"status": "ok"}
