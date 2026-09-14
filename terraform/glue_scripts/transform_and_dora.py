"""
GitPulse Transform Job (Glue Python Shell, not Spark — NFR-3).

Reads all raw JSON under s3://<raw>/raw/github/<resource>/<repo>/run_ts=*/,
dedups, types, flattens, and writes date-partitioned Parquet to
s3://<processed>/<resource>/dt=YYYY-MM-DD/.

Co-deliverable (PRD §11.3 FR-29): also builds `dora_metrics` in the
processed zone from commits/pull_requests/deployments/issues, using the
proxy convention documented in FR-31.
"""
import io
import json
from datetime import datetime, timezone

import boto3
import pandas as pd

try:
    from awsglue.utils import getResolvedOptions
    import sys
    args = getResolvedOptions(sys.argv, ["RAW_BUCKET", "PROCESSED_BUCKET", "REPOS"])
except Exception:
    # Local/manual run fallback — not used in the deployed Glue job.
    import os
    args = {
        "RAW_BUCKET": os.environ["RAW_BUCKET"],
        "PROCESSED_BUCKET": os.environ["PROCESSED_BUCKET"],
        "REPOS": os.environ["REPOS"],
    }

RAW_BUCKET = args["RAW_BUCKET"]
PROCESSED_BUCKET = args["PROCESSED_BUCKET"]
REPOS = [r.strip() for r in args["REPOS"].split(",") if r.strip()]

_s3 = boto3.client("s3")

# FR-1 (revised) / resource_type -> transform config
#   id_field    : dedup key (FR-13: sha for commits, id for others)
#   date_field  : dotted path to the field used for dt= partitioning
#                 (post-flatten, dots become underscores — see FLAT_DATE_FIELD)
#   envelope_key: set when raw pages wrap the array (workflow_runs)
#   is_list     : False for repository (single object per page)
RESOURCE_CONFIG = {
    "repository":    {"id_field": "id", "date_field": "updated_at", "envelope_key": None, "is_list": False},
    "commits":       {"id_field": "sha", "date_field": "commit_author_date", "envelope_key": None, "is_list": True},
    "issues":        {"id_field": "id", "date_field": "created_at", "envelope_key": None, "is_list": True},
    "pull_requests": {"id_field": "id", "date_field": "created_at", "envelope_key": None, "is_list": True},
    "releases":      {"id_field": "id", "date_field": "published_at", "envelope_key": None, "is_list": True},
    "contributors":  {"id_field": "id", "date_field": None, "envelope_key": None, "is_list": True},
    "deployments":   {"id_field": "id", "date_field": "created_at", "envelope_key": None, "is_list": True},
    "workflow_runs": {"id_field": "id", "date_field": "created_at", "envelope_key": "workflow_runs", "is_list": True},
}

# Known list-typed nested fields that can't become Parquet columns directly —
# reduced to simple scalar derivatives instead of dropped silently.
ARRAY_FIELD_REDUCERS = {
    "labels": lambda v: ",".join(x.get("name", "") for x in v) if isinstance(v, list) else "",
    "assignees": lambda v: ",".join(x.get("login", "") for x in v) if isinstance(v, list) else "",
    "requested_reviewers": lambda v: ",".join(x.get("login", "") for x in v) if isinstance(v, list) else "",
}


def list_run_objects(resource_type: str, repo: str) -> list:
    safe_repo = repo.replace("/", "_")
    prefix = f"raw/github/{resource_type}/{safe_repo}/"
    paginator = _s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=RAW_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def load_raw_records(resource_type: str, repo: str) -> pd.DataFrame:
    """Loads every page across every run_ts for this (resource, repo),
    tagging each record with its source run_ts for recency-based dedup."""
    config = RESOURCE_CONFIG[resource_type]
    records = []
    for key in list_run_objects(resource_type, repo):
        run_ts = key.split("run_ts=")[1].split("/")[0]
        body = _s3.get_object(Bucket=RAW_BUCKET, Key=key)["Body"].read()
        data = json.loads(body)

        if config["envelope_key"]:
            items = data.get(config["envelope_key"], [])
        elif config["is_list"]:
            items = data if isinstance(data, list) else [data]
        else:
            items = [data]

        for item in items:
            item["_run_ts"] = run_ts
            item["_repo"] = repo
            records.append(item)

    if not records:
        return pd.DataFrame()
    return pd.json_normalize(records, sep="_")


def reduce_array_fields(df: pd.DataFrame) -> pd.DataFrame:
    """json_normalize flattens nested dicts but leaves list-typed fields
    (labels, assignees) as Python objects — Parquet can't store those
    reliably, so collapse them to delimited strings (FR-13: flatten)."""
    for col, reducer in ARRAY_FIELD_REDUCERS.items():
        if col in df.columns:
            df[col] = df[col].apply(reducer)
    return df


def cast_types(df: pd.DataFrame, resource_type: str) -> pd.DataFrame:
    """FR-13: explicit typing — no raw/ambiguous JSON types left."""
    for col in df.columns:
        lower = col.lower()
        if lower.endswith(("_at", "_date")) or lower == "date":
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
        elif lower in ("id", "number", "comments", "additions", "deletions", "changed_files", "stargazers_count", "forks_count", "contributions"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif df[col].dtype == object:
            # Any remaining nested dict/list (deep/rare fields) -> JSON string
            # rather than leaving a raw Python object Parquet can't serialize.
            df[col] = df[col].apply(lambda v: json.dumps(v) if isinstance(v, (dict, list)) else v)
            df[col] = df[col].astype("string")
    return df


def dedupe(df: pd.DataFrame, resource_type: str) -> pd.DataFrame:
    """FR-13: dedup by sha/id. Sort by run_ts descending first so the KEPT
    row is the most recently ingested version of that record."""
    config = RESOURCE_CONFIG[resource_type]
    id_col = config["id_field"]
    if id_col not in df.columns:
        return df
    df = df.sort_values("_run_ts", ascending=False)
    return df.drop_duplicates(subset=[id_col], keep="first")


def partition_date(df: pd.DataFrame, resource_type: str) -> pd.DataFrame:
    """FR-14: partition by date. Uses the record's own business date where
    one exists; contributors has none (documented limitation, see ingestion
    ticket) so falls back to today's run date."""
    config = RESOURCE_CONFIG[resource_type]
    date_field = config["date_field"]
    if date_field and date_field in df.columns:
        df["dt"] = pd.to_datetime(df[date_field], errors="coerce", utc=True).dt.strftime("%Y-%m-%d")
        df["dt"] = df["dt"].fillna(datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    else:
        df["dt"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return df


def write_parquet_partitioned(df: pd.DataFrame, resource_type: str):
    """FR-14/FR-15: date-partitioned Parquet to the processed zone."""
    for dt, group in df.groupby("dt"):
        buf = io.BytesIO()
        group.drop(columns=["_run_ts"], errors="ignore").to_parquet(buf, engine="pyarrow", index=False)
        buf.seek(0)
        key = f"{resource_type}/dt={dt}/part-{resource_type}-{dt}.parquet"
        _s3.put_object(Bucket=PROCESSED_BUCKET, Key=key, Body=buf.getvalue())
        print(json.dumps({"event": "partition_written", "resource_type": resource_type, "dt": dt, "rows": len(group)}))


def transform_resource(resource_type: str) -> dict:
    frames = []
    for repo in REPOS:
        df = load_raw_records(resource_type, repo)
        if not df.empty:
            frames.append(df)
    if not frames:
        print(json.dumps({"event": "transform_skip_empty", "resource_type": resource_type}))
        return {}

    df = pd.concat(frames, ignore_index=True)
    df = reduce_array_fields(df)
    df = dedupe(df, resource_type)
    df = cast_types(df, resource_type)
    df = partition_date(df, resource_type)
    write_parquet_partitioned(df, resource_type)

    print(json.dumps({
        "event": "transform_complete",
        "resource_type": resource_type,
        "record_count": len(df),
        "partitions": df["dt"].nunique(),
    }))
    return {repo: g for repo, g in df.groupby("_repo")} if "_repo" in df.columns else {"_all": df}


# ---------------------------------------------------------------------------
# FR-29/FR-31: dora_metrics derived table (PRD §11.3, Architecture §11.2)
# ---------------------------------------------------------------------------

FAILURE_LABELS = {"bug", "incident", "hotfix"}
REVERT_PREFIXES = ("revert", "hotfix:")


def build_dora_metrics(commits_df, prs_df, deployments_df, issues_df) -> pd.DataFrame:
    rows = []

    if deployments_df is None or deployments_df.empty:
        print(json.dumps({"event": "dora_skip_no_deployments"}))
        return pd.DataFrame()

    deployments_df = deployments_df.copy()
    deployments_df["environment"] = deployments_df.get("environment", "production").fillna("production")
    deployments_df["created_at"] = pd.to_datetime(deployments_df["created_at"], utc=True, errors="coerce")
    deployments_df["dt"] = deployments_df["created_at"].dt.strftime("%Y-%m-%d")

    # --- Deployment Frequency (DF): count of deployments per repo/env/dt ---
    df_counts = deployments_df.groupby(["_repo", "environment", "dt"]).size().reset_index(name="deployment_frequency")

    # --- Lead Time (LT): deployment.created_at - most recent prior merged PR.merged_at, same repo ---
    lead_times = []
    if prs_df is not None and not prs_df.empty and "merged_at" in prs_df.columns:
        merged = prs_df[prs_df["merged_at"].notna()].copy()
        merged["merged_at"] = pd.to_datetime(merged["merged_at"], utc=True, errors="coerce")
        for _, dep in deployments_df.iterrows():
            candidates = merged[(merged["_repo"] == dep["_repo"]) & (merged["merged_at"] <= dep["created_at"])]
            if not candidates.empty:
                nearest = candidates.loc[candidates["merged_at"].idxmax()]
                lead_hours = (dep["created_at"] - nearest["merged_at"]).total_seconds() / 3600
                lead_times.append({
                    "_repo": dep["_repo"], "environment": dep["environment"], "dt": dep["dt"],
                    "lead_time_hours": lead_hours,
                })
    lt_df = pd.DataFrame(lead_times)
    if not lt_df.empty:
        lt_agg = lt_df.groupby(["_repo", "environment", "dt"])["lead_time_hours"].mean().reset_index()
    else:
        lt_agg = pd.DataFrame(columns=["_repo", "environment", "dt", "lead_time_hours"])

    # --- Failure signal (FR-31 proxy): labeled issues OR revert/hotfix PRs ---
    failures = []
    if issues_df is not None and not issues_df.empty and "labels" in issues_df.columns:
        for _, issue in issues_df.iterrows():
            issue_labels = {l.strip().lower() for l in str(issue.get("labels", "")).split(",") if l.strip()}
            if issue_labels & FAILURE_LABELS:
                failures.append({
                    "_repo": issue["_repo"],
                    "open_at": issue.get("created_at"),
                    "closed_at": issue.get("closed_at"),
                })
    if prs_df is not None and not prs_df.empty and "title" in prs_df.columns:
        for _, pr in prs_df.iterrows():
            title = str(pr.get("title", "")).strip().lower()
            if pr.get("merged_at") and title.startswith(REVERT_PREFIXES):
                failures.append({"_repo": pr["_repo"], "open_at": pr.get("merged_at"), "closed_at": pr.get("merged_at")})

    fail_df = pd.DataFrame(failures)
    if not fail_df.empty:
        fail_df["open_at"] = pd.to_datetime(fail_df["open_at"], utc=True, errors="coerce")
        fail_df["dt"] = fail_df["open_at"].dt.strftime("%Y-%m-%d")
        cfr_counts = fail_df.groupby(["_repo", "dt"]).size().reset_index(name="failure_count")

        fail_df["closed_at"] = pd.to_datetime(fail_df["closed_at"], utc=True, errors="coerce")
        fail_df["mttr_hours"] = (fail_df["closed_at"] - fail_df["open_at"]).dt.total_seconds() / 3600
        mttr_agg = fail_df.groupby(["_repo", "dt"])["mttr_hours"].mean().reset_index()
    else:
        cfr_counts = pd.DataFrame(columns=["_repo", "dt", "failure_count"])
        mttr_agg = pd.DataFrame(columns=["_repo", "dt", "mttr_hours"])

    # --- Assemble final table, one row per (repo, environment, dt) ---
    result = df_counts.merge(lt_agg, on=["_repo", "environment", "dt"], how="left")
    result = result.merge(cfr_counts, on=["_repo", "dt"], how="left")
    result = result.merge(mttr_agg, on=["_repo", "dt"], how="left")
    result["failure_count"] = result["failure_count"].fillna(0)
    result["change_failure_rate_pct"] = (result["failure_count"] / result["deployment_frequency"] * 100).round(2)
    result = result.rename(columns={"_repo": "repo"})

    return result[[
        "repo", "environment", "dt", "deployment_frequency",
        "lead_time_hours", "change_failure_rate_pct", "mttr_hours",
    ]]


def write_dora_metrics(df: pd.DataFrame):
    if df.empty:
        print(json.dumps({"event": "dora_metrics_skip_empty"}))
        return
    for dt, group in df.groupby("dt"):
        buf = io.BytesIO()
        group.to_parquet(buf, engine="pyarrow", index=False)
        buf.seek(0)
        key = f"dora_metrics/dt={dt}/part-dora-{dt}.parquet"
        _s3.put_object(Bucket=PROCESSED_BUCKET, Key=key, Body=buf.getvalue())
    print(json.dumps({"event": "dora_metrics_written", "rows": len(df), "partitions": df["dt"].nunique()}))


def main():
    results = {}
    for resource_type in RESOURCE_CONFIG:
        results[resource_type] = transform_resource(resource_type)

    def flat(resource_type):
        parts = [g for g in results.get(resource_type, {}).values()]
        return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    dora_df = build_dora_metrics(
        commits_df=flat("commits"),
        prs_df=flat("pull_requests"),
        deployments_df=flat("deployments"),
        issues_df=flat("issues"),
    )
    write_dora_metrics(dora_df)

    print(json.dumps({"event": "job_complete"}))


if __name__ == "__main__":
    main()
