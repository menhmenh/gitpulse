import io
import json
from datetime import datetime, timezone

import boto3
import numpy as np
import pandas as pd

try:
    from awsglue.utils import getResolvedOptions
    import sys
    args = getResolvedOptions(sys.argv, [
        "RAW_BUCKET", "PROCESSED_BUCKET", "REPOS",
        "DORA_FAILURE_LABELS", "DORA_REVERT_PREFIXES", "DORA_PROD_ENVIRONMENTS",
    ])
except Exception:
    import os
    args = {
        "RAW_BUCKET": os.environ["RAW_BUCKET"],
        "PROCESSED_BUCKET": os.environ["PROCESSED_BUCKET"],
        "REPOS": os.environ["REPOS"],
        "DORA_FAILURE_LABELS": os.environ.get("DORA_FAILURE_LABELS", "bug,incident,hotfix"),
        "DORA_REVERT_PREFIXES": os.environ.get("DORA_REVERT_PREFIXES", "revert,hotfix:"),
        "DORA_PROD_ENVIRONMENTS": os.environ.get("DORA_PROD_ENVIRONMENTS", "production,prod"),
    }

RAW_BUCKET = args["RAW_BUCKET"]
PROCESSED_BUCKET = args["PROCESSED_BUCKET"]
REPOS = [r.strip() for r in args["REPOS"].split(",") if r.strip()]

# FR-31: failure-signal proxy, configurable via Terraform (see variables.tf)
FAILURE_LABELS = {l.strip().lower() for l in args["DORA_FAILURE_LABELS"].split(",") if l.strip()}
REVERT_PREFIXES = tuple(p.strip().lower() for p in args["DORA_REVERT_PREFIXES"].split(",") if p.strip())
PROD_ENVIRONMENTS = {e.strip().lower() for e in args["DORA_PROD_ENVIRONMENTS"].split(",") if e.strip()}

_s3 = boto3.client("s3")

# FR-1 (revised) / resource_type -> transform config
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

# Known list-typed nested fields flattened to scalar derivatives instead of
# falling through to the raw-JSON-string fallback in cast_types().
ARRAY_FIELD_REDUCERS = {
    "labels": lambda v: ",".join(x.get("name", "") for x in v) if isinstance(v, list) else "",
    "assignees": lambda v: ",".join(x.get("login", "") for x in v) if isinstance(v, list) else "",
    "requested_reviewers": lambda v: ",".join(x.get("login", "") for x in v) if isinstance(v, list) else "",
    "requested_teams": lambda v: ",".join(x.get("name", "") for x in v) if isinstance(v, list) else "",
    "parents": lambda v: ",".join(x.get("sha", "") for x in v) if isinstance(v, list) else "",
    "topics": lambda v: ",".join(str(x) for x in v) if isinstance(v, list) else "",
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
        # Guard: skip anything that isn't a well-formed page object (folder
        # markers, stray non-JSON files, keys missing the run_ts= segment).
        if "run_ts=" not in key or not key.endswith(".json"):
            print(json.dumps({"event": "skip_malformed_key", "key": key}))
            continue

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
        elif lower in ("id", "number", "comments", "additions", "deletions", "changed_files",
                       "stargazers_count", "forks_count", "contributions"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif df[col].dtype == object:
            # Any remaining nested dict/list (rare/unmapped fields) -> JSON
            # string, rather than a raw Python object Parquet can't store.
            # Known array fields are handled by reduce_array_fields() BEFORE
            # this runs, so reaching here means it's genuinely unmapped.
            df[col] = df[col].apply(lambda v: json.dumps(v) if isinstance(v, (dict, list)) else v)
            df[col] = df[col].astype("string")
    return df


def dedupe(df: pd.DataFrame, resource_type: str) -> pd.DataFrame:
    """FR-13: dedup by sha/id, keeping the most recently ingested version."""
    config = RESOURCE_CONFIG[resource_type]
    id_col = config["id_field"]
    if id_col not in df.columns:
        return df
    df = df.sort_values("_run_ts", ascending=False)
    return df.drop_duplicates(subset=[id_col], keep="first")


def partition_date(df: pd.DataFrame, resource_type: str) -> pd.DataFrame:
    """FR-14: partition by the record's own business date; contributors has
    none (documented limitation, see ingestion ticket) -> falls back to
    today's run date."""
    config = RESOURCE_CONFIG[resource_type]
    date_field = config["date_field"]
    if date_field and date_field in df.columns:
        df["dt"] = pd.to_datetime(df[date_field], errors="coerce", utc=True).dt.strftime("%Y-%m-%d")
        df["dt"] = df["dt"].fillna(datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    else:
        df["dt"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return df


def write_parquet_partitioned(df: pd.DataFrame, resource_type: str):
    out = df.drop(columns=["_run_ts"], errors="ignore").rename(columns={"_repo": "repo"})
    for dt, group in out.groupby("dt"):
        buf = io.BytesIO()
        group.drop(columns=["dt"]).to_parquet(buf, engine="pyarrow", index=False)
        buf.seek(0)
        key = f"{resource_type}/dt={dt}/part-{resource_type}-{dt}.parquet"
        _s3.put_object(Bucket=PROCESSED_BUCKET, Key=key, Body=buf.getvalue())
        print(json.dumps({"event": "partition_written", "resource_type": resource_type, "dt": dt, "rows": len(group)}))


def transform_resource(resource_type: str) -> pd.DataFrame:
    frames = []
    for repo in REPOS:
        df = load_raw_records(resource_type, repo)
        if not df.empty:
            frames.append(df)
    if not frames:
        print(json.dumps({"event": "transform_skip_empty", "resource_type": resource_type}))
        return pd.DataFrame()

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
    return df


# ---------------------------------------------------------------------------
# FR-29/FR-31: dora_metrics derived table (PRD §11.1/§11.3, Architecture §11.2/§11.3)
# Output columns: dt, repo, environment, deploy_count, lead_time_p50,
# lead_time_p95, change_failure_rate, mttr_minutes
# ---------------------------------------------------------------------------

def build_dora_metrics(commits_df, prs_df, deployments_df, issues_df) -> pd.DataFrame:
    if deployments_df is None or deployments_df.empty:
        print(json.dumps({"event": "dora_skip_no_deployments"}))
        return pd.DataFrame()

    dep = deployments_df.copy()
    dep["environment"] = dep.get("environment", pd.Series(dtype=object)).fillna("production").astype(str)
    dep["created_at"] = pd.to_datetime(dep["created_at"], utc=True, errors="coerce")
    # Scope: only production-like environments feed DF/LT/CFR/MTTR.
    dep = dep[dep["environment"].str.lower().isin(PROD_ENVIRONMENTS)]
    if dep.empty:
        print(json.dumps({"event": "dora_skip_no_prod_deployments"}))
        return pd.DataFrame()
    dep["dt"] = dep["created_at"].dt.strftime("%Y-%m-%d")

    # --- Deployment Frequency: deploy_count per repo/environment/dt ---
    deploy_counts = dep.groupby(["_repo", "environment", "dt"]).size().reset_index(name="deploy_count")

    # --- Lead Time: deployment.created_at - nearest prior merged PR.merged_at, same repo ---
    # p50/p95 per (repo, environment, dt) as specified.
    lead_times = []
    if prs_df is not None and not prs_df.empty and "merged_at" in prs_df.columns:
        merged = prs_df[prs_df["merged_at"].notna()].copy()
        merged["merged_at"] = pd.to_datetime(merged["merged_at"], utc=True, errors="coerce")
        for _, d in dep.iterrows():
            candidates = merged[(merged["_repo"] == d["_repo"]) & (merged["merged_at"] <= d["created_at"])]
            if not candidates.empty:
                nearest = candidates.loc[candidates["merged_at"].idxmax()]
                lead_hours = (d["created_at"] - nearest["merged_at"]).total_seconds() / 3600
                lead_times.append({"_repo": d["_repo"], "environment": d["environment"], "dt": d["dt"], "lead_time_hours": lead_hours})
    lt_df = pd.DataFrame(lead_times)
    if not lt_df.empty:
        lt_agg = lt_df.groupby(["_repo", "environment", "dt"])["lead_time_hours"].agg(
            lead_time_p50=lambda s: float(np.percentile(s, 50)),
            lead_time_p95=lambda s: float(np.percentile(s, 95)),
        ).reset_index()
    else:
        lt_agg = pd.DataFrame(columns=["_repo", "environment", "dt", "lead_time_p50", "lead_time_p95"])

    # --- Failure signal (FR-31 proxy, configurable) ---
    # Issues: open_at = created_at, closed_at = closed_at.
    # Revert/hotfix PRs: open_at = merged_at, closed_at = the NEXT successful
    # prod deployment's created_at after the revert was merged (recovery via
    # redeploy), not the merge time itself — per this ticket's spec.
    failures = []
    if issues_df is not None and not issues_df.empty and "labels" in issues_df.columns:
        for _, issue in issues_df.iterrows():
            issue_labels = [l.strip().lower() for l in str(issue.get("labels", "")).split(",") if l.strip()]
            # =========================================================================
            # [ROOT CAUSE FIX 1]: Flexible substring/pattern match for issue labels
            # Previously used exact match `issue_labels & FAILURE_LABELS`, which caused
            # real-world prefixed labels like 'template: bug', 'type: bug', 'kind/bug'
            # to be completely ignored. Now checks if any label contains a failure keyword.
            # =========================================================================
            is_failure = any(any(fl in lbl for fl in FAILURE_LABELS) for lbl in issue_labels)
            if is_failure:
                o_at = pd.to_datetime(issue.get("created_at"), utc=True, errors="coerce")
                c_at = pd.to_datetime(issue.get("closed_at"), utc=True, errors="coerce")
                if pd.notna(o_at):
                    # =========================================================================
                    # [ROOT CAUSE FIX 2A]: Assign recovery date partition (dt) for Issue
                    # For closed issues, recovery finishes at closed_at. Assigning dt to
                    # closed_at (or created_at if still open) correctly aligns recovery
                    # with deployment date and avoids dropping resolved incidents.
                    # =========================================================================
                    res_dt = c_at.strftime("%Y-%m-%d") if pd.notna(c_at) else o_at.strftime("%Y-%m-%d")
                    failures.append({
                        "_repo": issue["_repo"],
                        "open_at": o_at,
                        "closed_at": c_at,
                        "dt": res_dt,
                    })

    if prs_df is not None and not prs_df.empty and "title" in prs_df.columns:
        for _, pr in prs_df.iterrows():
            title = str(pr.get("title", "")).strip().lower()
            if pr.get("merged_at") and title.startswith(REVERT_PREFIXES):
                merged_at = pd.to_datetime(pr["merged_at"], utc=True, errors="coerce")
                # Recovery time = next prod deployment after this revert merged.
                candidates = dep[(dep["_repo"] == pr["_repo"]) & (dep["created_at"] >= merged_at)]
                recovery = candidates["created_at"].min() if not candidates.empty else pd.NaT
                # =========================================================================
                # [ROOT CAUSE FIX 2B]: Align Revert PR recovery date with Deployment date
                # A revert is recovered when the next prod deployment successfully rolls out.
                # Assigning dt to the recovery deployment date (recovery.dt) ensures MTTR
                # and CFR are properly attributed to that deployment, avoiding date mismatch.
                # =========================================================================
                res_dt = recovery.strftime("%Y-%m-%d") if pd.notna(recovery) else merged_at.strftime("%Y-%m-%d")
                failures.append({
                    "_repo": pr["_repo"],
                    "open_at": merged_at,
                    "closed_at": recovery,
                    "dt": res_dt,
                })

    fail_df = pd.DataFrame(failures)
    if not fail_df.empty:
        cfr_counts = fail_df.groupby(["_repo", "dt"]).size().reset_index(name="failure_count")

        # =========================================================================
        # [ROOT CAUSE FIX 3]: Compute MTTR only on valid resolved incidents
        # Filter failures with non-null closed_at and closed_at >= open_at.
        # =========================================================================
        resolved = fail_df[fail_df["closed_at"].notna() & (fail_df["closed_at"] >= fail_df["open_at"])].copy()
        if not resolved.empty:
            resolved["mttr_minutes"] = (resolved["closed_at"] - resolved["open_at"]).dt.total_seconds() / 60
            mttr_agg = resolved.groupby(["_repo", "dt"])["mttr_minutes"].mean().round(2).reset_index()
        else:
            mttr_agg = pd.DataFrame(columns=["_repo", "dt", "mttr_minutes"])
    else:
        cfr_counts = pd.DataFrame(columns=["_repo", "dt", "failure_count"])
        mttr_agg = pd.DataFrame(columns=["_repo", "dt", "mttr_minutes"])

    # =========================================================================
    # [ROOT CAUSE FIX 4]: Use Outer Join instead of Left Join
    # Previously, a left join from deploy_counts discarded any day that had incidents
    # or MTTR but zero deployments. Outer join preserves both deployment days and incident days.
    # =========================================================================
    prod_env = list(PROD_ENVIRONMENTS)[0] if PROD_ENVIRONMENTS else "production"
    deploy_summary = deploy_counts.merge(lt_agg, on=["_repo", "environment", "dt"], how="outer")
    failure_summary = cfr_counts.merge(mttr_agg, on=["_repo", "dt"], how="outer")

    result = deploy_summary.merge(failure_summary, on=["_repo", "dt"], how="outer")
    result["environment"] = result["environment"].fillna(prod_env)
    result["deploy_count"] = result["deploy_count"].fillna(0).astype(int)
    result["failure_count"] = result["failure_count"].fillna(0)

    # Change Failure Rate (% failed deployments, division-by-zero safe when deploy_count = 0)
    result["change_failure_rate"] = np.where(
        result["deploy_count"] > 0,
        (result["failure_count"] / result["deploy_count"] * 100).round(2),
        np.nan
    )
    result = result.rename(columns={"_repo": "repo"})

    return result[[
        "dt", "repo", "environment", "deploy_count",
        "lead_time_p50", "lead_time_p95", "change_failure_rate", "mttr_minutes",
    ]]


def write_dora_metrics(df: pd.DataFrame):
    if df.empty:
        print(json.dumps({"event": "dora_metrics_skip_empty"}))
        return
    for dt, group in df.groupby("dt"):
        buf = io.BytesIO()
        group.drop(columns=["dt"]).to_parquet(buf, engine="pyarrow", index=False)
        buf.seek(0)
        key = f"dora_metrics/dt={dt}/part-dora-{dt}.parquet"
        _s3.put_object(Bucket=PROCESSED_BUCKET, Key=key, Body=buf.getvalue())
    print(json.dumps({"event": "dora_metrics_written", "rows": len(df), "partitions": df["dt"].nunique()}))


def main():
    results = {rt: transform_resource(rt) for rt in RESOURCE_CONFIG}

    dora_df = build_dora_metrics(
        commits_df=results.get("commits"),
        prs_df=results.get("pull_requests"),
        deployments_df=results.get("deployments"),
        issues_df=results.get("issues"),
    )
    write_dora_metrics(dora_df)

    print(json.dumps({"event": "job_complete"}))


if __name__ == "__main__":
    main()
