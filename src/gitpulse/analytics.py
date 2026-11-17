"""
GitPulse Analytical SQL Views and Iceberg Time-Travel Queries
=============================================================
Defines the workflow reliability analytical view and the 3 core queries:
1. 7-day failure trend
2. Flakiest / least stable workflow ranking
3. Time-travel snapshot before vs after comparison for late-arriving updates
"""

from typing import Any, Dict, List, Optional
from pyspark.sql import DataFrame, SparkSession


VIEW_NAME = "v_workflow_run_reliability"


def create_reliability_view(
    spark: SparkSession,
    table_name: str = "local.gitpulse.workflow_runs_current",
    view_name: str = VIEW_NAME,
) -> str:
    """
    Creates an analytical SQL view aggregated by (repo, workflow_name, created_date).
    Computes:
      - total_runs: total number of workflow runs
      - completed_runs: runs that reached completed status
      - success_rate: percentage of completed runs that succeeded (safe when completed_runs = 0)
      - failure_count: count of runs with conclusion = 'failure'
      - avg_latency_to_completion_minutes: minutes elapsed from created_at to updated_at for completed runs.
        (Documented as latency to completion, NOT job execution time).
    """
    view_sql = f"""
    CREATE OR REPLACE TEMPORARY VIEW {view_name} AS
    SELECT
        repo,
        name AS workflow_name,
        CAST(created_at AS DATE) AS created_date,
        COUNT(*) AS total_runs,
        COUNT_IF(status = 'completed') AS completed_runs,
        COUNT_IF(conclusion = 'failure') AS failure_count,
        ROUND(
            CAST(COUNT_IF(conclusion = 'success') AS DOUBLE) /
            NULLIF(COUNT_IF(status = 'completed'), 0) * 100.0,
            2
        ) AS success_rate_pct,
        ROUND(
            AVG(
                CASE
                    WHEN status = 'completed' AND updated_at >= created_at
                    THEN CAST((UNIX_TIMESTAMP(updated_at) - UNIX_TIMESTAMP(created_at)) AS DOUBLE) / 60.0
                    ELSE NULL
                END
            ),
            2
        ) AS avg_latency_to_completion_minutes
    FROM {table_name}
    GROUP BY
        repo,
        name,
        CAST(created_at AS DATE)
    """
    spark.sql(view_sql)
    return view_name


def query_7_day_failure_trend(
    spark: SparkSession,
    view_name: str = VIEW_NAME,
) -> DataFrame:
    """
    Query 1: 7-day failure trend across repositories and workflows.
    Returns daily failure counts and total runs.
    """
    sql = f"""
    SELECT
        repo,
        created_date,
        SUM(failure_count) AS total_failures,
        SUM(total_runs) AS total_runs,
        ROUND(
            CAST(SUM(CASE WHEN completed_runs > 0 THEN total_runs - failure_count ELSE 0 END) AS DOUBLE) /
            NULLIF(SUM(completed_runs), 0) * 100.0,
            2
        ) AS daily_success_rate
    FROM {view_name}
    GROUP BY
        repo,
        created_date
    ORDER BY
        created_date DESC,
        total_failures DESC
    """
    return spark.sql(sql)


def query_least_stable_workflows(
    spark: SparkSession,
    view_name: str = VIEW_NAME,
    limit: int = 10,
) -> DataFrame:
    """
    Query 2: Ranking of flakiest / least stable workflows by failure count and success rate.
    """
    sql = f"""
    SELECT
        repo,
        workflow_name,
        SUM(total_runs) AS total_runs,
        SUM(completed_runs) AS total_completed,
        SUM(failure_count) AS total_failures,
        ROUND(
            CAST(SUM(completed_runs) - SUM(failure_count) AS DOUBLE) /
            NULLIF(SUM(completed_runs), 0) * 100.0,
            2
        ) AS overall_success_rate,
        ROUND(AVG(avg_latency_to_completion_minutes), 2) AS avg_latency_minutes
    FROM {view_name}
    GROUP BY
        repo,
        workflow_name
    ORDER BY
        total_failures DESC,
        overall_success_rate ASC
    LIMIT {limit}
    """
    return spark.sql(sql)


def query_snapshot_time_travel_comparison(
    spark: SparkSession,
    table_name: str = "local.gitpulse.workflow_runs_current",
    snapshot_before: Optional[int] = None,
    snapshot_after: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Query 3: Time-travel comparison using Apache Iceberg snapshots.
    Compares the table metrics before and after a late-arriving batch is merged.
    """
    # If snapshots not provided, get the last two snapshots from metadata
    if snapshot_before is None or snapshot_after is None:
        snapshots_df = spark.sql(f"SELECT snapshot_id, committed_at FROM {table_name}.snapshots ORDER BY committed_at DESC LIMIT 2")
        rows = snapshots_df.collect()
        if len(rows) < 2:
            return {"error": "Need at least 2 snapshots to perform comparison"}
        snapshot_after = rows[0]["snapshot_id"]
        snapshot_before = rows[1]["snapshot_id"]

    # Read table at snapshot before
    df_before = spark.read.option("snapshot-id", snapshot_before).table(table_name)
    df_after = spark.read.option("snapshot-id", snapshot_after).table(table_name)

    df_before.createOrReplaceTempView("runs_snapshot_before")
    df_after.createOrReplaceTempView("runs_snapshot_after")

    # Metrics before
    summary_before = spark.sql("""
        SELECT
            COUNT(*) AS total_runs,
            COUNT_IF(status = 'completed') AS completed_runs,
            COUNT_IF(conclusion = 'failure') AS failure_runs,
            MIN(created_at) AS earliest_created,
            MAX(created_at) AS latest_created
        FROM runs_snapshot_before
    """).first().asDict()

    # Metrics after
    summary_after = spark.sql("""
        SELECT
            COUNT(*) AS total_runs,
            COUNT_IF(status = 'completed') AS completed_runs,
            COUNT_IF(conclusion = 'failure') AS failure_runs,
            MIN(created_at) AS earliest_created,
            MAX(created_at) AS latest_created
        FROM runs_snapshot_after
    """).first().asDict()

    return {
        "snapshot_before": {
            "snapshot_id": snapshot_before,
            "metrics": summary_before,
        },
        "snapshot_after": {
            "snapshot_id": snapshot_after,
            "metrics": summary_after,
        },
        "delta": {
            "new_runs_count": summary_after["total_runs"] - summary_before["total_runs"],
            "new_completed_count": summary_after["completed_runs"] - summary_before["completed_runs"],
            "new_failures_count": summary_after["failure_runs"] - summary_before["failure_runs"],
        },
    }
