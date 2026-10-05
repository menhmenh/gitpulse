-- ============================================================
-- GitPulse - analytical + DORA queries (Snowflake dialect)
-- Ported from Athena/Trino: date_diff -> DATEDIFF, date(dt) -> dt is already DATE.
-- Context: USE WAREHOUSE GITPULSE_WH; USE SCHEMA GITPULSE_DB.PROCESSED;
-- ============================================================

-- 1. Commits by week
SELECT repo,
       DATE_TRUNC('week', commit_author_date) AS week,
       COUNT(*) AS commit_count
FROM commits
WHERE dt BETWEEN '2026-08-01'::DATE AND '2026-09-30'::DATE
GROUP BY repo, DATE_TRUNC('week', commit_author_date)
ORDER BY week DESC;

-- 2. Issue resolution trend by state
SELECT repo,
       state,
       DATE_TRUNC('week', created_at) AS week_opened,
       COUNT(*) AS issue_count,
       AVG(DATEDIFF('hour', created_at, closed_at)) AS avg_hours_to_close   -- open issues have NULL closed_at, ignored by AVG
FROM issues
WHERE dt BETWEEN '2026-08-01'::DATE AND '2026-09-30'::DATE
GROUP BY repo, state, DATE_TRUNC('week', created_at)
ORDER BY week_opened DESC;

-- 3. PRs by repository
SELECT repo,
       COUNT(*) AS pull_request_count,
       COUNT_IF(merged_at IS NOT NULL) AS merged_count
FROM pull_requests
WHERE dt BETWEEN '2026-08-01'::DATE AND '2026-09-30'::DATE
GROUP BY repo
ORDER BY pull_request_count DESC;

-- 4. DORA - Deployment Frequency (weekly)
SELECT repo, environment,
       DATE_TRUNC('week', dt) AS week,
       SUM(deploy_count) AS total_deployments
FROM dora_metrics            -- or dora_metrics_daily for the gap-free spine
WHERE dt BETWEEN '2026-08-01'::DATE AND '2026-09-30'::DATE
GROUP BY repo, environment, DATE_TRUNC('week', dt)
ORDER BY week DESC;

-- 5. DORA - Lead Time for Changes p50 / p95 (hours)
SELECT repo, environment, dt, lead_time_p50, lead_time_p95
FROM dora_metrics
WHERE dt BETWEEN '2026-08-01'::DATE AND '2026-09-30'::DATE
  AND lead_time_p50 IS NOT NULL
ORDER BY dt DESC;

-- 6. DORA - Change Failure Rate and MTTR
SELECT repo, environment, dt, deploy_count, change_failure_rate, mttr_minutes
FROM dora_metrics
WHERE dt BETWEEN '2026-08-01'::DATE AND '2026-09-30'::DATE
ORDER BY dt DESC;

-- ============================================================
-- Partition pruning evidence. Disable the result cache so both runs really scan,
-- then compare "Partitions scanned / Partitions total" in Query Profile (TableScan node).
-- ============================================================
ALTER SESSION SET USE_CACHED_RESULT = FALSE;

SELECT COUNT(*) FROM commits;                            -- scans every dt= partition
SELECT COUNT(*) FROM commits WHERE dt = '2026-09-16';    -- should scan only that partition

-- Same numbers without the UI (run after the two queries above):
SELECT query_text, partitions_scanned, partitions_total
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(RESULT_LIMIT => 10))
WHERE query_text ILIKE 'SELECT COUNT(*) FROM commits%'
ORDER BY start_time DESC;
