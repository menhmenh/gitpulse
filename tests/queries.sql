-- ============================================================
-- GitPulse — Sample Analytics Queries (M4)
-- Run in Athena, workgroup: gitpulse-workgroup, database: gitpulse_processed
-- ============================================================

-- ------------------------------------------------------------
-- Query 1: Commits by week
-- FR-18 / US-DA-2
-- ------------------------------------------------------------
SELECT
    repo,
    date_trunc('week', commit_author_date) AS week,
    COUNT(*) AS commit_count
FROM commits
WHERE dt BETWEEN '2026-08-01' AND '2026-09-30'   -- partition filter, see pruning note below
GROUP BY repo, date_trunc('week', commit_author_date)
ORDER BY week DESC;


-- ------------------------------------------------------------
-- Query 2: Issue resolution trend by state
-- FR-18 / US-DA-3
-- ------------------------------------------------------------
SELECT
    repo,
    state,
    date_trunc('week', created_at) AS week_opened,
    COUNT(*) AS issue_count,
    AVG(date_diff('hour', created_at, closed_at)) AS avg_hours_to_close  -- NULL rows (still open) excluded automatically
FROM issues
WHERE dt BETWEEN '2026-08-01' AND '2026-09-30'
GROUP BY repo, state, date_trunc('week', created_at)
ORDER BY week_opened DESC;


-- ------------------------------------------------------------
-- Query 3: PR count by repository
-- FR-18 / US-DA-1
-- ------------------------------------------------------------
SELECT
    repo,
    COUNT(*) AS pull_request_count,
    SUM(CASE WHEN merged_at IS NOT NULL THEN 1 ELSE 0 END) AS merged_count
FROM pull_requests
WHERE dt BETWEEN '2026-08-01' AND '2026-09-30'
GROUP BY repo
ORDER BY pull_request_count DESC;


-- ------------------------------------------------------------
-- Query 4 (DORA): Deployment Frequency, weekly trend
-- FR-29/FR-31, PRD §11.4
-- ------------------------------------------------------------
SELECT
    repo,
    environment,
    date_trunc('week', date(dt)) AS week,
    SUM(deploy_count) AS total_deployments
FROM dora_metrics
WHERE dt BETWEEN '2026-08-01' AND '2026-09-30'
GROUP BY repo, environment, date_trunc('week', date(dt))
ORDER BY week DESC;


-- ------------------------------------------------------------
-- Query 5 (DORA): Lead Time for Changes (p50/p95 trend)
-- FR-29/FR-31, PRD §11.4
-- ------------------------------------------------------------
SELECT
    repo,
    environment,
    dt,
    lead_time_p50,
    lead_time_p95
FROM dora_metrics
WHERE dt BETWEEN '2026-08-01' AND '2026-09-30'
    AND lead_time_p50 IS NOT NULL
ORDER BY dt DESC;


-- ------------------------------------------------------------
-- Query 6 (DORA, bonus — all 4 metrics in one place)
-- Satisfies "ideally all four: DF, LT p50/p95, CFR, MTTR"
-- ------------------------------------------------------------
SELECT
    repo,
    environment,
    dt,
    deploy_count,
    lead_time_p50,
    lead_time_p95,
    change_failure_rate,
    mttr_minutes
FROM dora_metrics
WHERE dt BETWEEN '2026-08-01' AND '2026-09-30'
ORDER BY dt DESC;


-- ============================================================
-- PARTITION PRUNING VERIFICATION (AC 3)
-- Run each pair back-to-back and compare "Data scanned" in the
-- Athena console/query results panel (also visible via
-- aws athena get-query-execution --query-execution-id <id>
-- --query "QueryExecution.Statistics.DataScannedInBytes").
-- ============================================================

-- 6a. WITHOUT partition filter — scans ALL dt= partitions
SELECT COUNT(*) FROM commits;

-- 6b. WITH partition filter — should scan a small fraction of the data
SELECT COUNT(*) FROM commits WHERE dt = '2026-09-16';
