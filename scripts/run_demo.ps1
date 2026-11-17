# GitPulse: Apache Iceberg & PySpark Automated Demo (PowerShell)
$ErrorActionPreference = "Stop"

$env:PYTHONPATH = "src;$env:PYTHONPATH"
$WarehouseDir = "$PWD\data\warehouse"

Write-Host "`n========================================================================" -ForegroundColor Cyan
Write-Host "  GitPulse: PySpark + Apache Iceberg Engineering Verification Demo" -ForegroundColor Cyan
Write-Host "========================================================================" -ForegroundColor Cyan
Write-Host "Runtime: Spark 3.5.4 | Apache Iceberg 1.7.1 | Catalog: Local Hadoop"
Write-Host "Warehouse: $WarehouseDir`n"

if (Test-Path $WarehouseDir) {
    Remove-Item -Recurse -Force $WarehouseDir
}

Write-Host ">>> [1/7] Generating synthetic fixtures..." -ForegroundColor Yellow
python src/gitpulse/generate_fixtures.py

Write-Host "`n>>> [2/7] Ingesting Batch 1: Baseline..." -ForegroundColor Yellow
python -m gitpulse.cli --manifest data/fixtures/batch_1/batch_manifest.json --action ingest --warehouse $WarehouseDir

Write-Host "`n>>> [3/7] Ingesting Batch 2: Updates & Late-Arriving data..." -ForegroundColor Yellow
python -m gitpulse.cli --manifest data/fixtures/batch_2/batch_manifest.json --action ingest --warehouse $WarehouseDir

Write-Host "`n>>> [4/7] Performing Iceberg Time-Travel Snapshot Comparison..." -ForegroundColor Yellow
python -m gitpulse.cli --action time-travel --warehouse $WarehouseDir

Write-Host "`n>>> [5/7] Replaying Batch 3 (Idempotency verification)..." -ForegroundColor Yellow
python -m gitpulse.cli --manifest data/fixtures/batch_3_replay/batch_manifest.json --action ingest --warehouse $WarehouseDir

Write-Host "`n>>> [6/7] Running Analytical Queries..." -ForegroundColor Yellow
python -m gitpulse.cli --action queries --warehouse $WarehouseDir

Write-Host "`n>>> [7/7] Testing Negative Contracts (Rejection of corrupt data)..." -ForegroundColor Yellow
python -m gitpulse.cli --manifest data/fixtures/corruptions/missing_page/manifest.json --action ingest --warehouse $WarehouseDir
Write-Host "`nAll verification steps completed!" -ForegroundColor Green
