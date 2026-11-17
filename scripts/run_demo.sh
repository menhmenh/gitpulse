#!/usr/bin/env bash
# ==============================================================================
# GitPulse: Apache Iceberg & PySpark Lakehouse Automated Demo
# ==============================================================================
# Demonstrates:
#   1. Contract validation & SHA-256 manifest integrity
#   2. Idempotent MERGE INTO with Apache Iceberg
#   3. Correct resolution of late-arriving records into historical partitions
#   4. Multi-repo primary key isolation (same ID across different repos preserved)
#   5. Time-Travel snapshot comparison query
#   6. Replay idempotency & rejection of stale / out-of-order state
#   7. Automated failure injection & strict rejection of corrupt raw data
# ==============================================================================

set -e

export PYTHONPATH="src:${PYTHONPATH}"
WAREHOUSE_DIR="${PWD}/data/warehouse"

echo ""
echo "========================================================================"
echo "  GitPulse: PySpark + Apache Iceberg Engineering Verification Demo"
echo "========================================================================"
echo "Runtime: Spark 3.5.4 | Apache Iceberg 1.7.1 | Catalog: Local Hadoop"
echo "Warehouse: ${WAREHOUSE_DIR}"
echo ""

# Clean previous demo warehouse if exists
rm -rf "${WAREHOUSE_DIR}"

# ------------------------------------------------------------------------------
# STEP 1: Generate Synthetic Fixtures
# ------------------------------------------------------------------------------
echo ">>> [1/7] Generating synthetic fixtures (2 repos, 3 batches + corruptions)..."
python src/gitpulse/generate_fixtures.py

# ------------------------------------------------------------------------------
# STEP 2: Ingest Batch 1 (Initial Baseline)
# ------------------------------------------------------------------------------
echo ""
echo ">>> [2/7] Ingesting Batch 1: Baseline for apache/spark and apache/iceberg..."
python -m gitpulse.cli --manifest data/fixtures/batch_1/batch_manifest.json --action ingest --warehouse "${WAREHOUSE_DIR}"

# ------------------------------------------------------------------------------
# STEP 3: Ingest Batch 2 (Updates & Late-Arriving records)
# ------------------------------------------------------------------------------
echo ""
echo ">>> [3/7] Ingesting Batch 2: Status update (in_progress -> completed) and late-arriving run 99..."
python -m gitpulse.cli --manifest data/fixtures/batch_2/batch_manifest.json --action ingest --warehouse "${WAREHOUSE_DIR}"

# ------------------------------------------------------------------------------
# STEP 4: Time-Travel Snapshot Comparison
# ------------------------------------------------------------------------------
echo ""
echo ">>> [4/7] Performing Iceberg Time-Travel Snapshot Comparison..."
python -m gitpulse.cli --action time-travel --warehouse "${WAREHOUSE_DIR}"

# ------------------------------------------------------------------------------
# STEP 5: Replay Batch 3 (Idempotency & Stale State Guard)
# ------------------------------------------------------------------------------
echo ""
echo ">>> [5/7] Replaying Batch 3: Testing Idempotency & Stale Version Protection..."
python -m gitpulse.cli --manifest data/fixtures/batch_3_replay/batch_manifest.json --action ingest --warehouse "${WAREHOUSE_DIR}"

# ------------------------------------------------------------------------------
# STEP 6: Execute Analytical SQL Queries
# ------------------------------------------------------------------------------
echo ""
echo ">>> [6/7] Querying Analytical Views: 7-Day Trend & Flakiest Workflows..."
python -m gitpulse.cli --action queries --warehouse "${WAREHOUSE_DIR}"

# ------------------------------------------------------------------------------
# STEP 7: Verify Negative Contract Rejection (Corrupt Fixtures)
# ------------------------------------------------------------------------------
echo ""
echo ">>> [7/7] Testing Negative Data Contracts (Ensuring Corrupt Data Is Blocked)..."

set +e
echo "  [Test A] Attempting to ingest missing page..."
python -m gitpulse.cli --manifest data/fixtures/corruptions/missing_page/manifest.json --action ingest --warehouse "${WAREHOUSE_DIR}" 2>/dev/null
STATUS_A=$?
if [ $STATUS_A -eq 1 ]; then
    echo "  --> PASSED: Missing page correctly rejected with exit code 1."
else
    echo "  --> FAILED: Expected exit code 1, got $STATUS_A"
    exit 1
fi

echo "  [Test B] Attempting to ingest checksum mismatch..."
python -m gitpulse.cli --manifest data/fixtures/corruptions/checksum_mismatch/manifest.json --action ingest --warehouse "${WAREHOUSE_DIR}" 2>/dev/null
STATUS_B=$?
if [ $STATUS_B -eq 1 ]; then
    echo "  --> PASSED: Checksum mismatch correctly rejected with exit code 1."
else
    echo "  --> FAILED: Expected exit code 1, got $STATUS_B"
    exit 1
fi
set -e

echo ""
echo "========================================================================"
echo "  ALL VERIFICATION CHECKS PASSED SUCCESSFULLY!"
echo "========================================================================"
