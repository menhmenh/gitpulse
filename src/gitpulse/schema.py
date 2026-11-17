"""
Explicit PySpark Schemas and Contract Definitions for GitPulse
=============================================================
Defines explicit, strongly-typed schemas for workflow_runs.
Eliminates untyped JSON, enforces data contracts, and protects privacy
by discarding actors, personal emails, and freeform commit messages.
"""

from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Explicit schema for extracting workflow runs from raw GitHub JSON pages
RAW_WORKFLOW_RUNS_SCHEMA = StructType([
    StructField("id", LongType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("workflow_id", LongType(), nullable=True),
    StructField("status", StringType(), nullable=False),
    StructField("conclusion", StringType(), nullable=True),
    StructField("run_attempt", IntegerType(), nullable=True),
    StructField("event", StringType(), nullable=True),
    StructField("head_branch", StringType(), nullable=True),
    StructField("created_at", StringType(), nullable=False),
    StructField("updated_at", StringType(), nullable=False),
    StructField("run_started_at", StringType(), nullable=True),
    # Privacy protection: actor, triggering_actor, commit authors, emails, and
    # freeform commit messages are deliberately EXCLUDED from this schema.
])

# Canonical target table schema for Iceberg table `workflow_runs_current`
WORKFLOW_RUNS_ICEBERG_SCHEMA = StructType([
    StructField("repo", StringType(), nullable=False),
    StructField("id", LongType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("workflow_id", LongType(), nullable=True),
    StructField("status", StringType(), nullable=False),
    StructField("conclusion", StringType(), nullable=True),
    StructField("run_attempt", IntegerType(), nullable=True),
    StructField("event", StringType(), nullable=True),
    StructField("head_branch", StringType(), nullable=True),
    StructField("created_at", TimestampType(), nullable=False),
    StructField("updated_at", TimestampType(), nullable=False),
    StructField("run_started_at", TimestampType(), nullable=True),
    StructField("ingested_at", TimestampType(), nullable=False),
    StructField("run_ts", StringType(), nullable=False),
])
