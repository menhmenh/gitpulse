"""
SparkSession Factory for Apache Iceberg Lakehouse
=================================================
Configures SparkSession with Iceberg extensions, catalog integrations
(local Hadoop catalog on disk or AWS Glue Data Catalog), and timezone UTC.
"""

import os
from pathlib import Path
from typing import Optional
from pyspark.sql import SparkSession


def get_spark_session(
    catalog_name: str = "local",
    warehouse_path: Optional[str] = None,
    catalog_type: str = "hadoop",
    app_name: str = "GitPulse-Iceberg-Pipeline",
) -> SparkSession:
    """
    Initializes and returns a configured SparkSession with Apache Iceberg extensions.
    
    Parameters:
        catalog_name: Name of the Spark catalog identifier (e.g. 'local' or 'glue').
        warehouse_path: Path to the warehouse directory (local directory or s3:// path).
        catalog_type: Catalog backend type ('hadoop' for disk/volume, 'glue' for AWS Glue).
        app_name: Name of the Spark application.
    """
    if warehouse_path is None:
        default_dir = Path(__file__).resolve().parent.parent.parent / "data" / "warehouse"
        warehouse_path = str(default_dir).replace("\\", "/")

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
    )

    if catalog_type == "hadoop":
        builder = (
            builder.config(f"spark.sql.catalog.{catalog_name}.type", "hadoop")
            .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path)
            .config("spark.sql.defaultCatalog", catalog_name)
        )
    elif catalog_type == "glue":
        builder = (
            builder.config(
                f"spark.sql.catalog.{catalog_name}.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog",
            )
            .config(
                f"spark.sql.catalog.{catalog_name}.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO",
            )
            .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path)
            .config("spark.sql.defaultCatalog", catalog_name)
        )

    # In local test/development mode, optimize shuffle partitions for single-machine performance
    builder = (
        builder.config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.ui.enabled", "false")
    )

    return builder.getOrCreate()
