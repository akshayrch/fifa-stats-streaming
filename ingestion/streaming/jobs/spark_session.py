"""Shared SparkSession builder configured for Kafka + Delta Lake, driven by
environment variables (see streaming/README.md).

Storage backend is controlled by LAKEHOUSE_BASE_PATH:
  - "s3a://..." (default for cloud/MinIO)  -> configures the S3A connector
  - "file:///..." or a plain local path     -> local filesystem, no S3A/MinIO
    needed (handy for local dev without Docker)
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

DEFAULT_LAKEHOUSE_BASE_PATH = "file:///tmp/fifa-lakehouse"


def _lakehouse_base() -> str:
    return os.environ.get("LAKEHOUSE_BASE_PATH", DEFAULT_LAKEHOUSE_BASE_PATH)


def get_spark(app_name: str) -> SparkSession:
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
        "io.delta:delta-spark_2.12:3.2.1",
    ]

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    if _lakehouse_base().startswith("s3a://"):
        packages.append("org.apache.hadoop:hadoop-aws:3.3.4")
        minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
        minio_access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
        minio_secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )

    builder = builder.config("spark.jars.packages", ",".join(packages))
    return builder.getOrCreate()


def kafka_bootstrap_servers() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def lakehouse_path(layer: str, table: str) -> str:
    return f"{_lakehouse_base()}/{layer}/{table}"


def checkpoint_path(job_name: str) -> str:
    return f"{_lakehouse_base()}/_checkpoints/{job_name}"
