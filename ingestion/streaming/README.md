# Streaming (Spark Structured Streaming)

Three jobs, one per medallion hop. Each is a standalone PySpark application
that can run locally (`spark-submit` or `python job.py`) against Kafka and a
Delta lakehouse (local filesystem for dev, or MinIO/S3A via
`infra/docker-compose.yml`).

| Job | Reads | Writes | Mode |
|---|---|---|---|
| [`jobs/bronze_ingest.py`](jobs/bronze_ingest.py) | Kafka topics (`football.*`) | `bronze.*` Delta tables | Streaming (`--continuous`) or one-shot (`trigger(availableNow=True)`, default) |
| [`jobs/silver_transform.py`](jobs/silver_transform.py) | `bronze.*` | `silver.*` dim/fact Delta tables | Streaming or micro-batch (`trigger(availableNow=True)`) |
| [`jobs/gold_aggregate.py`](jobs/gold_aggregate.py) | `silver.*` | `gold.*` feature tables | Batch (scheduled, e.g. via Airflow later) |

## Common config

All jobs read connection settings from environment variables (so the same
code runs locally and, later, in a cloud environment):

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Local dev (default, no Docker/MinIO required):
export LAKEHOUSE_BASE_PATH=file:///tmp/fifa-lakehouse

# Cloud/MinIO instead:
# export LAKEHOUSE_BASE_PATH=s3a://fifa-lakehouse
# export MINIO_ENDPOINT=http://localhost:9000
# export MINIO_ACCESS_KEY=minioadmin
# export MINIO_SECRET_KEY=minioadmin
```

`spark_session.py` only configures the S3A connector when
`LAKEHOUSE_BASE_PATH` starts with `s3a://`; otherwise Delta tables are written
straight to the local filesystem — the simplest way to run this locally
without Docker.

## Running locally

```bash
pip install -r streaming/requirements.txt

# one-shot: process everything currently on the topics, then exit
python streaming/jobs/bronze_ingest.py

# or run forever, picking up new messages as they arrive
python streaming/jobs/bronze_ingest.py --continuous

# in another terminal, once Bronze has data:
python streaming/jobs/silver_transform.py
python streaming/jobs/gold_aggregate.py
```

Read back a Bronze table from `pyspark`:

```python
from streaming.jobs.spark_session import get_spark, lakehouse_path

spark = get_spark("check")
spark.read.format("delta").load(lakehouse_path("bronze", "fixtures_raw")).show()
```

## Status

✅ Phase 2 — `bronze_ingest.py` implemented and verified end-to-end: all 5
`football.*` Kafka topics stream into their `bronze.*` Delta tables (raw
`value` + Kafka metadata), on the local filesystem lakehouse.

📋 `silver_transform.py` and `gold_aggregate.py` remain skeletons — schema
definitions and transformation logic are TODOs for Phase 3.
