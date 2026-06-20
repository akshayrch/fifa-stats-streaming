# Local Dev Infrastructure

`docker-compose.yml` brings up:

| Service | Port(s) | Purpose |
|---|---|---|
| `kafka` | `9092` | Single-broker Kafka (KRaft mode, no Zookeeper) |
| `kafka-ui` | `8080` | Web UI for browsing topics/messages — http://localhost:8080 |
| `minio` | `9000` (API), `9001` (console) | S3-compatible object storage for the Delta lakehouse — console at http://localhost:9001 (`minioadmin` / `minioadmin`) |

## Usage

```bash
cd infra
docker compose up -d

# create the lakehouse bucket (one-time)
docker run --rm --network infra_default \
  -e MC_HOST_local=http://minioadmin:minioadmin@minio:9000 \
  minio/mc mb local/fifa-lakehouse

# create Kafka topics
cd ../ingestion/kafka && ./create_topics.sh

# tear down
docker compose down            # keep data
docker compose down -v         # wipe volumes too
```

Spark runs locally via `pyspark` (see `streaming/`) and connects to Kafka at
`localhost:9092` and MinIO at `http://localhost:9000` using the S3A
connector.

## Alternative: Kafka without Docker

Docker Hub's anonymous pull rate limit can block `docker compose up` (it did
during development of this repo). If that happens, run Kafka directly via the
Apache binary distribution instead:

```bash
./run_local_kafka.sh   # downloads Kafka 3.8.1 to /opt/kafka, runs in KRaft mode on :9092
```

Then create topics directly with `kafka-topics.sh` (see
[`ingestion/kafka/create_topics.sh`](../ingestion/kafka/create_topics.sh) for
the `--no-docker` mode, which calls the binaries in `/opt/kafka/bin`
directly instead of `docker exec`). MinIO/Delta (the lakehouse storage layer)
still requires Docker — only needed starting in Phase 2.
