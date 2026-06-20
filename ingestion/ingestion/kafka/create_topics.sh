#!/usr/bin/env bash
# Creates Kafka topics defined in topics.yaml against a local broker.
#
# Usage:
#   ./create_topics.sh              # via `docker exec` against the Compose Kafka container
#   ./create_topics.sh --no-docker  # via kafka-topics.sh in /opt/kafka/bin
#                                    # (use with infra/run_local_kafka.sh)
#
# Requires: yq (https://github.com/mikefarah/yq)

set -euo pipefail

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"
TOPICS_FILE="$(dirname "$0")/topics.yaml"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-fifa-kafka}"
KAFKA_BIN="${KAFKA_INSTALL_DIR:-/opt/kafka}/bin"

if [[ "${1:-}" == "--no-docker" ]]; then
  KAFKA_TOPICS=("$KAFKA_BIN/kafka-topics.sh")
else
  KAFKA_TOPICS=(docker exec "$KAFKA_CONTAINER" kafka-topics.sh)
fi

count=$(yq '.topics | length' "$TOPICS_FILE")

for i in $(seq 0 $((count - 1))); do
  name=$(yq -r ".topics[$i].name" "$TOPICS_FILE")
  partitions=$(yq -r ".topics[$i].partitions" "$TOPICS_FILE")
  rf=$(yq -r ".topics[$i].replication_factor" "$TOPICS_FILE")
  retention=$(yq -r ".topics[$i].config.\"retention.ms\"" "$TOPICS_FILE")

  echo "Creating topic: $name (partitions=$partitions, rf=$rf, retention.ms=$retention)"

  "${KAFKA_TOPICS[@]}" \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor "$rf" \
    --config "retention.ms=$retention"
done

echo "Done. Listing topics:"
"${KAFKA_TOPICS[@]}" --bootstrap-server "$BOOTSTRAP_SERVER" --list
