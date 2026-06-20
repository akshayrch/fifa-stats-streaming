#!/usr/bin/env bash
# Runs a single-node Kafka broker (KRaft mode, no Zookeeper) directly via the
# Apache Kafka binary distribution -- no Docker required.
#
# Useful as a fallback when Docker Hub's anonymous pull rate limit blocks
# `docker compose up` (confluentinc/cp-kafka, etc.) -- this script pulls
# straight from archive.apache.org instead.
#
# Requires: Java 17+ (`java -version`)

set -euo pipefail

KAFKA_VERSION="3.8.1"
SCALA_VERSION="2.13"
INSTALL_DIR="${KAFKA_INSTALL_DIR:-/opt/kafka}"
ARCHIVE_URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"

if [ ! -d "$INSTALL_DIR" ]; then
  echo "Downloading Kafka ${KAFKA_VERSION}..."
  tmp_tgz=$(mktemp)
  curl -sL -o "$tmp_tgz" "$ARCHIVE_URL"
  mkdir -p "$INSTALL_DIR"
  tar -xzf "$tmp_tgz" -C "$(dirname "$INSTALL_DIR")"
  mv "$(dirname "$INSTALL_DIR")/kafka_${SCALA_VERSION}-${KAFKA_VERSION}"/* "$INSTALL_DIR"/
  rm "$tmp_tgz"
fi

cd "$INSTALL_DIR"

if [ ! -d /tmp/kraft-combined-logs ]; then
  echo "Formatting KRaft storage..."
  CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)
  bin/kafka-storage.sh format -t "$CLUSTER_ID" -c config/kraft/server.properties
fi

echo "Starting Kafka broker on localhost:9092..."
exec bin/kafka-server-start.sh config/kraft/server.properties
