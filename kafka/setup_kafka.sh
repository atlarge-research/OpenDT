#!/bin/bash


parts=$(($(nproc) / 2))
bootstrap_server=localhost:9092

recreate_topic() {
  local topic=$1

  # Delete topic if it exists
  "$KAFKA_HOME"/bin/kafka-topics.sh --bootstrap-server $bootstrap_server --delete --topic "$topic"
  sleep 0.3
  # Recreate topic
  "$KAFKA_HOME"/bin/kafka-topics.sh \
    --create \
    --topic "$topic" \
    --partitions $parts \
    --replication-factor 1 \
    --bootstrap-server "$bootstrap_server" \
    --config retention.ms=604800000 \
    --config cleanup.policy=delete
}

recreate_topic tasks
recreate_topic fragments