#!/bin/bash
set -euo pipefail

# Configuration
KAFKA_HOME=${KAFKA_HOME:-"/opt/kafka"}
BOOTSTRAP_SERVER="localhost:9092"
PARTITIONS=${PARTITIONS:-4}
REPLICATION_FACTOR=${REPLICATION_FACTOR:-1}

echo "🔧 Setting up Kafka topics..."

# Function to create/recreate topic
recreate_topic() {
  local topic=$1
  echo "📝 Processing topic: $topic"

  # Delete if exists (ignore errors)
  "$KAFKA_HOME/bin/kafka-topics.sh" \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --delete --topic "$topic" 2>/dev/null || true

  sleep 1

  # Create topic
  "$KAFKA_HOME/bin/kafka-topics.sh" \
    --create \
    --topic "$topic" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION_FACTOR" \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --config retention.ms=604800000 \
    --config cleanup.policy=delete

  echo "✅ Topic $topic created"
}

# Create all required topics
recreate_topic "tasks"
recreate_topic "fragments"
recreate_topic "topology_updates"
recreate_topic "simulation_results"

# Verify topics
echo "🔍 Verifying topics..."
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVER" --list

echo "🎉 Kafka setup complete!"
