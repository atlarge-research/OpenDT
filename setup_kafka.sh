bin/kafka-topics.sh \
  --create \
  --topic events \
  --partitions 6 \
  --replication-factor 1 \
  --bootstrap-server broker1:9092,broker2:9092 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2