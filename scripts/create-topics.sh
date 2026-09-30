#!/usr/bin/env bash
# Topics are created explicitly because KAFKA_AUTO_CREATE_TOPICS_ENABLE is false.
# Auto-create is convenient right up until a typo in a producer silently creates
# `settlement.transaction.v1` with one partition and nobody notices for a week.
set -euo pipefail
K=/opt/kafka/bin/kafka-topics.sh
B=localhost:19092

for spec in "settlement.transactions.v1:6" "settlement.settlements.v1:3"; do
  topic="${spec%%:*}"; parts="${spec##*:}"
  if docker exec np_kafka $K --bootstrap-server $B --list | grep -qx "$topic"; then
    echo "  exists  $topic"
  else
    docker exec np_kafka $K --bootstrap-server $B --create \
      --topic "$topic" --partitions "$parts" --replication-factor 1 \
      --config retention.ms=172800000 --config compression.type=lz4
    echo "  created $topic ($parts partitions)"
  fi
done
