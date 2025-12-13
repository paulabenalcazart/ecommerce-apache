#!/bin/bash

KAFKA_HOME=/opt/kafka
BOOTSTRAP=localhost:9092

echo "Creando topicos de e-commerce..."

$KAFKA_HOME/bin/kafka-topics.sh --create \
  --bootstrap-server $BOOTSTRAP \
  --topic orders \
  --partitions 3 \
  --replication-factor 1 || true

$KAFKA_HOME/bin/kafka-topics.sh --create \
  --bootstrap-server $BOOTSTRAP \
  --topic cart_events \
  --partitions 3 \
  --replication-factor 1 || true

$KAFKA_HOME/bin/kafka-topics.sh --create \
  --bootstrap-server $BOOTSTRAP \
  --topic page_views \
  --partitions 3 \
  --replication-factor 1 || true

echo "Topicos listos."
