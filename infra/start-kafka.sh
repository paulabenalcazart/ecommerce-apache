#!/bin/bash

KAFKA_HOME=/opt/kafka

echo "Iniciando Kafka usando server.properties..."

cd $KAFKA_HOME || exit 1

bin/kafka-server-start.sh config/server.properties

