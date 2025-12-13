#!/bin/bash

SPARK_HOME=/opt/spark

echo "Iniciando Spark Standalone..."

$SPARK_HOME/sbin/start-master.sh
sleep 2
$SPARK_HOME/sbin/start-worker.sh spark://localhost:7077
