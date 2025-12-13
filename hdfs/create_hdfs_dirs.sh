#!/bin/bash

echo "Creando estructura HDFS para e-commerce..."

hdfs dfs -mkdir -p /ecommerce/orders
hdfs dfs -mkdir -p /ecommerce/views
hdfs dfs -mkdir -p /ecommerce/cart

hdfs dfs -mkdir -p /ecommerce/checkpoints

echo "Estructura HDFS creada:"
hdfs dfs -ls /ecommerce
