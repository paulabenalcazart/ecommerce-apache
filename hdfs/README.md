# HDFS Storage – E-commerce Analytics

This folder contains scripts related to HDFS storage configuration.

## HDFS Structure Created

The Spark Streaming job writes processed data to the following HDFS paths:

/ecommerce/
├── orders/fecha=YYYY-MM-DD/
├── views/fecha=YYYY-MM-DD/
├── cart/fecha=YYYY-MM-DD/
├── sales_per_minute/
├── orders_avg_ticket/
├── views_by_category/
└── checkpoints/

## How to create the structure

```bash
./create_hdfs_dirs.sh
