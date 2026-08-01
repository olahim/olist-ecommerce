#!/bin/bash
# Optimize Iceberg tables

echo "Optimizing Iceberg tables..."

# Compact all tables
spark-submit --master yarn --deploy-mode cluster \
    /home/hadoop-user/olist-ecommerce/spark/jobs/iceberg_optimization.py \
    --operation all

echo "Iceberg optimization completed"
