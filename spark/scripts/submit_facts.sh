#!/bin/bash
# Submit fact table building jobs

DATE=${1:-$(date +%Y-%m-%d)}

echo "Building fact tables for date: $DATE"

# Build orders fact
spark-submit --master yarn --deploy-mode cluster \
    /home/hadoop-user/olist-ecommerce/spark/jobs/fact_orders.py \
    --date $DATE

# Build order items fact
spark-submit --master yarn --deploy-mode cluster \
    /home/hadoop-user/olist-ecommerce/spark/jobs/fact_order_items.py \
    --date $DATE

# Build payments fact
spark-submit --master yarn --deploy-mode cluster \
    /home/hadoop-user/olist-ecommerce/spark/jobs/fact_order_payments.py \
    --date $DATE

echo "All fact table jobs completed"
