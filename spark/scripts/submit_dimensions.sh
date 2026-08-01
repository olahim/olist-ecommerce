#!/bin/bash
# Submit dimension building jobs

DATE=${1:-$(date +%Y-%m-%d)}

echo "Building dimensions for date: $DATE"

# Build customer dimension
spark-submit --master yarn --deploy-mode cluster \
    /home/hadoop-user/olist-ecommerce/spark/jobs/dim_customers.py \
    --date $DATE

# Build product dimension
spark-submit --master yarn --deploy-mode cluster \
    /home/hadoop-user/olist-ecommerce/spark/jobs/dim_products.py \
    --date $DATE

# Build seller dimension
spark-submit --master yarn --deploy-mode cluster \
    /home/hadoop-user/olist-ecommerce/spark/jobs/dim_sellers.py \
    --date $DATE

echo "All dimension jobs completed"