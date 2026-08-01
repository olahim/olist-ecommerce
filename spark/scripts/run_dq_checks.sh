#!/bin/bash
# Run data quality checks on all datasets

DATE=${1:-$(date +%Y-%m-%d)}
CONFIG_PATH="/home/hadoop-user/olist-ecommerce/airflow/dags/config/data_quality_rules.yaml"

echo "Running data quality checks for date: $DATE"

datasets=("customers" "orders" "products" "sellers" "geolocation" "reviews" "payments" "order_items")

for dataset in "${datasets[@]}"; do
    echo "Checking $dataset..."
    
    spark-submit --master yarn --deploy-mode cluster \
        /home/hadoop-user/olist-ecommerce/spark/jobs/data_quality_checks.py \
        --dataset "$dataset" \
        --config "$CONFIG_PATH" \
        --date "$DATE"
    
    if [ $? -eq 0 ]; then
        echo "✓ $dataset DQ checks passed"
    else
        echo "✗ $dataset DQ checks failed"
        exit 1
    fi
done

# Send alerts
spark-submit --master yarn --deploy-mode cluster \
    /home/hadoop-user/olist-ecommerce/spark/jobs/dq_alerting.py \
    --date "$DATE" --action alert --threshold 90

echo "All data quality checks completed"