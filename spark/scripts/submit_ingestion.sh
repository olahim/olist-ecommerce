#!/bin/bash
# Submit all ingestion jobs

DATE=${1:-$(date +%Y-%m-%d)}
YEAR=${DATE:0:4}
MONTH=${DATE:5:2}
DAY=${DATE:8:2}

echo "Submitting ingestion jobs for date: $DATE"

# Submit customers ingestion
spark-submit --master yarn --deploy-mode cluster \
    /home/hadoop-user/olist-ecommerce/spark/jobs/customers_ingestion.py \
    --year $YEAR --month $MONTH --day $DAY

# Submit orders ingestion
spark-submit --master yarn --deploy-mode cluster \
    /home/hadoop-user/olist-ecommerce/spark/jobs/orders_ingestion.py \
    --year $YEAR --month $MONTH --day $DAY

# Submit products ingestion
spark-submit --master yarn --deploy-mode cluster \
    /home/hadoop-user/olist-ecommerce/spark/jobs/products_ingestion.py \
    --year $YEAR --month $MONTH --day $DAY

# Submit sellers ingestion
spark-submit --master yarn --deploy-mode cluster \
    /home/hadoop-user/olist-ecommerce/spark/jobs/sellers_ingestion.py \
    --year $YEAR --month $MONTH --day $DAY

# Submit geolocation ingestion
spark-submit --master yarn --deploy-mode cluster \
    /home/hadoop-user/olist-ecommerce/spark/jobs/geolocation_ingestion.py \
    --year $YEAR --month $MONTH --day $DAY

# Submit reviews ingestion
spark-submit --master yarn --deploy-mode cluster \
    /home/hadoop-user/olist-ecommerce/spark/jobs/reviews_ingestion.py \
    --year $YEAR --month $MONTH --day $DAY

# Submit payments ingestion
spark-submit --master yarn --deploy-mode cluster \
    /home/hadoop-user/olist-ecommerce/spark/jobs/payments_ingestion.py \
    --year $YEAR --month $MONTH --day $DAY

# Submit order items ingestion
spark-submit --master yarn --deploy-mode cluster \
    /home/hadoop-user/olist-ecommerce/spark/jobs/order_items_ingestion.py \
    --year $YEAR --month $MONTH --day $DAY

# Submit category translation ingestion
spark-submit --master yarn --deploy-mode cluster \
    /home/hadoop-user/olist-ecommerce/spark/jobs/category_translation_ingestion.py \
    --year $YEAR --month $MONTH --day $DAY

echo "All ingestion jobs submitted"