#!/bin/bash
# Trino query runner script

QUERY_FILE=$1
OUTPUT_FORMAT=${2:-csv}

if [ -z "$QUERY_FILE" ]; then
    echo "Usage: $0 <query_file> [output_format]"
    echo "Output formats: csv, json, tsv"
    exit 1
fi

QUERY_PATH="/home/hadoop-user/olist-ecommerce/trino/queries/$QUERY_FILE"

if [ ! -f "$QUERY_PATH" ]; then
    echo "Query file not found: $QUERY_PATH"
    exit 1
fi

QUERY=$(cat "$QUERY_PATH")

echo "Executing query from: $QUERY_FILE"
echo "Output format: $OUTPUT_FORMAT"

trino --server localhost:8080 \
      --catalog iceberg \
      --schema olist_warehouse \
      --user admin \
      --output-format "$OUTPUT_FORMAT" \
      --execute "$QUERY"