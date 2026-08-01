#!/usr/bin/env python3
"""
Data Quality Checks Job
Performs comprehensive data quality validation on datasets
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, sum as spark_sum
import yaml
import argparse
import json
from datetime import datetime

def create_spark_session():
    return SparkSession.builder \
        .appName("Olist Data Quality Checks") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()

def load_dq_rules(config_path):
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def check_null_conditions(df, column, severity, dataset_name):
    null_count = df.filter(col(column).isNull()).count()
    total_count = df.count()
    null_percentage = (null_count / total_count) * 100 if total_count > 0 else 0
    
    return {
        "check_type": "null_check",
        "dataset": dataset_name,
        "column": column,
        "severity": severity,
        "failed_count": null_count,
        "failed_percentage": null_percentage,
        "passed": null_count == 0
    }

def check_duplicates(df, columns, severity, dataset_name):
    duplicate_count = df.groupBy(columns).count().filter(col("count") > 1).count()
    
    return {
        "check_type": "duplicate_check",
        "dataset": dataset_name,
        "columns": columns,
        "severity": severity,
        "failed_count": duplicate_count,
        "passed": duplicate_count == 0
    }

def check_referential_integrity(df, fk_column, reference_df, reference_column, severity, dataset_name):
    invalid_fks = df.join(reference_df, df[fk_column] == reference_df[reference_column], "left_anti") \
                    .select(fk_column).distinct().count()
    
    return {
        "check_type": "referential_integrity",
        "dataset": dataset_name,
        "foreign_key": fk_column,
        "references": f"{reference_column}",
        "severity": severity,
        "failed_count": invalid_fks,
        "passed": invalid_fks == 0
    }

def check_range(df, column, min_val, max_val, severity, dataset_name):
    out_of_range = df.filter((col(column) < min_val) | (col(column) > max_val)).count()
    
    return {
        "check_type": "range_check",
        "dataset": dataset_name,
        "column": column,
        "min": min_val,
        "max": max_val,
        "severity": severity,
        "failed_count": out_of_range,
        "passed": out_of_range == 0
    }

def main(dataset, config_path, date_str):
    spark = create_spark_session()
    config = load_dq_rules(config_path)
    
    dataset_config = config.get(dataset, {})
    results = []
    
    try:
        # Read staging data
        df = spark.read.parquet(f"/opt/hadoop/data/staging/{dataset}/")
        
        # Run null checks
        for rule in dataset_config.get("null_checks", []):
            result = check_null_conditions(df, rule["column"], rule["severity"], dataset)
            results.append(result)
        
        # Run duplicate checks
        for rule in dataset_config.get("duplicate_checks", []):
            result = check_duplicates(df, rule["columns"], rule["severity"], dataset)
            results.append(result)
        
        # Run referential integrity checks
        for rule in dataset_config.get("referential_integrity", []):
            ref_parts = rule["references"].split('.')
            ref_df = spark.read.parquet(f"/opt/hadoop/data/staging/{ref_parts[0]}/")
            result = check_referential_integrity(
                df, rule["foreign_key"], ref_df, ref_parts[1], rule["severity"], dataset
            )
            results.append(result)
        
        # Run range checks
        for rule in dataset_config.get("range_checks", []):
            result = check_range(df, rule["column"], rule["min"], rule["max"], rule["severity"], dataset)
            results.append(result)
        
        # Calculate DQ score
        total_checks = len(results)
        failed_checks = len([r for r in results if not r["passed"]])
        dq_score = ((total_checks - failed_checks) / total_checks) * 100 if total_checks > 0 else 100
        
        # Save report
        report = {
            "dataset": dataset,
            "date": date_str,
            "dq_score": dq_score,
            "total_checks": total_checks,
            "failed_checks": failed_checks,
            "results": results,
            "timestamp": datetime.now().isoformat()
        }
        
        output_path = f"/opt/hadoop/data/quality/dq_reports/{dataset}_report_{date_str}.json"
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"DQ Score for {dataset}: {dq_score:.2f}%")
        
    except Exception as e:
        print(f"Error in DQ checks for {dataset}: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    main(args.dataset, args.config, args.date)