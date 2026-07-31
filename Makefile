#!/usr/bin/make -f

# =====================================================
# Olist Ecommerce Pipeline Makefile
# Usage: make <command>
# =====================================================

.PHONY: help deploy init clean \
        airflow-start airflow-stop airflow-status airflow-restart \
        dag-trigger dag-unpause dag-pause \
        dq-check run-dq \
        run-query \
        optimize backup restore monitor \
        test test-unit test-integration \
        metrics dashboards \
        seed shell sql logs

# =====================================================
# Configuration
# =====================================================

PROJECT_DIR := /home/hadoop-user/olist-ecommerce
HADOOP_DATA_DIR := /opt/hadoop/data
AIRFLOW_HOME ?= /opt/airflow

DAG_ID := olist_etl_pipeline

# =====================================================
# Colors
# =====================================================

RED := \033[0;31m
GREEN := \033[0;32m
YELLOW := \033[0;33m
BLUE := \033[0;34m
NC := \033[0m

# =====================================================
# Default Target
# =====================================================

help:
	@echo "$(BLUE)╔══════════════════════════════════════════════════════════════╗$(NC)"
	@echo "$(BLUE)║$(NC)      $(GREEN)Olist Ecommerce Pipeline - Available Commands$(NC)      $(BLUE)║$(NC)"
	@echo "$(BLUE)╚══════════════════════════════════════════════════════════════╝$(NC)"
	@echo ""
	@echo "$(YELLOW)Deployment & Setup:$(NC)"
	@echo "  $(GREEN)make deploy$(NC)              - Deploy entire pipeline"
	@echo "  $(GREEN)make init$(NC)                - Initialize environment"
	@echo "  $(GREEN)make clean$(NC)               - Clean temporary files"
	@echo ""
	@echo "$(YELLOW)Airflow Control:$(NC)"
	@echo "  $(GREEN)make airflow-start$(NC)       - Start Airflow services"
	@echo "  $(GREEN)make airflow-stop$(NC)        - Stop Airflow services"
	@echo "  $(GREEN)make airflow-status$(NC)      - Check Airflow service status"
	@echo "  $(GREEN)make airflow-restart$(NC)     - Restart Airflow services"
	@echo ""
	@echo "$(YELLOW)DAG Management:$(NC)"
	@echo "  $(GREEN)make dag-trigger$(NC)         - Trigger main ETL DAG"
	@echo "  $(GREEN)make dag-unpause$(NC)         - Unpause main ETL DAG"
	@echo "  $(GREEN)make dag-pause$(NC)           - Pause main ETL DAG"
	@echo ""
	@echo "$(YELLOW)Data Quality:$(NC)"
	@echo "  $(GREEN)make dq-check$(NC)            - Run data quality checks"
	@echo "  $(GREEN)make run-dq$(NC)              - Run daily DQ monitoring"
	@echo ""
	@echo "$(YELLOW)Analytics:$(NC)"
	@echo "  $(GREEN)make run-query$(NC)           - Run Trino query"
	@echo "    Usage: make run-query QUERY=customer_360/customer_summary.sql"
	@echo "  $(GREEN)make sql$(NC)                 - Open Trino SQL shell"
	@echo ""
	@echo "$(YELLOW)Maintenance:$(NC)"
	@echo "  $(GREEN)make optimize$(NC)            - Optimize Iceberg tables"
	@echo "  $(GREEN)make backup$(NC)              - Backup Hive metastore"
	@echo "  $(GREEN)make restore$(NC)             - Restore Hive metastore"
	@echo "  $(GREEN)make monitor$(NC)             - Show pipeline status"
	@echo "  $(GREEN)make logs$(NC)                - Show Airflow scheduler logs"
	@echo ""
	@echo "$(YELLOW)Testing:$(NC)"
	@echo "  $(GREEN)make test$(NC)                - Run all tests"
	@echo "  $(GREEN)make test-unit$(NC)           - Run unit tests only"
	@echo "  $(GREEN)make test-integration$(NC)    - Run integration tests only"
	@echo ""
	@echo "$(YELLOW)Monitoring:$(NC)"
	@echo "  $(GREEN)make metrics$(NC)             - Show Prometheus metrics"
	@echo "  $(GREEN)make dashboards$(NC)          - Show service URLs"
	@echo ""
	@echo "$(YELLOW)Development:$(NC)"
	@echo "  $(GREEN)make seed$(NC)                - Seed test data"
	@echo "  $(GREEN)make shell$(NC)               - Open Spark shell"

# =====================================================
# Deployment & Setup
# =====================================================

deploy:
	@echo "$(GREEN) Deploying Olist Ecommerce Pipeline...$(NC)"
	@./scripts/deploy.sh

init:
	@echo "$(GREEN) Initializing environment...$(NC)"

	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "$(YELLOW)Created .env file. Please edit with your configuration.$(NC)"; \
	fi

	@mkdir -p $(HADOOP_DATA_DIR)/raw
	@mkdir -p $(HADOOP_DATA_DIR)/staging
	@mkdir -p $(HADOOP_DATA_DIR)/quality/dq_reports
	@mkdir -p $(HADOOP_DATA_DIR)/quality/dq_metrics
	@mkdir -p $(HADOOP_DATA_DIR)/quality/failed_records
	@mkdir -p $(HADOOP_DATA_DIR)/warehouse/iceberg

	@echo "$(GREEN) Environment initialized$(NC)"

clean:
	@echo "$(YELLOW) Cleaning temporary files...$(NC)"
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@find . -type f -name ".DS_Store" -delete 2>/dev/null || true
	@echo "$(GREEN) Cleanup completed$(NC)"

# =====================================================
# Airflow Control
# =====================================================

airflow-start:
	@echo "$(GREEN) Starting Airflow services...$(NC)"
	@./scripts/airflow_ctl.sh start

airflow-stop:
	@echo "$(YELLOW) Stopping Airflow services...$(NC)"
	@./scripts/airflow_ctl.sh stop

airflow-status:
	@./scripts/airflow_ctl.sh status

airflow-restart: airflow-stop airflow-start
	@echo "$(GREEN) Airflow restarted$(NC)"

# =====================================================
# DAG Management
# =====================================================

dag-trigger:
	@echo "$(GREEN) Triggering $(DAG_ID) DAG...$(NC)"
	@./scripts/airflow_ctl.sh trigger $(DAG_ID)

dag-unpause:
	@echo "$(GREEN) Unpausing $(DAG_ID) DAG...$(NC)"
	@./scripts/airflow_ctl.sh unpause $(DAG_ID)

dag-pause:
	@echo "$(YELLOW) Pausing $(DAG_ID) DAG...$(NC)"
	@./scripts/airflow_ctl.sh pause $(DAG_ID)

# =====================================================
# Data Quality
# =====================================================

dq-check:
	@echo "$(GREEN) Running data quality checks...$(NC)"
	@./scripts/run_data_quality.sh

run-dq:
	@echo "$(GREEN) Running daily DQ monitoring...$(NC)"
	@spark-submit spark/jobs/dq_alerting.py \
		--date $$(date +%Y-%m-%d) \
		--action alert

# =====================================================
# Analytics
# =====================================================

run-query:
	@if [ -z "$(QUERY)" ]; then \
		echo "$(RED)Error: QUERY parameter is required$(NC)"; \
		echo "Usage: make run-query QUERY=customer_360/customer_summary.sql"; \
		exit 1; \
	fi
	@echo "$(GREEN) Running query: $(QUERY)$(NC)"
	@./scripts/trino_query_runner.sh "$(QUERY)" csv

sql:
	@echo "$(GREEN) Opening Trino SQL shell...$(NC)"
	@trino --server localhost:8081 \
		--catalog iceberg \
		--schema olist_warehouse

# =====================================================
# Maintenance
# =====================================================

optimize:
	@echo "$(GREEN) Optimizing Iceberg tables...$(NC)"
	@./spark/scripts/optimize_iceberg.sh

backup:
	@echo "$(GREEN) Backing up Hive metastore...$(NC)"
	@./scripts/backup_metastore.sh

restore:
	@echo "$(YELLOW) Restoring Hive metastore...$(NC)"
	@./scripts/restore_metastore.sh

monitor:
	@echo "$(GREEN) Pipeline Status$(NC)"
	@echo "==================="
	@echo ""
	@echo "$(YELLOW)Airflow:$(NC)"
	@./scripts/airflow_ctl.sh status

	@echo ""
	@echo "$(YELLOW)Latest DQ Reports:$(NC)"
	@ls -t $(HADOOP_DATA_DIR)/quality/dq_reports/*.json 2>/dev/null | \
		head -5 | \
		while read f; do \
			echo "  $$(basename "$$f")"; \
		done || echo "  No DQ reports found"

	@echo ""
	@echo "$(YELLOW)Data Freshness:$(NC)"
	@find $(HADOOP_DATA_DIR)/raw -name "_SUCCESS" -mtime -1 2>/dev/null | \
		wc -l | \
		xargs echo "  Files updated in last 24h:"

	@echo ""
	@echo "$(YELLOW)Failed Records:$(NC)"
	@find $(HADOOP_DATA_DIR)/quality/failed_records \
		-name "*.parquet" 2>/dev/null | \
		wc -l | \
		xargs echo "  Quarantined record files:"

# =====================================================
# Testing
# =====================================================

test:
	@echo "$(GREEN) Running all tests...$(NC)"
	@python -m pytest tests -v

test-unit:
	@echo "$(GREEN) Running unit tests...$(NC)"
	@python -m pytest tests/unit -v

test-integration:
	@echo "$(GREEN) Running integration tests...$(NC)"
	@python -m pytest tests/integration -v

# =====================================================
# Monitoring
# =====================================================

metrics:
	@echo "$(GREEN) Prometheus Metrics$(NC)"
	@curl -s \
		'http://localhost:9090/api/v1/query?query=dq_score_current' \
		2>/dev/null | \
		python -m json.tool || \
		echo "Prometheus not accessible"

dashboards:
	@echo "$(GREEN) Service URLs$(NC)"
	@echo "  Airflow UI:     http://localhost:8080"
	@echo "  Trino UI:       http://localhost:8081"
	@echo "  Superset UI:    http://localhost:8088"
	@echo "  Grafana UI:     http://localhost:3000"
	@echo "  Prometheus UI:  http://localhost:9090"

# =====================================================
# Development Utilities
# =====================================================

seed:
	@echo "$(GREEN) Seeding test data...$(NC)"
	@./scripts/seed_test_data.sh

shell:
	@echo "$(GREEN) Opening Spark shell...$(NC)"
	@spark-shell \
		--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

logs:
	@echo "$(GREEN) Showing Airflow scheduler logs...$(NC)"
	@tail -f $(AIRFLOW_HOME)/logs/scheduler/latest/*.log
