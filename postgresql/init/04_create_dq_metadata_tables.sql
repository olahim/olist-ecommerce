-- =====================================================
-- PostgreSQL: Create Data Quality Metadata Tables
-- =====================================================

-- Create DQ metadata database
CREATE DATABASE olist_dq_metadata
    WITH 
    OWNER = postgres
    ENCODING = 'UTF8';

\c olist_dq_metadata;

-- Create DQ rules table
CREATE TABLE IF NOT EXISTS dq_rules (
    rule_id SERIAL PRIMARY KEY,
    rule_name VARCHAR(255) NOT NULL,
    dataset_name VARCHAR(100) NOT NULL,
    check_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    rule_definition JSONB NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_by VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(rule_name)
);

-- Create DQ thresholds table
CREATE TABLE IF NOT EXISTS dq_thresholds (
    threshold_id SERIAL PRIMARY KEY,
    dataset_name VARCHAR(100) NOT NULL,
    threshold_type VARCHAR(50) NOT NULL,
    warning_threshold DECIMAL(5,2),
    critical_threshold DECIMAL(5,2),
    sla_threshold DECIMAL(5,2),
    notification_channels JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(dataset_name, threshold_type)
);

-- Create DQ rule execution log
CREATE TABLE IF NOT EXISTS dq_rule_execution_log (
    execution_id SERIAL PRIMARY KEY,
    rule_id INTEGER REFERENCES dq_rules(rule_id),
    execution_date DATE NOT NULL,
    execution_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    passed BOOLEAN,
    failed_count BIGINT,
    total_count BIGINT,
    execution_time_ms INTEGER,
    error_message TEXT,
    executed_by VARCHAR(100)
);

-- Create DQ score tracking table
CREATE TABLE IF NOT EXISTS dq_score_tracking (
    score_id SERIAL PRIMARY KEY,
    dataset_name VARCHAR(100) NOT NULL,
    score_date DATE NOT NULL,
    dq_score DECIMAL(5,2),
    passed_sla BOOLEAN,
    details JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create alert configurations table
CREATE TABLE IF NOT EXISTS alert_configurations (
    config_id SERIAL PRIMARY KEY,
    alert_type VARCHAR(50) NOT NULL,
    destination VARCHAR(500) NOT NULL,
    is_enabled BOOLEAN DEFAULT TRUE,
    conditions JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert default DQ thresholds
INSERT INTO dq_thresholds (dataset_name, threshold_type, warning_threshold, critical_threshold, sla_threshold) VALUES
('customers', 'data_quality', 95.00, 80.00, 90.00),
('orders', 'data_quality', 95.00, 80.00, 90.00),
('products', 'data_quality', 95.00, 80.00, 90.00),
('sellers', 'data_quality', 95.00, 80.00, 90.00),
('order_items', 'data_quality', 95.00, 80.00, 90.00)
ON CONFLICT (dataset_name, threshold_type) DO NOTHING;

-- Insert default alert configurations
INSERT INTO alert_configurations (alert_type, destination, is_enabled, conditions) VALUES
('slack', 'https://hooks.slack.com/services/your-webhook-url', TRUE, '{"dq_score_below": 90.00}'),
('email', 'data-quality@olist.com', TRUE, '{"dq_score_below": 85.00}')
ON CONFLICT DO NOTHING;

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_dq_score_tracking_dataset_date ON dq_score_tracking(dataset_name, score_date);
CREATE INDEX IF NOT EXISTS idx_dq_rule_execution_log_date ON dq_rule_execution_log(execution_date);
CREATE INDEX IF NOT EXISTS idx_dq_rule_execution_log_rule_id ON dq_rule_execution_log(rule_id);

-- Create view for current DQ status
CREATE OR REPLACE VIEW vw_current_dq_status AS
SELECT 
    dst.dataset_name,
    dst.score_date,
    dst.dq_score,
    dst.passed_sla,
    dt.sla_threshold,
    CASE 
        WHEN dst.dq_score >= dt.sla_threshold THEN 'PASS'
        WHEN dst.dq_score >= dt.warning_threshold THEN 'WARNING'
        ELSE 'FAIL'
    END AS status,
    dst.details,
    dst.created_at
FROM dq_score_tracking dst
JOIN dq_thresholds dt ON dst.dataset_name = dt.dataset_name
WHERE dst.score_date = CURRENT_DATE
ORDER BY dst.dataset_name;

SELECT 'Data quality metadata tables created successfully' AS status;