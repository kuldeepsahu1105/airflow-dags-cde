{% set analytics_db = params.analytics_database %}

CREATE DATABASE IF NOT EXISTS {{ analytics_db }};

CREATE TABLE IF NOT EXISTS {{ analytics_db }}.vehicle_health_enriched (
    source_file STRING,
    vin STRING,
    event_timestamp TIMESTAMP,
    event_date DATE,
    odometer_km DOUBLE,
    battery_voltage DOUBLE,
    engine_temp_c DOUBLE,
    oil_pressure_kpa DOUBLE,
    tire_pressure_fl DOUBLE,
    tire_pressure_fr DOUBLE,
    tire_pressure_rl DOUBLE,
    tire_pressure_rr DOUBLE,
    dtc_code STRING,
    severity STRING,
    country STRING,
    business_date STRING,
    source_zip STRING,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP,
    model_year INT,
    powertrain STRING,
    production_plant STRING,
    warranty_start_date DATE,
    customer_region STRING,
    dealer_id STRING,
    service_type STRING,
    service_status STRING,
    warranty_flag BOOLEAN,
    service_duration_hours DOUBLE,
    dealer_name STRING,
    region STRING,
    dealer_tier STRING,
    is_critical_event BOOLEAN,
    is_warning_event BOOLEAN,
    model STRING
)
PARTITIONED BY SPEC (business_date)
STORED BY ICEBERG
TBLPROPERTIES ('format-version'='2');

CREATE TABLE IF NOT EXISTS {{ analytics_db }}.daily_vehicle_health_summary (
    business_date STRING,
    event_date DATE,
    country STRING,
    customer_region STRING,
    model_year INT,
    powertrain STRING,
    severity STRING,
    telemetry_event_count BIGINT,
    affected_vehicle_count BIGINT,
    critical_dtc_count BIGINT,
    avg_odometer_km DOUBLE,
    max_odometer_km DOUBLE,
    avg_battery_voltage DOUBLE,
    avg_engine_temp_c DOUBLE,
    repeated_warning_vehicle_count BIGINT,
    model STRING
)
PARTITIONED BY SPEC (business_date)
STORED BY ICEBERG
TBLPROPERTIES ('format-version'='2');

CREATE TABLE IF NOT EXISTS {{ analytics_db }}.service_kpi_summary (
    business_date STRING,
    dealer_id STRING,
    dealer_name STRING,
    country STRING,
    dealer_tier STRING,
    model STRING,
    model_year INT,
    powertrain STRING,
    service_type STRING,
    service_event_count BIGINT,
    avg_service_duration_hours DOUBLE,
    avg_labor_hours DOUBLE,
    total_parts_cost DECIMAL(38,2),
    warranty_service_rate DOUBLE,
    region STRING
)
PARTITIONED BY SPEC (business_date)
STORED BY ICEBERG
TBLPROPERTIES ('format-version'='2');

CREATE TABLE IF NOT EXISTS {{ analytics_db }}.data_quality_report (
    dataset_name STRING,
    metric_name STRING,
    metric_value BIGINT,
    status STRING,
    business_date STRING,
    pipeline_run_id STRING,
    reported_at TIMESTAMP
)
PARTITIONED BY SPEC (business_date)
STORED BY ICEBERG
TBLPROPERTIES ('format-version'='2');

CREATE VIEW IF NOT EXISTS {{ analytics_db }}.vehicle_health_risk_story AS
SELECT
    business_date,
    model,
    customer_region,
    country,
    SUM(telemetry_event_count) AS telemetry_events,
    SUM(affected_vehicle_count) AS affected_vehicle_groups,
    SUM(critical_dtc_count) AS critical_dtc_events,
    SUM(CASE WHEN severity IN ('HIGH', 'CRITICAL') THEN telemetry_event_count ELSE 0 END) AS warning_or_critical_events,
    AVG(avg_battery_voltage) AS avg_battery_voltage,
    AVG(avg_engine_temp_c) AS avg_engine_temp_c,
    CASE
        WHEN SUM(critical_dtc_count) > 0 THEN 'CRITICAL_ATTENTION'
        WHEN SUM(CASE WHEN severity = 'HIGH' THEN telemetry_event_count ELSE 0 END) > 0 THEN 'WATCHLIST'
        ELSE 'NORMAL'
    END AS risk_band
FROM {{ analytics_db }}.daily_vehicle_health_summary
GROUP BY
    business_date,
    model,
    customer_region,
    country;
