{% set business_date = dag_run.conf.get('business_date', macros.datetime.utcnow().strftime('%Y-%m-%d')) %}
{% set analytics_db = params.analytics_database %}
{% set staging_db = params.staging_database %}

SELECT
    'vehicle_health_enriched' AS table_name,
    '{{ business_date }}' AS business_date,
    COUNT(*) AS iceberg_row_count
FROM {{ analytics_db }}.vehicle_health_enriched
WHERE business_date = '{{ business_date }}';

SELECT
    'daily_vehicle_health_summary' AS table_name,
    '{{ business_date }}' AS business_date,
    COUNT(*) AS iceberg_row_count
FROM {{ analytics_db }}.daily_vehicle_health_summary
WHERE business_date = '{{ business_date }}';

SELECT
    'service_kpi_summary' AS table_name,
    '{{ business_date }}' AS business_date,
    COUNT(*) AS iceberg_row_count
FROM {{ analytics_db }}.service_kpi_summary
WHERE business_date = '{{ business_date }}';

SELECT
    'data_quality_report' AS table_name,
    '{{ business_date }}' AS business_date,
    COUNT(*) AS iceberg_row_count
FROM {{ analytics_db }}.data_quality_report
WHERE business_date = '{{ business_date }}';

SELECT
    'vehicle_health_enriched' AS table_name,
    staging.row_count AS staging_row_count,
    target.row_count AS iceberg_row_count,
    target.row_count - staging.row_count AS row_count_delta
FROM
    (
        SELECT COUNT(*) AS row_count
        FROM {{ staging_db }}.vehicle_health_enriched_parquet
        WHERE business_date = '{{ business_date }}'
    ) staging,
    (
        SELECT COUNT(*) AS row_count
        FROM {{ analytics_db }}.vehicle_health_enriched
        WHERE business_date = '{{ business_date }}'
    ) target;

SELECT
    'daily_vehicle_health_summary' AS table_name,
    staging.row_count AS staging_row_count,
    target.row_count AS iceberg_row_count,
    target.row_count - staging.row_count AS row_count_delta
FROM
    (
        SELECT COUNT(*) AS row_count
        FROM {{ staging_db }}.daily_vehicle_health_summary_parquet
        WHERE business_date = '{{ business_date }}'
    ) staging,
    (
        SELECT COUNT(*) AS row_count
        FROM {{ analytics_db }}.daily_vehicle_health_summary
        WHERE business_date = '{{ business_date }}'
    ) target;

SELECT
    'service_kpi_summary' AS table_name,
    staging.row_count AS staging_row_count,
    target.row_count AS iceberg_row_count,
    target.row_count - staging.row_count AS row_count_delta
FROM
    (
        SELECT COUNT(*) AS row_count
        FROM {{ staging_db }}.service_kpi_summary_parquet
        WHERE business_date = '{{ business_date }}'
    ) staging,
    (
        SELECT COUNT(*) AS row_count
        FROM {{ analytics_db }}.service_kpi_summary
        WHERE business_date = '{{ business_date }}'
    ) target;

SELECT
    'data_quality_report' AS table_name,
    staging.row_count AS staging_row_count,
    target.row_count AS iceberg_row_count,
    target.row_count - staging.row_count AS row_count_delta
FROM
    (
        SELECT COUNT(*) AS row_count
        FROM {{ staging_db }}.data_quality_report_parquet
        WHERE business_date = '{{ business_date }}'
    ) staging,
    (
        SELECT COUNT(*) AS row_count
        FROM {{ analytics_db }}.data_quality_report
        WHERE business_date = '{{ business_date }}'
    ) target;

SELECT
    dataset_name,
    metric_name,
    metric_value,
    status,
    business_date,
    pipeline_run_id,
    reported_at
FROM {{ analytics_db }}.data_quality_report
WHERE business_date = '{{ business_date }}'
ORDER BY reported_at DESC;

SELECT
    business_date,
    model,
    customer_region,
    country,
    telemetry_events,
    warning_or_critical_events,
    critical_dtc_events,
    risk_band
FROM {{ analytics_db }}.vehicle_health_risk_story
WHERE business_date = '{{ business_date }}'
ORDER BY
    critical_dtc_events DESC,
    warning_or_critical_events DESC,
    telemetry_events DESC;
