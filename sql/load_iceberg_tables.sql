{% set business_date = dag_run.conf.get('business_date', macros.datetime.utcnow().strftime('%Y-%m-%d')) %}
{% set analytics_db = params.analytics_database %}
{% set staging_db = params.staging_database %}

DELETE FROM {{ analytics_db }}.vehicle_health_enriched
WHERE business_date = '{{ business_date }}';

INSERT INTO {{ analytics_db }}.vehicle_health_enriched (
    source_file,
    vin,
    event_timestamp,
    event_date,
    odometer_km,
    battery_voltage,
    engine_temp_c,
    oil_pressure_kpa,
    tire_pressure_fl,
    tire_pressure_fr,
    tire_pressure_rl,
    tire_pressure_rr,
    dtc_code,
    severity,
    country,
    business_date,
    source_zip,
    pipeline_run_id,
    ingested_at,
    model_year,
    powertrain,
    production_plant,
    warranty_start_date,
    customer_region,
    dealer_id,
    service_type,
    service_status,
    warranty_flag,
    service_duration_hours,
    dealer_name,
    region,
    dealer_tier,
    is_critical_event,
    is_warning_event,
    model
)
SELECT
    source_file,
    vin,
    event_timestamp,
    event_date,
    odometer_km,
    battery_voltage,
    engine_temp_c,
    oil_pressure_kpa,
    tire_pressure_fl,
    tire_pressure_fr,
    tire_pressure_rl,
    tire_pressure_rr,
    dtc_code,
    severity,
    country,
    business_date,
    source_zip,
    pipeline_run_id,
    ingested_at,
    model_year,
    powertrain,
    production_plant,
    warranty_start_date,
    customer_region,
    dealer_id,
    service_type,
    service_status,
    warranty_flag,
    service_duration_hours,
    dealer_name,
    region,
    dealer_tier,
    is_critical_event,
    is_warning_event,
    model
FROM {{ staging_db }}.vehicle_health_enriched_parquet
WHERE business_date = '{{ business_date }}';

DELETE FROM {{ analytics_db }}.daily_vehicle_health_summary
WHERE business_date = '{{ business_date }}';

INSERT INTO {{ analytics_db }}.daily_vehicle_health_summary (
    business_date,
    event_date,
    country,
    customer_region,
    model_year,
    powertrain,
    severity,
    telemetry_event_count,
    affected_vehicle_count,
    critical_dtc_count,
    avg_odometer_km,
    max_odometer_km,
    avg_battery_voltage,
    avg_engine_temp_c,
    repeated_warning_vehicle_count,
    model
)
SELECT
    business_date,
    event_date,
    country,
    customer_region,
    model_year,
    powertrain,
    severity,
    telemetry_event_count,
    affected_vehicle_count,
    critical_dtc_count,
    avg_odometer_km,
    max_odometer_km,
    avg_battery_voltage,
    avg_engine_temp_c,
    repeated_warning_vehicle_count,
    model
FROM {{ staging_db }}.daily_vehicle_health_summary_parquet
WHERE business_date = '{{ business_date }}';

DELETE FROM {{ analytics_db }}.service_kpi_summary
WHERE business_date = '{{ business_date }}';

INSERT INTO {{ analytics_db }}.service_kpi_summary (
    business_date,
    dealer_id,
    dealer_name,
    country,
    dealer_tier,
    model,
    model_year,
    powertrain,
    service_type,
    service_event_count,
    avg_service_duration_hours,
    avg_labor_hours,
    total_parts_cost,
    warranty_service_rate,
    region
)
SELECT
    business_date,
    dealer_id,
    dealer_name,
    country,
    dealer_tier,
    model,
    model_year,
    powertrain,
    service_type,
    service_event_count,
    avg_service_duration_hours,
    avg_labor_hours,
    total_parts_cost,
    warranty_service_rate,
    region
FROM {{ staging_db }}.service_kpi_summary_parquet
WHERE business_date = '{{ business_date }}';

DELETE FROM {{ analytics_db }}.data_quality_report
WHERE business_date = '{{ business_date }}';

INSERT INTO {{ analytics_db }}.data_quality_report (
    dataset_name,
    metric_name,
    metric_value,
    status,
    business_date,
    pipeline_run_id,
    reported_at
)
SELECT
    dataset_name,
    metric_name,
    metric_value,
    status,
    business_date,
    pipeline_run_id,
    reported_at
FROM {{ staging_db }}.data_quality_report_parquet
WHERE business_date = '{{ business_date }}';
