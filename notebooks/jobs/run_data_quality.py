# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Checks
# MAGIC 
# MAGIC This notebook runs comprehensive data quality checks on Gold layer tables

# COMMAND ----------

# Get parameters
dbutils.widgets.text("catalog", "iot_streaming_dev")
dbutils.widgets.text("schema_prefix", "dev")

catalog = dbutils.widgets.get("catalog")
schema_prefix = dbutils.widgets.get("schema_prefix")

gold_schema = f"gold_{schema_prefix}"
silver_schema = f"silver_{schema_prefix}"

print(f"Checking catalog: {catalog}")
print(f"Gold schema: {gold_schema}")
print(f"Silver schema: {silver_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 1: Table Row Counts

# COMMAND ----------

tables_to_check = [
    ("dim_customers", gold_schema),
    ("dim_devices", gold_schema),
    ("dim_locations", gold_schema),
    ("fct_iot_events", gold_schema),
    ("fct_orders", gold_schema),
    ("fct_alerts", gold_schema),
    ("agg_daily_device_metrics", gold_schema),
    ("agg_customer_summary", gold_schema),
]

row_counts = []
for table, schema in tables_to_check:
    try:
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM {catalog}.{schema}.{table}").collect()[0]["cnt"]
        row_counts.append((table, schema, count, "OK" if count > 0 else "EMPTY"))
    except Exception as e:
        row_counts.append((table, schema, 0, f"ERROR: {str(e)[:50]}"))

# Display results
df = spark.createDataFrame(row_counts, ["table", "schema", "row_count", "status"])
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 2: Data Freshness

# COMMAND ----------

freshness_checks = []

# IoT Events freshness
try:
    result = spark.sql(f"""
        SELECT 
            MAX(event_timestamp) as latest_event,
            MAX(_loaded_at) as latest_load,
            TIMESTAMPDIFF(HOUR, MAX(event_timestamp), CURRENT_TIMESTAMP()) as hours_since_event
        FROM {catalog}.{gold_schema}.fct_iot_events
    """).collect()[0]
    
    hours = result["hours_since_event"]
    status = "OK" if hours is None or hours < 6 else ("WARN" if hours < 24 else "CRITICAL")
    freshness_checks.append(("fct_iot_events", str(result["latest_event"]), hours, status))
except Exception as e:
    freshness_checks.append(("fct_iot_events", "N/A", None, f"ERROR: {str(e)[:50]}"))

# Alerts freshness
try:
    result = spark.sql(f"""
        SELECT 
            MAX(alert_timestamp) as latest_alert,
            TIMESTAMPDIFF(HOUR, MAX(alert_timestamp), CURRENT_TIMESTAMP()) as hours_since_alert
        FROM {catalog}.{gold_schema}.fct_alerts
    """).collect()[0]
    
    hours = result["hours_since_alert"]
    status = "OK" if hours is None or hours < 24 else "WARN"
    freshness_checks.append(("fct_alerts", str(result["latest_alert"]), hours, status))
except Exception as e:
    freshness_checks.append(("fct_alerts", "N/A", None, f"ERROR: {str(e)[:50]}"))

df_fresh = spark.createDataFrame(freshness_checks, ["table", "latest_timestamp", "hours_ago", "status"])
display(df_fresh)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 3: Null Value Analysis

# COMMAND ----------

# Check for unexpected nulls in key columns
null_checks = []

key_columns = [
    ("dim_customers", "customer_id", gold_schema),
    ("dim_devices", "device_id", gold_schema),
    ("fct_iot_events", "event_id", gold_schema),
    ("fct_iot_events", "device_key", gold_schema),
    ("fct_orders", "customer_key", gold_schema),
]

for table, column, schema in key_columns:
    try:
        result = spark.sql(f"""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) as null_count
            FROM {catalog}.{schema}.{table}
        """).collect()[0]
        
        null_pct = (result["null_count"] / result["total"] * 100) if result["total"] > 0 else 0
        status = "OK" if null_pct == 0 else ("WARN" if null_pct < 1 else "CRITICAL")
        null_checks.append((table, column, result["total"], result["null_count"], round(null_pct, 2), status))
    except Exception as e:
        null_checks.append((table, column, 0, 0, 0, f"ERROR: {str(e)[:50]}"))

df_nulls = spark.createDataFrame(null_checks, ["table", "column", "total_rows", "null_count", "null_pct", "status"])
display(df_nulls)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 4: Referential Integrity

# COMMAND ----------

# Check foreign key relationships
integrity_checks = []

# Devices should have valid locations
try:
    orphans = spark.sql(f"""
        SELECT COUNT(*) as cnt
        FROM {catalog}.{gold_schema}.dim_devices d
        LEFT JOIN {catalog}.{gold_schema}.dim_locations l ON d.location_key = l.location_key
        WHERE d.location_key IS NOT NULL AND l.location_key IS NULL
    """).collect()[0]["cnt"]
    
    status = "OK" if orphans == 0 else "WARN"
    integrity_checks.append(("dim_devices -> dim_locations", orphans, status))
except Exception as e:
    integrity_checks.append(("dim_devices -> dim_locations", -1, f"ERROR: {str(e)[:50]}"))

# Events should have valid devices
try:
    orphans = spark.sql(f"""
        SELECT COUNT(*) as cnt
        FROM {catalog}.{gold_schema}.fct_iot_events e
        LEFT JOIN {catalog}.{gold_schema}.dim_devices d ON e.device_key = d.device_key
        WHERE e.device_key IS NOT NULL 
          AND e.device_key != '{{ dbt_utils.generate_surrogate_key(["UNKNOWN"]) }}'
          AND d.device_key IS NULL
    """).collect()[0]["cnt"]
    
    status = "OK" if orphans == 0 else "WARN"
    integrity_checks.append(("fct_iot_events -> dim_devices", orphans, status))
except Exception as e:
    integrity_checks.append(("fct_iot_events -> dim_devices", -1, f"ERROR: {str(e)[:50]}"))

df_integrity = spark.createDataFrame(integrity_checks, ["relationship", "orphan_count", "status"])
display(df_integrity)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Aggregate all checks
all_statuses = (
    [r[3] for r in row_counts] + 
    [r[3] for r in freshness_checks] + 
    [r[5] for r in null_checks] + 
    [r[2] for r in integrity_checks]
)

critical_count = sum(1 for s in all_statuses if "CRITICAL" in str(s))
warn_count = sum(1 for s in all_statuses if "WARN" in str(s))
error_count = sum(1 for s in all_statuses if "ERROR" in str(s))
ok_count = sum(1 for s in all_statuses if s == "OK")

print("=" * 50)
print("DATA QUALITY SUMMARY")
print("=" * 50)
print(f"✅ OK: {ok_count}")
print(f"⚠️  Warnings: {warn_count}")
print(f"🔴 Critical: {critical_count}")
print(f"❌ Errors: {error_count}")
print("=" * 50)

if critical_count > 0 or error_count > 0:
    print("\n❌ Data quality checks FAILED")
    # Uncomment to fail the job:
    # raise Exception(f"Data quality failed: {critical_count} critical, {error_count} errors")
else:
    print("\n✅ All data quality checks passed!")
