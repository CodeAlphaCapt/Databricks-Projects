# ==========================================================
# DATA QUALITY VALIDATION FRAMEWORK
# Project: Kafka → Databricks → Delta Lake
# Layers : Bronze → Silver → Gold
# ==========================================================

from pyspark.sql.functions import *
from datetime import datetime

validation_results = []

def validate(test_name, actual_value, expected_condition, status):
    validation_results.append({
        "Validation": test_name,
        "Actual_Value": actual_value,
        "Expected": expected_condition,
        "Status": status
    })

print("="*80)
print("PIPELINE DATA QUALITY VALIDATION REPORT")
print("Execution Time :", datetime.now())
print("="*80)

# ==========================================================
# LOAD TABLES
# ==========================================================

bronze_df = spark.table(
    "kafka_data.kafka_bronze.kafka_raw_stream"
)

silver_df = spark.table(
    "kafka_data.kafka_silver.clean_orders"
)

gold_df = spark.table(
    "kafka_data.kafka_gold.aggregated_sales"
)

# ==========================================================
# BRONZE VALIDATIONS
# ==========================================================

print("\n[BRONZE VALIDATIONS]")

bronze_count = bronze_df.count()

validate(
    "Bronze Record Count",
    bronze_count,
    "> 0",
    "PASS" if bronze_count > 0 else "FAIL"
)

# ==========================================================
# SILVER DATA QUALITY VALIDATIONS
# ==========================================================

print("\n[SILVER VALIDATIONS]")

null_records = silver_df.filter(
    col("order_id").isNull() |
    col("amount").isNull() |
    col("city").isNull()
).count()

validate(
    "Mandatory Field Validation",
    null_records,
    "0 Null Records",
    "PASS" if null_records == 0 else "FAIL"
)

duplicate_orders = (
    silver_df
    .groupBy("order_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

validate(
    "Duplicate Order Validation",
    duplicate_orders,
    "0 Duplicates",
    "PASS" if duplicate_orders == 0 else "FAIL"
)

invalid_amounts = silver_df.filter(
    col("amount") <= 0
).count()

validate(
    "Amount Business Rule",
    invalid_amounts,
    "Amount > 0",
    "PASS" if invalid_amounts == 0 else "FAIL"
)

# ==========================================================
# COMPLETENESS VALIDATION
# ==========================================================

silver_count = silver_df.count()

retention_pct = round(
    (silver_count / bronze_count) * 100,
    2
) if bronze_count > 0 else 0

validate(
    "Data Completeness",
    f"{retention_pct}%",
    ">= 90%",
    "PASS" if retention_pct >= 90 else "FAIL"
)

# ==========================================================
# GOLD VALIDATION
# ==========================================================

gold_count = gold_df.count()

validate(
    "Gold Aggregation Records",
    gold_count,
    "> 0",
    "PASS" if gold_count > 0 else "FAIL"
)

# ==========================================================
# SALES RECONCILIATION
# ==========================================================

silver_sales = (
    silver_df
    .agg(sum("amount").alias("sales"))
    .collect()[0]["sales"]
)

gold_sales = (
    gold_df
    .agg(sum("total_sales").alias("sales"))
    .collect()[0]["sales"]
)

sales_variance = abs(
    (silver_sales or 0) -
    (gold_sales or 0)
)

validate(
    "Sales Reconciliation",
    sales_variance,
    "Variance = 0",
    "PASS" if sales_variance == 0 else "FAIL"
)

# ==========================================================
# ORDER RECONCILIATION
# ==========================================================

silver_orders = silver_df.count()

gold_orders = (
    gold_df
    .agg(sum("total_orders").alias("orders"))
    .collect()[0]["orders"]
)

order_variance = abs(
    silver_orders -
    (gold_orders or 0)
)

validate(
    "Order Reconciliation",
    order_variance,
    "Variance = 0",
    "PASS" if order_variance == 0 else "FAIL"
)

# ==========================================================
# DATA FRESHNESS SLA
# ==========================================================

latest_event = silver_df.select(
    max("event_time")
).collect()[0][0]

if latest_event:

    lag_minutes = round(
        (
            datetime.now() -
            latest_event.replace(tzinfo=None)
        ).total_seconds() / 60,
        2
    )

else:
    lag_minutes = 99999

validate(
    "Data Freshness SLA",
    f"{lag_minutes} Minutes",
    "< 60 Minutes",
    "PASS" if lag_minutes < 60 else "FAIL"
)

# ==========================================================
# CITY LEVEL RECONCILIATION
# ==========================================================

city_variance = spark.sql("""

WITH silver_sales AS (

SELECT
city,
SUM(amount) AS silver_sales
FROM kafka_data.kafka_silver.clean_orders
GROUP BY city

),

gold_sales AS (

SELECT
city,
SUM(total_sales) AS gold_sales
FROM kafka_data.kafka_gold.aggregated_sales
GROUP BY city

)

SELECT COUNT(*) AS mismatch_count

FROM (

SELECT
s.city
FROM silver_sales s
JOIN gold_sales g
ON s.city = g.city

WHERE s.silver_sales <> g.gold_sales

)

""").collect()[0][0]

validate(
    "City Level Reconciliation",
    city_variance,
    "0 Mismatches",
    "PASS" if city_variance == 0 else "FAIL"
)

# ==========================================================
# VALIDATION SUMMARY
# ==========================================================

validation_df = spark.createDataFrame(validation_results)

print("\n")
print("="*80)
print("VALIDATION SUMMARY")
print("="*80)

display(validation_df)

# ==========================================================
# FINAL PIPELINE STATUS
# ==========================================================

failed_tests = validation_df.filter(
    col("Status") == "FAIL"
).count()

print("\n")

if failed_tests == 0:

    print("PIPELINE STATUS : HEALTHY")
    print("ALL VALIDATIONS PASSED")

else:

    print("PIPELINE STATUS : FAILED")
    print(f"Failed Tests : {failed_tests}")

    raise Exception(
        f"Pipeline Validation Failed. Failed Tests = {failed_tests}"
    )

print("="*80)
