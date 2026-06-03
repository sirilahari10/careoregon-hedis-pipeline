import pandas import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, when, count, round

# 1. INITIALIZE DATABRICKS/SPARK ENVIRONMENT
print("[INFO] Initializing PySpark Session for Quality Metrics Analysis...")
spark = SparkSession.builder \
    .appName("HEDIS_Prenatal_Care_Metric") \
    .master("local[*]") \
    .getOrCreate()

# 2. DATA ENGINEERING: SIMULATE CLAIMS & ENROLLMENT
print("[INFO] Loading Simulated Member Claims and Enrollment Data...")

data = [
    {"member_id": "M001", "delivery_date": "2025-10-15", "enrollment_date": "2025-01-01", "first_prenatal_visit": "2025-03-10"},
    {"member_id": "M002", "delivery_date": "2025-11-20", "enrollment_date": "2024-11-01", "first_prenatal_visit": "2025-09-05"},
    {"member_id": "M003", "delivery_date": "2025-12-05", "enrollment_date": "2025-06-01", "first_prenatal_visit": "2025-06-15"},
    {"member_id": "M004", "delivery_date": "2025-08-30", "enrollment_date": "2025-02-01", "first_prenatal_visit": None}
]


df = spark.createDataFrame(pd.DataFrame(data))

df = df.withColumn("delivery_date", col("delivery_date").cast("date")) \
       .withColumn("enrollment_date", col("enrollment_date").cast("date")) \
       .withColumn("first_prenatal_visit", col("first_prenatal_visit").cast("date"))

# 3. CLINICAL QUALITY MEASURE (HEDIS LOGIC)
print("[INFO] Calculating HEDIS 'Timeliness of Prenatal Care' Compliance...")


df_metrics = df.withColumn(
    "days_before_delivery", datediff(col("delivery_date"), col("first_prenatal_visit"))
).withColumn(
    "days_after_enrollment", datediff(col("first_prenatal_visit"), col("enrollment_date"))
)

df_compliance = df_metrics.withColumn(
    "is_compliant",
    when(
        (col("first_prenatal_visit").isNotNull()) &
        ((col("days_before_delivery") >= 175) | (col("days_after_enrollment") <= 42)),
        1
    ).otherwise(0)
)

# 4. AGGREGATION & REPORTING
summary = df_compliance.agg(
    count("member_id").alias("Total_Deliveries"),
    sum("is_compliant").alias("Compliant_Visits")
).withColumn(
    "HEDIS_Care_Rate_Pct",
    round((col("Compliant_Visits") / col("Total_Deliveries")) * 100, 1)
)

print("\n--- CareOregon Quality Improvement Dashboard Summary ---")
summary.show()

print("[ACTIONABLE INSIGHT] Identifying the non-compliant cohort allows for targeted SDOH interventions and provider outreach.")

# Clean up
spark.stop()
