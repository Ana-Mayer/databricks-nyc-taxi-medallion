# Databricks notebook source
# MAGIC %md
# MAGIC # NYC Taxi Medallion Pipeline (Bronze → Silver → Gold)
# MAGIC Source: `samples.nyctaxi.trips` (built into every workspace, no download).
# MAGIC Storage: Delta tables governed by Unity Catalog.
# MAGIC
# MAGIC | Layer | Purpose |
# MAGIC |---|---|
# MAGIC | Bronze | Raw copy + ingestion metadata |
# MAGIC | Silver | Cleaned, validated, enriched |
# MAGIC | Gold | Business aggregates for BI / dashboards |

# COMMAND ----------

# On Free Edition the default catalog is `workspace`. Change if yours differs.
CATALOG = "workspace"
SCHEMA = "taxi_demo"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Explore the source

# COMMAND ----------

raw = spark.table("samples.nyctaxi.trips")
raw.printSchema()
display(raw.limit(10))
print("rows:", raw.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bronze: raw data, untouched, plus lineage columns

# COMMAND ----------

from pyspark.sql import functions as F

bronze = (
    spark.table("samples.nyctaxi.trips")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source", F.lit("samples.nyctaxi.trips"))
)
bronze.write.format("delta").mode("overwrite").saveAsTable("bronze_trips")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silver: clean, validate, enrich
# MAGIC Rules: positive fare and distance, dropoff after pickup, plausible speed, dedupe.

# COMMAND ----------

b = spark.table("bronze_trips")

enriched = (
    b.withColumn(
        "duration_min",
        (F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long")) / 60,
    )
    .withColumn("avg_mph", F.col("trip_distance") / (F.col("duration_min") / 60))
    .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
    .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    .withColumn("pickup_dow", F.date_format("tpep_pickup_datetime", "E"))
)

silver = (
    enriched.filter(
        (F.col("fare_amount") > 0)
        & (F.col("trip_distance") > 0)
        & (F.col("duration_min") > 0)
        & (F.col("avg_mph") < 100)
    )
    .dropDuplicates(["tpep_pickup_datetime", "tpep_dropoff_datetime", "pickup_zip", "dropoff_zip", "fare_amount"])
    .withColumn("fare_per_mile", F.col("fare_amount") / F.col("trip_distance"))
)
silver.write.format("delta").mode("overwrite").saveAsTable("silver_trips")

# Data quality report: how many rows did we reject, and why?
n_bronze, n_silver = b.count(), silver.count()
print(f"bronze={n_bronze:,}  silver={n_silver:,}  rejected={n_bronze - n_silver:,} ({(n_bronze - n_silver) / n_bronze:.1%})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Gold: business-ready aggregates

# COMMAND ----------

s = spark.table("silver_trips")

# Revenue and demand by day
(
    s.groupBy("pickup_date")
    .agg(
        F.count("*").alias("trips"),
        F.round(F.sum("fare_amount"), 2).alias("total_fare"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
    )
    .orderBy("pickup_date")
    .write.format("delta").mode("overwrite").saveAsTable("gold_daily_revenue")
)

# Demand by hour of day
(
    s.groupBy("pickup_hour")
    .agg(F.count("*").alias("trips"), F.round(F.avg("fare_amount"), 2).alias("avg_fare"))
    .orderBy("pickup_hour")
    .write.format("delta").mode("overwrite").saveAsTable("gold_hourly_demand")
)

# Top pickup zones
(
    s.groupBy("pickup_zip")
    .agg(
        F.count("*").alias("trips"),
        F.round(F.sum("fare_amount"), 2).alias("total_fare"),
        F.round(F.avg("fare_per_mile"), 2).alias("avg_fare_per_mile"),
    )
    .orderBy(F.desc("trips"))
    .write.format("delta").mode("overwrite").saveAsTable("gold_zone_summary")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT pickup_hour, trips, avg_fare FROM gold_hourly_demand ORDER BY pickup_hour

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT pickup_zip, trips, total_fare, avg_fare_per_mile
# MAGIC FROM gold_zone_summary ORDER BY trips DESC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Delta features worth showing off

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY silver_trips

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Time travel: query the table as of its first version
# MAGIC SELECT count(*) FROM silver_trips VERSION AS OF 0


