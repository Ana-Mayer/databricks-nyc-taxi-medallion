# NYC Taxi Medallion Pipeline on Databricks

A bronze --> silver --> gold data pipeline built on Databricks with PySpark, Delta Lake and Unity Catalog, using the built-in `samples.nyctaxi.trips` dataset.

| Layer | Table | What it does |
|---|---|---|
| Bronze | `bronze_trips` | Raw copy of the source plus ingestion metadata |
| Silver | `silver_trips` | Validated (positive fare/distance, dropoff after pickup, speed < 100 mph), deduplicated, enriched with duration, speed, hour, day of week |
| Gold | `gold_daily_revenue`, `gold_hourly_demand`, `gold_zone_summary` | Business aggregates for dashboards |

The silver step prints a data-quality report showing how many rows were rejected.

## Run it
1. Sign up for [Databricks Free Edition](https://www.databricks.com/learn/free-edition).
2. Workspace → Create → Git folder, and paste this repo's URL (or import `nyc_taxi_medallion.py` as a notebook).
3. Attach serverless compute and run the notebook. If your catalog isn't `workspace`, change `CATALOG` in the first cell.

## Delta features demonstrated
- `DESCRIBE HISTORY` and time travel (`VERSION AS OF`)
- Unity Catalog three-level namespace and lineage
