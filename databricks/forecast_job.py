# Databricks notebook source
"""
Saans - AQI forecasting job (runs on Databricks).

This is the real distributed-processing piece of the pipeline: reads the
full historical archive from Snowflake into a Spark DataFrame, engineers
rolling features per location, trains a gradient-boosted regressor to
predict each location's AQI one hour ahead, and writes the predictions
back to Snowflake for the dashboard/API to serve.

HOW TO RUN THIS ON DATABRICKS:
  1. Databricks workspace -> Workspace -> Import -> upload this file
     (Databricks recognizes the "# Databricks notebook source" header
     and imports it as a notebook with cells split on "# COMMAND ----------").
  2. Cluster: any small cluster works (a single-node "Personal Compute"
     cluster is enough for this data volume). Runtime 14.x+ recommended.
  3. Set the four SNOWFLAKE_* values below via cluster environment
     variables or Databricks secrets (recommended) rather than hardcoding.
  4. Attach this notebook to the cluster and Run All -- or turn it into
     a scheduled Databricks Job (Workflows -> Create Job -> Notebook task)
     and trigger it from Airflow (see airflow/dags/databricks_forecast_dag.py).
"""

# COMMAND ----------

import os
from datetime import timedelta

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# --- Snowflake connection options -----------------------------------------
# Prefer Databricks secrets over plain env vars for real credentials:
#   dbutils.secrets.get(scope="saans", key="snowflake_password")
SNOWFLAKE_OPTIONS = {
    "sfURL": os.environ.get("SNOWFLAKE_ACCOUNT", "VPSREHI-PF65358.snowflakecomputing.com"),
    "sfUser": os.environ.get("SNOWFLAKE_USER", "Saans"),
    "sfPassword": os.environ.get("SNOWFLAKE_PASSWORD", "p9sDeRz8ufxY3HL"),
    "sfDatabase": "SAANS",
    "sfSchema": "CORE",
    "sfWarehouse": "SAANS_WH",
}

SNOWFLAKE_SOURCE = "net.snowflake.spark.snowflake"

# COMMAND ----------

# --- 1. Read the full historical archive from Snowflake -------------------
df = (
    spark.read.format(SNOWFLAKE_SOURCE)
    .options(**SNOWFLAKE_OPTIONS)
    .option("dbtable", "READINGS_ARCHIVE")
    .load()
)

print(f"Loaded {df.count()} historical readings")
df.printSchema()

# COMMAND ----------

# --- 2. Feature engineering: rolling stats per location --------------------
# Predict AQI one hour ahead using the last 3 hours of readings as context.
window_spec = Window.partitionBy("LOCATION").orderBy("RECORDED_AT")

featured = (
    df.withColumn("aqi_lag_1", F.lag("US_AQI", 1).over(window_spec))
    .withColumn("aqi_lag_2", F.lag("US_AQI", 2).over(window_spec))
    .withColumn("aqi_lag_3", F.lag("US_AQI", 3).over(window_spec))
    .withColumn(
        "rolling_avg_aqi",
        F.avg("US_AQI").over(window_spec.rowsBetween(-5, -1)),
    )
    .withColumn("hour_of_day", F.hour("RECORDED_AT"))
    .withColumn(
        "target_next_aqi",
        F.lead("US_AQI", 1).over(window_spec),
    )
    .na.drop(subset=["aqi_lag_1", "aqi_lag_2", "aqi_lag_3", "rolling_avg_aqi", "target_next_aqi"])
)

feature_cols = ["aqi_lag_1", "aqi_lag_2", "aqi_lag_3", "rolling_avg_aqi", "hour_of_day"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
training_data = assembler.transform(featured).select("LOCATION", "RECORDED_AT", "features", "target_next_aqi")

# COMMAND ----------

# --- 3. Train/test split + model -------------------------------------------
train_df, test_df = training_data.randomSplit([0.8, 0.2], seed=42)

gbt = GBTRegressor(featuresCol="features", labelCol="target_next_aqi", maxIter=50)
model = gbt.fit(train_df)

predictions = model.transform(test_df)

from pyspark.ml.evaluation import RegressionEvaluator

evaluator = RegressionEvaluator(labelCol="target_next_aqi", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print(f"Test RMSE: {rmse:.2f} AQI points")

# COMMAND ----------

# --- 4. Score the most recent reading per location for a live forecast -----
latest_per_location = (
    featured.withColumn(
        "rn", F.row_number().over(Window.partitionBy("LOCATION").orderBy(F.desc("RECORDED_AT")))
    )
    .filter("rn = 1")
)
latest_features = assembler.transform(latest_per_location)

forecast = (
    model.transform(latest_features)
    .withColumn("predicted_for", F.col("RECORDED_AT") + F.expr("INTERVAL 1 HOURS"))
    .withColumn("model_version", F.lit("gbt_v1"))
    .selectExpr(
        "LOCATION as location",
        "predicted_for",
        "prediction as predicted_aqi",
        "model_version",
    )
)

forecast.show(truncate=False)

# COMMAND ----------

# --- 5. Write predictions back to Snowflake --------------------------------
(
    forecast.write.format(SNOWFLAKE_SOURCE)
    .options(**SNOWFLAKE_OPTIONS)
    .option("dbtable", "AQI_PREDICTIONS_ARCHIVE")
    .mode("append")
    .save()
)

print("Forecast written to Snowflake: AQI_PREDICTIONS_ARCHIVE")

# COMMAND ----------

# --- 6. Optional: also push predictions to Supabase so the live dashboard --
# can show them without waiting on the next Snowflake sync. Uncomment and
# set SUPABASE_DB_URL as a cluster env var / secret to enable.
#
# import psycopg2
# rows = forecast.collect()
# conn = psycopg2.connect(os.environ["SUPABASE_DB_URL"])
# cur = conn.cursor()
# for r in rows:
#     cur.execute(
#         "insert into aqi_predictions (location, predicted_for, predicted_aqi, model_version) "
#         "values (%s, %s, %s, %s)",
#         (r["location"], r["predicted_for"], r["predicted_aqi"], r["model_version"]),
#     )
# conn.commit()
# cur.close()
# conn.close()
