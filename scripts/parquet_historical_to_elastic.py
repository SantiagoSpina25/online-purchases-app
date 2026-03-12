import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

DATA_PATH = "/data/online_purchases"
STATE_PATH = f"{DATA_PATH}/_state/last_daily_metrics_date.txt"

spark = (
    SparkSession.builder.appName("Historical Metrics Revenue")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.ui.port", "4041")
    .getOrCreate()
)

df = spark.read.parquet(DATA_PATH).select(
    "year",
    "month",
    "day",
    "total_amount",
    "quantity",
    "purchase_id",
    "user_id",
)

with open(STATE_PATH, "r", encoding="utf-8") as file:
    raw = file.read().strip()
    last_date = datetime.strptime(raw, "%Y-%m-%d").date()

df = df.withColumn("date", make_date(col("year"), col("month"), col("day")))
df = df.filter(col("date") >= last_date).drop("date")

daily_metrics = df.groupBy("year", "month", "day").agg(
    round(sum("total_amount"), 2).alias("total_revenue"),
    round(avg("total_amount"), 2).alias("average_revenue"),
    round(sum("quantity"), 2).alias("total_quantity"),
    round(avg("quantity"), 2).alias("average_quantity"),
    countDistinct("purchase_id").alias("orders_count"),
    countDistinct("user_id").alias("distinct_users"),
)

daily_metrics = daily_metrics.withColumn(
    "doc_id",
    format_string(
        "%04d-%02d-%02d",
        col("year"),
        col("month"),
        col("day"),
    ),
)

max_date = (
    daily_metrics.select(make_date(col("year"), col("month"), col("day")).alias("date"))
    .agg(max("date").alias("max_date"))
    .first()["max_date"]
)

(
    daily_metrics.write.format("org.elasticsearch.spark.sql")
    .option("es.nodes", "elasticsearch")
    .option("es.port", "9200")
    .option("es.nodes.wan.only", "true")
    .option("es.nodes.discovery", "false")
    .option("es.net.ssl", "false")
    .option("es.net.http.auth.user", "elastic")
    .option("es.net.http.auth.pass", "elasticpass123!")
    .option("es.resource", "online-purchases-daily-metrics")
    .option("es.mapping.id", "doc_id")
    .option("es.write.operation", "upsert")
    .mode("append")
    .save()
)
os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
with open(STATE_PATH, "w", encoding="utf-8") as file:
    file.write(max_date.strftime("%Y-%m-%d"))
