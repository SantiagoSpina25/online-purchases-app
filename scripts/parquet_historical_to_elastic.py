from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = (
    SparkSession.builder.appName("Historical Metrics Revenue")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.ui.port", "4041")
    .getOrCreate()
)

df = spark.read.parquet("/data/online_purchases")

# hourly_metrics = df.groupBy("year", "month", "day", "hour").agg(
#     round(sum("total_amount"), 2).alias("revenue_sum")
# )

daily_metrics = df.groupBy("year", "month", "day").agg(
    round(sum("total_amount"), 2).alias("total_revenue"),
    round(avg("total_amount"), 2).alias("average_revenue"),
    round(sum("quantity"), 2).alias("total_quantity"),
    round(avg("quantity"), 2).alias("average_quantity"),
    countDistinct("purchase_id").alias("orders_count"),
    countDistinct("user_id").alias("distinct_users"),
)

# hourly_metrics.show(10, truncate=False)
# daily_metrics.show(truncate=False)

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
    .mode("overwrite")
    .save()
)
