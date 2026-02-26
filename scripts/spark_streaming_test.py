from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession.builder.appName("Spark Streaming Test").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType(
    [
        StructField("order_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("user_id", StringType()),
        StructField("product", StringType()),
        StructField("category", StringType()),
        StructField("price", DoubleType()),
        StructField("quantity", IntegerType()),
        StructField("country", StringType()),
    ]
)

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "online_purchases")
    .option("startingOffsets", "earliest")
    .load()
)

parsed = (
    raw.selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp("timestamp"))
    .withColumn("total_amount", expr("price * quantity"))
)

query = (
    parsed.writeStream.format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
)

query.awaitTermination()
