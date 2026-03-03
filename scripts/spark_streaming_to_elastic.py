from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


HEARTBEAT_PATH = Path("/opt/spark-logs/stream_heartbeat")


def write_heartbeat() -> None:
    HEARTBEAT_PATH.parent.mkdir(parents=True, exist_ok=True)
    HEARTBEAT_PATH.touch()


# Crear sesion Spark
spark = (
    SparkSession.builder.appName("Spark Streaming - Elastic connection")
    .config("spark.sql.session.timeZone", "Europe/Madrid")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType(
    [
        StructField("purchase_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("user_id", StringType()),
        StructField("product", StringType()),
        StructField("category", StringType()),
        StructField("price", DoubleType()),
        StructField("quantity", IntegerType()),
        StructField("country", StringType()),
        StructField("payment_method", StringType()),
        StructField("device_type", StringType()),
    ]
)

rawData = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "online_purchases")
    .option("startingOffsets", "earliest")
    .load()
)

# Agrega los campos "total_amount", "year", "month", "month_string", "day", "hour", "day_of_week"
aggData = (
    rawData.selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
    .withColumn("total_amount", round(expr("price * quantity"), 2))
    .withColumn("year", year("timestamp"))
    .withColumn("month", month("timestamp"))
    .withColumn("month_string", date_format("timestamp", "MMMM"))
    .withColumn("day", dayofmonth("timestamp"))
    .withColumn("hour", hour("timestamp"))
    .withColumn("day_of_week", date_format("timestamp", "EEEE"))
)


# Escribir en Elasticsearch
def process_batch(batch_data, batch_id):
    # Genera una marca para que Airflow sepa que el stream sigue procesando
    write_heartbeat()

    # Guarda en formato parquet particionado por ano, mes, dia y hora
    (
        batch_data.write.mode("append")
        .format("parquet")
        .option("path", "/data/online_purchases")
        .option("checkpointLocation", f"/tmp/spark-checkpoints/parquet/{batch_id}")
        .partitionBy("year", "month", "day", "hour")
        .save()
    )

    print(f"✅ Batch {batch_id} guardado en Parquet")

    # Sube a elasticsearch
    (
        batch_data.write.format("org.elasticsearch.spark.sql")
        .option("es.nodes", "elasticsearch")
        .option("es.port", "9200")
        .option("es.nodes.wan.only", "true")
        .option("es.resource", "online-purchases-parsed")
        .option("es.mapping.id", "purchase_id")
        .mode("append")
        .save()
    )
    print(f"✅ Batch {batch_id} enviado a Elasticsearch")


query = (
    aggData.writeStream.outputMode("append")
    .foreachBatch(process_batch)
    .option("checkpointLocation", "/tmp/spark-checkpoints-online-purchases")
    .start()
)

query.awaitTermination()
