from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Guarda la ruta del archivo stream_heartbeat
HEARTBEAT_PATH = Path("/opt/spark-logs/stream_heartbeat")


# Funcion que asegura que la carpeta spark-logs exista y actualiza la fecha de modificacion del archivo para indicar que el proceso sigue vivo
def write_heartbeat():
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
        StructField("price", StringType()),
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
    .withColumns(
        {
            "year": year("timestamp"),
            "month": month("timestamp"),
            "month_string": date_format("timestamp", "MMMM"),
            "day": dayofmonth("timestamp"),
            "hour": hour("timestamp"),
            "day_of_week": date_format("timestamp", "EEEE"),
        }
    )
)

normalizedProducts = aggData.withColumns(
    {
        "product": trim(initcap(col("product"))),
        "category": trim(initcap(col("category"))),
        "price": round(trim(col("price")).cast("float") * (0.2 + rand() * 1.8), 2),
        "total_amount": round(expr("price * quantity"), 2),
    }
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
        .option("es.nodes.discovery", "false")
        .option("es.net.ssl", "false")
        .option("es.net.http.auth.user", "elastic")
        .option("es.net.http.auth.pass", "elasticpass123!")
        .option("es.resource", "online-purchases-parsed")
        .mode("append")
        .save()
    )
    print(f"✅ Batch {batch_id} enviado a Elasticsearch")


query = (
    normalizedProducts.writeStream.outputMode("append")
    .foreachBatch(process_batch)
    .option("checkpointLocation", "/tmp/spark-checkpoints-online-purchases")
    .start()
)

query.awaitTermination()
