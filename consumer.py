from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json

# Configuración de Kafka
consumer = KafkaConsumer(
    "online_purchases",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",  # lee desde el inicio
    group_id="online-purchases_consumers",
    value_deserializer=lambda v: json.loads(
        v
    ),  # Convierte los bytes de Kafka en dict de Python
)

# Configuración de Elasticsearch
es = Elasticsearch("http://localhost:9200")

INDEX_NAME = "online-purchases"

required_fields = ["order_id", "product", "category", "price", "quantity", "timestamp"]

print("🟢 Consumer escuchando...")

try:

    for msg in consumer:

        purchase = msg.value  # dict ya parseado

        if not all(fields in purchase for fields in required_fields):
            continue

        if "country" not in purchase:
            purchase["country"] = "Unknown"

        # Indexamos en Elasticsearch
        es.index(
            index=INDEX_NAME, document=purchase
        )  # 	Inserta cada compra en Elastic bajo el índice purchases

        order_id = purchase.get("order_id", "without_id")

        print(f"✅ Insertado en ES: {order_id}")

except KeyboardInterrupt:
    print("🛑 Stopping consumer...")

finally:
    consumer.close()
    print("✅ Consumer closed cleanly")
