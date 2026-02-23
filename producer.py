import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

TOPIC = "online_purchases"

# Configuración de Kafka
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode(
        "utf-8"
    ),  # Convierte el diccionario de Python en bytes de JSON, que Kafka puede enviar
    acks="all",  # asegura que el mensaje llegó y fue replicado
    retries=5,  # si falla el envío, reintenta hasta 5 veces antes de tirar error
    linger_ms=10,  # espera hasta 10 milisegundos para agrupar varios mensajes antes de enviarlos
)

products = [
    ("Laptop", "Electronics", 900),
    ("Phone", "Electronics", 600),
    ("Headphones", "Electronics", 150),
    ("Keyboard", "Gaming", 120),
    ("Mouse", "Gaming", 80),
    ("Monitor", "Electronics", 300),
]

print("🚀 Producer started")

try:
    while True:
        product, category, base_price = random.choice(products)

        purchase = {
            "order_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "user_id": random.randint(1, 100),
            "product": product,
            "category": category,
            "price": round(base_price * random.uniform(0.5, 1.5), 2),
            "quantity": random.randint(1, 3),
        }

        future = producer.send(TOPIC, purchase)

        try:
            record_metadata = future.get(timeout=10)
            print(
                f"🛒 Sent → topic={record_metadata.topic} "
                f"partition={record_metadata.partition} "
                f"offset={record_metadata.offset}"
            )
        except KafkaError as e:
            print(f"❌ Error sending message: {e}")

        time.sleep(random.randint(1, 3))

except KeyboardInterrupt:
    print("🛑 Stopping producer...")

finally:
    producer.flush()  # fuerza al producer a enviar todo lo que está en memoria antes de continuar
    producer.close()
    print("✅ Producer closed cleanly")
