import json
import random
import time
import uuid
from datetime import datetime, timezone
from logger_config import setup_logger


from kafka import KafkaProducer
from kafka.errors import KafkaError


logger = setup_logger("producer")


TOPIC = "online_purchases"

# Configuración de Kafka
producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode(
        "utf-8"
    ),  # Convierte el diccionario de Python en bytes de JSON, que Kafka puede enviar
    acks="all",  # asegura que el mensaje llegó y fue replicado
    retries=5,  # si falla el envío, reintenta hasta 5 veces antes de tirar error
    linger_ms=10,  # espera hasta 10 milisegundos para agrupar varios mensajes antes de enviarlos
)

products = [
    # Electronics
    ("Laptop", "Electronics", 900),
    ("Phone", "Electronics", 600),
    ("Headphones", "Electronics", 150),
    ("Monitor", "Electronics", 300),
    ("Tablet", "Electronics", 400),
    ("Smartwatch", "Electronics", 250),
    ("Camera", "Electronics", 700),
    # Gaming
    ("Keyboard", "Gaming", 120),
    ("Mouse", "Gaming", 80),
    ("Gaming Chair", "Gaming", 350),
    ("Console", "Gaming", 500),
    ("VR Headset", "Gaming", 400),
    # Home Appliances
    ("Coffee Maker", "Home Appliances", 100),
    ("Blender", "Home Appliances", 70),
    ("Vacuum Cleaner", "Home Appliances", 200),
    ("Air Fryer", "Home Appliances", 150),
    # Fashion
    ("T-Shirt", "Fashion", 25),
    ("Jeans", "Fashion", 60),
    ("Sneakers", "Fashion", 120),
    ("Jacket", "Fashion", 150),
    # Books
    ("Novel", "Books", 20),
    ("Cookbook", "Books", 35),
    ("Comic Book", "Books", 15),
    # Sports
    ("Tennis Racket", "Sports", 120),
    ("Football", "Sports", 30),
    ("Yoga Mat", "Sports", 40),
]

countries = [
    "ESP",  # Spain
    "ARG",  # Argentina
    "USA",  # United States
    "BRA",  # Brazil
    "FRA",  # France
    "DEU",  # Germany
    "ITA",  # Italy
    "GBR",  # United Kingdom
    "PRT",  # Portugal
    "MEX",  # Mexico
    "CHL",  # Chile
    "COL",  # Colombia
    "PER",  # Peru
    "URY",  # Uruguay
    "CAN",  # Canada
    "JPN",  # Japan
    "CHN",  # China
    "KOR",  # South Korea
    "AUS",  # Australia
    "NZL",  # New Zealand
]

logger.info("🚀 Producer started")

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
            "country": random.choice(countries),
        }

        future = producer.send(TOPIC, purchase)

        try:
            record_metadata = future.get(timeout=10)
            logger.info(
                f"🛒 Sent → topic={record_metadata.topic} "
                f"partition={record_metadata.partition} "
                f"offset={record_metadata.offset}"  # offset: número de posición de un mensaje dentro de un topic
            )
        except KafkaError as e:
            logger.error(f"❌ Error sending message: {e}")

        time.sleep(random.randint(1, 2))

except KeyboardInterrupt:
    logger.info("🛑 Stopping producer...")

finally:
    producer.flush()  # fuerza al producer a enviar todo lo que está en memoria antes de continuar
    producer.close()
    logger.info("✅ Producer closed cleanly")
