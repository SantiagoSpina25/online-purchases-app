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

# Constantes para controlar el tiempo de producción random
FAST_PRODUCTION = 1
SLOW_PRODUCTION = 10
ULTRA_SLOW_PRODUCTION = 20

# Configuración de Kafka
producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode(
        "utf-8"
    ),  # Convierte el diccionario de Python en bytes de JSON, que Kafka puede enviar
    acks="all",  # Espera confirmacion de todas las replicas (en este caso solo hay 1)
    retries=5,  # si falla el envío, reintenta hasta 5 veces antes de tirar error
    linger_ms=10,  # espera hasta 10 milisegundos para agrupar varios mensajes antes de enviarlos
)


# Listado de productos con inconsistencias
products = [
    ("Laptop", "Electronics", 900),
    ("PHONE", "electronics", "600"),
    ("Headphones ", "Electronics", 150.0),
    ("monitor", "Electronics", 300),
    ("Tablet", "ELECTRONICS", 400),
    ("SmartWatch", "Electronics", "250.0"),
    ("camera", "Electronics", 700),
    ("Keyboard", "Gaming", 120.0),
    ("mouse", "GAMING", 80),
    ("Gaming Chair", "gaming", 350),
    ("Console ", "Gaming", "500.0"),
    ("VR HEADSET", "Gaming", 400),
    ("Coffee Maker", "Home Appliances", "100"),
    ("blender", "Home Appliances", 70),
    ("Vacuum Cleaner", "Home appliances", 200),
    ("Air Fryer", "HOME APPLIANCES", 150.0),
    ("Toaster", "Home appliances", "50"),
    ("Microwave", "home appliances", 180.0),
    ("T-Shirt", "Fashion", 25),
    ("JEANS", "fashion", 60.0),
    ("Sneakers", "FASHION", "120"),
    ("Jacket", "Fashion", 150),
    ("Hat", "fashion", "35"),
    ("Socks", "Fashion", 10.0),
    ("Novel", "books", "20"),
    ("cookbook", "Books", 35.0),
    ("Comic Book", "Books", 15),
    ("Magazine", "Books", "12"),
    ("Tennis Racket", "Sports", 120),
    ("Football", "sports", "30.0"),
    ("Yoga Mat", "SPORTS", 40),
    ("Basketball", "sports", "25"),
    ("Running Shoes", "Sports", 90.0),
    ("Smart Speaker", "Electronics", "180"),
    ("E-Book Reader", "Electronics", 130.0),
    ("Drone", "Electronics", "650"),
    ("VR Gloves", "Gaming", 220),
    ("Gaming Desk", "GAMING", "300.0"),
    ("Fitness Tracker", "Electronics", 110),
    ("Air Purifier", "Home Appliances", "200"),
    ("Electric Kettle", "Home appliances", 60.0),
    ("Hoodie", "Fashion", "50"),
    ("Running Shorts", "fashion", 30.0),
]

countries = [
    "ESP",
    "ARG",
    "USA",
    "BRA",
    "FRA",
    "DEU",
    "ITA",
    "GBR",
    "PRT",
    "MEX",
    "CHL",
    "COL",
    "PER",
    "URY",
    "CAN",
    "JPN",
    "CHN",
    "KOR",
    "AUS",
    "NZL",
]

payment_methods = ["card", "card", "card", "card", "transfer", "transfer", "wallet"]

device_types = [
    "mobile",
    "mobile",
    "mobile",
    "mobile",
    "mobile",
    "desktop",
    "desktop",
    "desktop",
    "tablet",
]

logger.info("🟢 Producer started")

try:
    while True:
        product, category, base_price = random.choice(products)

        purchase = {
            "purchase_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "user_id": random.randint(1, 10000),
            "product": product,
            "category": category,
            "price": base_price,
            "quantity": random.randint(1, 5),
            "country": random.choice(countries),
            "payment_method": random.choice(payment_methods),
            "device_type": random.choice(device_types),
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

        time.sleep(random.randint(1, ULTRA_SLOW_PRODUCTION))

except KeyboardInterrupt:
    logger.info("🛑 Stopping producer...")

finally:
    producer.flush()  # fuerza al producer a enviar todo lo que está en memoria antes de continuar
    producer.close()
    logger.info("🟢 Producer closed cleanly") 
