import json
import random
import time
import uuid
from datetime import datetime

from kafka import KafkaProducer
from kafka.errors import KafkaError

TOPIC = "online_purchases"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=5,
    linger_ms=10,
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
            "timestamp": datetime.utcnow().isoformat(),
            "user_id": random.randint(1, 100),
            "product": product,
            "category": category,
            "price": round(base_price * random.uniform(0.8, 1.2), 2),
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

        time.sleep(5)

except KeyboardInterrupt:
    print("🛑 Stopping producer...")

finally:
    producer.flush()
    producer.close()
    print("✅ Producer closed cleanly")
