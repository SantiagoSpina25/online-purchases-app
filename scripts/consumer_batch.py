import os
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json
from logger_config import setup_logger


def run_consumer_batch():
    logger = setup_logger("consumer_batch")

    BROKER = "kafka:9092"
    ELASTIC = "http://elasticsearch:9200"

    # Kafka
    consumer = KafkaConsumer(
        "online_purchases",
        bootstrap_servers=BROKER,
        auto_offset_reset="earliest",
        group_id="online-purchases_consumers_batch",
        value_deserializer=lambda v: json.loads(v),
    )

    # Elasticsearch
    es = Elasticsearch(ELASTIC)
    INDEX_NAME = "online-purchases"

    logger.info("🟢 Consumer started")

    # Solo leer 200 mensajes
    MAX_MESSAGES = 200
    count = 0

    for msg in consumer:
        purchase = msg.value
        es.index(index=INDEX_NAME, document=purchase)
        logger.info(f"✅ Inserted in elastic: {purchase.get('order_id', 'no_id')}")
        count += 1
        logger.info(f"Purchase number: {count}")
        if count >= MAX_MESSAGES:
            break

    consumer.close()
    logger.info("🟢 Consumer finished")
