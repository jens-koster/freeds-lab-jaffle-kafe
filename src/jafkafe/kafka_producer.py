import json
import os
from confluent_kafka import Producer
from loguru import logger
from jafkafe.datatypes import defdict
from jafkafe.constants import topics

# Global producer instance
_producer = None

def get_producer() -> Producer:
    """Get a kafka producer instance (singleton pattern)"""
    global _producer

    if _producer is None:
        # Kafka configuration
        bootstraps = os.environ.get('FREEDS_KAFKA_BOOTSTRAP_SERVERS')
        config = {
            'bootstrap.servers': bootstraps,
            'client.id': 'jafkafe-simulator',
            'acks': 'all',  # Wait for all replicas to acknowledge
            'retries': 3,   # Retry failed sends
            'delivery.timeout.ms': 30000,  # milliseconds
            'request.timeout.ms': 5000,    # milliseconds
        }
        logger.info(f'Attempting to connect to kafka cluster through{bootstraps}')

        try:
            _producer = Producer(config)
            logger.info(f"Kafka producer initialized with bootstrap servers: {config['bootstrap.servers']}")
        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {e}")
            raise

    return _producer

def delivery_report(err, msg):
    """Callback for message delivery reports"""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def send_message(topic: str, message: dict, key: str = None):
    """Send a message to Kafka topic"""
    producer = get_producer()
    logger.info(f"Send message topic {topic}, key: {key}")
    try:
        # Serialize message to JSON
        value = json.dumps(message).encode('utf-8')
        key_bytes = key.encode('utf-8') if key else None

        # Send message asynchronously
        producer.produce(
            topic=topic,
            value=value,
            key=key_bytes,
            callback=delivery_report
        )

        # Optional: Poll only if queue is getting full
        # This prevents memory buildup while being efficient
        if len(producer) > 100:  # If more than 100 messages queued
            producer.poll(0)

    except Exception as e:
        logger.error(f"Failed to send message to topic {topic}: {e}")
        raise

def flush_producer():
    """Flush any pending messages"""
    if _producer is not None:
        _producer.flush()
        logger.debug("Kafka producer flushed")

def close_producer():
    """Close the Kafka producer gracefully"""
    global _producer
    if _producer is not None:
        _producer.flush()  # Ensure all messages are sent
        _producer = None
        logger.info("Kafka producer closed")


def dispatch_order(order: defdict):
    """Send one order to Kafka"""
    topic = topics.orders
    order_id = order['id']
    send_message(
        topic=topic,
        message=order,
        key=order_id
    )

def dispatch_customer(customer: defdict):
    """Send one customer to Kafka"""
    topic = topics.customers
    customer_id = customer['id']
    send_message(
        topic=topic,
        message=customer,
        key=customer_id
    )


# if __name__ == '__main__':
#     # Example usage
#     sample_order = {
#         'id': 'order_123',
#         'customer_id': 'cust_456',
#         'msg': 'Sample order message'
#     }
#     dispatch_order(sample_order)
#     flush_producer()