import json
import os
from confluent_kafka import Consumer, TopicPartition
from loguru import logger
from jafkafe.datatypes import defdict


_consumer:Consumer = None

def get_consumer() -> Consumer:
    """Get a kafka consumer instance (singleton pattern)"""
    global _consumer

    if _consumer is None:
        bootstraps = os.environ.get('FREEDS_KAFKA_BOOTSTRAP_SERVERS')
        config = {
            'bootstrap.servers': bootstraps,
            'group.id': 'jafkafe-consumer',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        }
        logger.info(f'Attempting to connect to kafka cluster through {bootstraps}')

        try:
            _consumer = Consumer(config)
            logger.info(f"Kafka consumer initialized with bootstrap servers: {config['bootstrap.servers']}")
        except Exception as e:
            logger.error(f"Failed to create Kafka consumer: {e}")
            raise

    return _consumer

def rewind(consumer: Consumer):
    """
    Rewind the consumer to read from the first message of each assigned partition.
    """
    # Get the currently assigned partitions
    assigned_partitions = consumer.assignment()

    if not assigned_partitions:
        logger.warning("No partitions assigned to the consumer. Cannot rewind.")
        return

    # Set the offset to the beginning for each assigned partition
    for tp in assigned_partitions:
        tp.offset = 0  # Offset 0 is the first message in the partition

    # Reassign the consumer to the updated partitions
    consumer.assign(assigned_partitions)


def assign_all_partitions(consumer: Consumer, topic: str):
    """
    Assign the consumer to all partitions of the given topic.
    """
    partitions = get_partitions(topic)
    consumer.assign(partitions)
    logger.info(f"Assigned to all partitions of topic '{topic}': {partitions}")

def close_consumer():
    """Gracefully close the consumer."""
    global _consumer
    if _consumer:
        _consumer.close()
        logger.info("Kafka consumer closed.")
        _consumer = None


def get_message(topic):
    """
    Get the next message from the given topic.
    Returns deserialized value or None if no message.
    """
    consumer = get_consumer()
    consumer.subscribe([topic])

    msg = consumer.poll(timeout=5.0)
    if msg is None:
        return None
    if msg.error():
        raise msg.error()

    try:
        value = json.loads(msg.value())
    except json.JSONDecodeError:
        value = msg.value().decode('utf-8')

    return defdict(value)

def get_partitions(topic)->list[TopicPartition]:
    # Fetch metadata for topic partitions
    meta = get_consumer().list_topics(topic)
    partitions = meta.topics[topic].partitions
    return [TopicPartition(topic, p) for p in partitions.keys()]

def get_head(topic):
    """
    Get the last message in the topic (latest message).
    """
    consumer = get_consumer()
    topic_partitions = get_partitions(topic)

    # For each partition, get the high watermark offset (end)
    last_msgs = []
    for tp in topic_partitions:
        low, high = consumer.get_watermark_offsets(tp)
        logger.debug(f"Topic: {topic} Partition {tp.partition}: Low={low}, High={high}, Messages={high-low}")
        if high-low <= 0:
            continue  # no messages in this partition
        # Seek to the last existing offset (high - 1)
        tp.offset = high - 1

        consumer.assign([tp])
        msg = consumer.poll(timeout=1.0)
        if msg and not msg.error():
            last_msgs.append(msg)

    if not last_msgs:
        return None

    # Return the last message with the highest timestamp
    msg = max(last_msgs, key=lambda m: m.timestamp()[1])

    try:
        value = json.loads(msg.value())
    except json.JSONDecodeError:
        value = msg.value().decode('utf-8')

    return defdict(value)
