from pathlib import Path
from dataclasses import dataclass, field
import jafkafe.kafka_consumer as kafka_consumer
from jafkafe.datatypes import defdict
from jafkafe.constants import topics
from loguru import logger

class State:

    def __init__(self):
        last_order = kafka_consumer.get_head(topics.orders)
        self.last_sent_order_id = last_order['id'] if last_order else None
        self.customers = {}


        consumer = kafka_consumer.get_consumer()
        kafka_consumer.assign_all_partitions(consumer=consumer, topic=topics.customers)
        kafka_consumer.rewind(consumer=consumer)
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                break
            if msg.key() is None:
                continue
            customer_id = msg.key().decode('utf-8')
            self.add_customer(customer_id=customer_id)


    def add_customer(self, customer_id: str)->bool:
        if customer_id in self.customers:
            return False
        self.customers[customer_id] = customer_id
        return True

    def __repr__(self)->str:
        return f'State - last order: {self.last_sent_order_id}, #customers: {len(self.customers)}'

print(State())