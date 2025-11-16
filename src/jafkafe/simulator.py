import datetime as dt
from zoneinfo import ZoneInfo
import typing
from time import sleep
from jafkafe.state import State
import jafkafe.datagen as datagen
import jafkafe.kafka_producer as kafka_producer

from loguru import logger


from jafkafe.datatypes import defdict, dictlist


def ffwdt(orders: dictlist) -> int:
    now = dt.datetime.now(ZoneInfo("Europe/Stockholm")).replace(tzinfo=None).isoformat()
    for index in range(0,len(orders)-1):
        if orders[index]["ordered_at"] >= now:
            return index
    return None

def ffwd(orders: dictlist, order_id:str) -> int:
    if not order_id:
        return ffwdt(orders)
    for index in range(0,len(orders)-1):
        if orders[index]["id"] == order_id:
            return index + 1
    return None

def now()->dt.datetime:
    return dt.datetime.now(ZoneInfo("Europe/Stockholm")).replace(tzinfo=None)

def now_offset(time_at: dt.datetime)->int:
    return (time_at - now()).total_seconds()

def order_offset_from_now(order:defdict)->int:
    o_dt = dt.datetime.fromisoformat(order["ordered_at"])
    return now_offset(o_dt)

def run_simulator():
    sim_data = datagen.get_sim_data()
    state = State()
    logger.info(f'Staring in state: {state}')
    index = ffwd(sim_data["orders"], state.last_sent_order_id)
    if index is None:
        # first run or sim data us stale. get new data and ffwd to now.
        logger.info('out of data, generating new simulation')
        sim_data = datagen.get_sim_data(force=True)
        index = ffwdt(sim_data["orders"])
    while True:
        order_cnt = 0
        for o in sim_data["orders"][index:]:
            # let time "pass" until we reach a future order
            if (offset := order_offset_from_now(o)) > 0:
                # don't send yet
                logger.info(f"Sent {order_cnt} orders, next order in {int(offset)} seconds")
                break
            # send this order
            # also send customer data if it is the first time we see it
            customer_id = o["customer"]
            if state.add_customer(customer_id):
                # this si anew customer, send the info
                customer_data = sim_data["customers"][customer_id]
                kafka_producer.dispatch_customer(customer_data)
            kafka_producer.dispatch_order(o)
            order_cnt += 1
            state.last_sent_order_id = o["id"]
            index += 1
        sleep(10)


if __name__ == '__main__':
    logger.info("Starting jafkafe simulator")
    run_simulator()
