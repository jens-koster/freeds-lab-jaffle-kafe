import datetime as dt
from zoneinfo import ZoneInfo
import typing
from time import sleep
import jafkafe.datagen as datagen
import jafkafe.kafka as kafka
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
    raise ValueError(f"Order id {order_id} not found in orders")



def run_simulator():

    state, sim_data = datagen.init_sim()

    index = ffwd(sim_data["orders"], state.last_sent_order_id)
    while True:
        order_cnt = 0
        for o in sim_data["orders"][index:]:
            # let time "pass" until we reach a future order
            now_dt = dt.datetime.now(ZoneInfo("Europe/Stockholm")).replace(tzinfo=None)
            o_dt = dt.datetime.fromisoformat(o["ordered_at"])
            secs = (o_dt - now_dt).total_seconds()
            if secs > 0:
                logger.info(f"Sent {order_cnt} orders, next order in {int(secs)} seconds")
                break
            # we're sending this order
            # also send customer data if it is the first time we see it
            customer_id = o["customer"]
            if state.add_customer(customer_id):
                customer_data = sim_data["customers"][customer_id]
                kafka.dispatch_customer(customer_data)
            kafka.dispatch_order(o)
            order_cnt += 1
            state.last_sent_order_id = o["id"]
            index += 1

        if order_cnt>0:
            datagen.save_state()
        sleep(10)


if __name__ == '__main__':
    logger.info("Starting jafkafe simulator")
    run_simulator()
