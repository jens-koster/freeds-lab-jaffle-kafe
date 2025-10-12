import datetime as dt
from pathlib import Path
import pickle
from zoneinfo import ZoneInfo
import typing
from dataclasses import dataclass, field
from time import sleep
from jafgen.simulation import Simulation
from jafgen.customers.customers import Customer
from jafgen.customers.order import Order
from jafgen.customers.tweet import Tweet
from jafgen.stores.inventory import Inventory
from jafgen.stores.market import Market
from jafgen.stores.stock import Stock
from jafgen.stores.store import Store

defdict = dict[str, typing.Any]
optdefdict = defdict | None

dictlist = list[defdict]
optdictlist = dictlist | None


@dataclass(frozen=True)
class Files:
    jaffle_file = Path("jaffle_data.pkl")
    state_file = Path("jaffle_state.pkl")


@dataclass
class State:
    customers: defdict = field(default_factory=dict)
    last_sent_order_id: str = ""
    def add_customer(self, customer_id: str,  ):
        self.customers[customer_id] = customer_id

def init_sim():
    s = Simulation(years=0, days=100,prefix = "dummy")
    s.run_simulation()
    orders = s.orders
    orders.sort(key=lambda x: x.day.date)
    sim_data = {
        "customers": s.customers,
        "orders": [order.to_dict() for order in orders],
        "stores" : [market.store.to_dict() for market in s.markets],
        "supplies" : Stock().to_dict(),
        "products" : Inventory().to_dict(),
    }
    save_data(Files.jaffle_file, sim_data)
    save_data(Files.state_file, State())


def save_data(file_path:Path, data: typing.Any):
    with open(file_path, "wb") as f:
        pickle.dump(data, f)

def load_data(file_path:Path)-> typing.Any:
    with open(file_path, "rb") as f:
        return pickle.load(f)


def ffwdt(orders: dictlist) -> int:
    now = dt.datetime.now(ZoneInfo("Europe/Stockholm")).replace(tzinfo=None).isoformat()
    for index in range(0,len(orders)-1):
        if orders[index]["ordered_at"] >= now:
            return index
    return None

def ffwd(orders: dictlist, order_id:str) -> int:
    if not order_id:
        return 0
    for index in range(0,len(orders)-1):
        if orders[index]["id"] == order_id:
            return index + 1
    raise ValueError(f"Order id {order_id} not found in orders")

def dispatch_order(order: defdict):
    print(f"Sending order: {order}")

def dispatch_customer(customer: defdict):
    print(f"Sending customer: {customer}")



if not (Files.jaffle_file.exists() and Files.state_file.exists()):
    init_sim()
state = load_data(Files.state_file)
sim_data:defdict = load_data(Files.jaffle_file)

index = ffwd(sim_data["orders"], state.last_sent_order_id)
while True:
    now = dt.datetime.now(ZoneInfo("Europe/Stockholm")).replace(tzinfo=None).isoformat()
    for o in sim_data["orders"][index:]:
        # let time "pass" until we reach a future order
        if o["ordered_at"] >= now:
            break
        # we're sending this order
        # send customer if it is the first time we see it
        customer_id = o["customer"]
        if not state.customers.get(customer_id):
            customer_data = sim_data["customers"][customer_id]
            dispatch_customer(customer_data)
            state.add_customer(customer_id)
        dispatch_order(o)
        state.last_sent_order_id = o["id"]
        index += 1

    sleep(10)