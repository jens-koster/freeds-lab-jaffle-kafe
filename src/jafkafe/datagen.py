from dataclasses import dataclass, field
from pathlib import Path
import os
from jafkafegen.simulation import Simulation
from jafkafegen.stores.inventory import Inventory
from jafkafegen.stores.stock import Stock
import typing
import pickle
from loguru import logger
from jafkafe.datatypes import defdict, dictlist

@dataclass(frozen=True)
class Files:
    data_dir = Path(f"{os.environ.get('FREEDS_ROOT_PATH')}/data/jafkafe")
    jaffle_file = data_dir / "jaffle_data.pkl"
    state_file = data_dir / "jaffle_state.pkl"

@dataclass
class State:
    customers: defdict = field(default_factory=dict)
    last_sent_order_id: str = ""

    def add_customer(self, customer_id: str)->bool:
        if customer_id in self.customers:
            return False
        self.customers[customer_id] = customer_id
        return True

_state:State = None
_sim_data:defdict = {}
def save_data(file_path:Path, data: typing.Any):
    logger.info(f"Saving data to {file_path}")
    with open(file_path, "wb") as f:
        pickle.dump(data, f)

def load_data(file_path:Path)-> typing.Any:
    logger.info(f"Loading data from {file_path}")
    with open(file_path, "rb") as f:
        return pickle.load(f)

def save_state():
    save_data(Files.state_file, _state)

def init_sim(force: bool = False) -> tuple[State, defdict]:
    # Ensure data directory exists
    global _state, _sim_data

    Files.data_dir.mkdir(parents=True, exist_ok=True)

    # Load existing data if force=False and files exist
    if (
        force is False
        and Files.jaffle_file.exists()
        and Files.state_file.exists()):

        _state = load_data(Files.state_file)
        _sim_data = load_data(Files.jaffle_file)
        return _state, _sim_data

    s = Simulation(years=0, days=4,prefix = "dummy")
    s.run_simulation()
    orders = s.orders
    orders.sort(key=lambda x: x.day.date)
    customers = {c.id: c.to_dict() for c in s.customers.values()}
    _sim_data = {
        "customers": customers,
        "orders": [order.to_dict() for order in orders],
        "stores" : [market.store.to_dict() for market in s.markets],
        "supplies" : Stock().to_dict(),
        "products" : Inventory().to_dict(),
    }
    _state = State()
    save_data(Files.jaffle_file, _sim_data)
    save_data(Files.state_file, _state)
    return _state, _sim_data



