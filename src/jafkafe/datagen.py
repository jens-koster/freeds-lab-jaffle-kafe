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
    data_dir = Path(f"{os.environ.get('FDS_ROOT_PATH')}/data/jafkafe")
    jaffle_file = data_dir / "jaffle_data.pkl"

def save_data(file_path:Path, data: typing.Any):
    logger.info(f"Saving data to {file_path}")
    with open(file_path, "wb") as f:
        pickle.dump(data, f)

def load_data(file_path:Path)-> typing.Any:
    logger.info(f"Loading data from {file_path}")
    with open(file_path, "rb") as f:
        return pickle.load(f)


def get_sim_data(force: bool = False) -> defdict:
    """Generate or load sim data as a dictionary."""
    # Ensure data directory exists
    Files.data_dir.mkdir(parents=True, exist_ok=True)

    # Load existing data if force=False and files exist
    if not force and Files.jaffle_file.exists():
        return load_data(Files.jaffle_file)

    s = Simulation(years=0, days=4,prefix = "dummy")
    s.run_simulation()
    orders = s.orders
    orders.sort(key=lambda x: x.day.date)
    customers = {c.id: c.to_dict() for c in s.customers.values()}
    sim_data = {
        "customers": customers,
        "orders": [order.to_dict() for order in orders],
        "stores" : [market.store.to_dict() for market in s.markets],
        "supplies" : Stock().to_dict(),
        "products" : Inventory().to_dict(),
    }
    save_data(Files.jaffle_file, sim_data)
    return sim_data
