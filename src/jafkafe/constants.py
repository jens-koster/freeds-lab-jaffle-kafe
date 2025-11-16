"""static values used in more than one module"""
from dataclasses import dataclass, field

@dataclass(frozen=True)
class topics:
    orders = 'orders'
    customers = 'customers'
