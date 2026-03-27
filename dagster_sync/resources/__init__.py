from .sales_db import SalesDbResource
from .prices_db import PricesDbResource
from .warehouse import WarehouseResource
from .redis import RedisResource
from .client_service import ClientServiceResource

__all__ = [
    'SalesDbResource',
    'PricesDbResource',
    'WarehouseResource',
    'RedisResource',
    'ClientServiceResource',
]
