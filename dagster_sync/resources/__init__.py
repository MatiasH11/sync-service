from .distri_rds_db import DistriRdsDbResource
from .prices_db import PricesDbResource
from .warehouse import WarehouseResource
from .redis import RedisResource
from .client_service import ClientServiceResource
from .product_service import ProductServiceResource

__all__ = [
    'DistriRdsDbResource',
    'PricesDbResource',
    'WarehouseResource',
    'RedisResource',
    'ClientServiceResource',
    'ProductServiceResource',
]
