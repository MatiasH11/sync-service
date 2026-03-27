import os

import redis
from dagster import ConfigurableResource


class RedisResource(ConfigurableResource):
    """Cache Redis para coordinacion entre servicios."""

    host: str = os.getenv('REDIS_HOST', 'localhost')
    port: int = int(os.getenv('REDIS_PORT', '6379'))
    password: str = os.getenv('REDIS_PASSWORD', '')
    db: int = int(os.getenv('REDIS_DB_IDX', '1'))

    def get_client(self, decode_responses: bool = True) -> redis.Redis:
        return redis.Redis(
            host=self.host,
            port=self.port,
            password=self.password or None,
            db=self.db,
            decode_responses=decode_responses,
        )
