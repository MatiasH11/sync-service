import os

import fdb
from dagster import ConfigurableResource


class PricesDbResource(ConfigurableResource):
    """Base de datos legacy de historial de precios de proveedor (Firebird)."""

    host: str = os.getenv('DB_FIREBIRD_DISTRI_PPAL_HOST', 'localhost')
    port: int = int(os.getenv('DB_FIREBIRD_DISTRI_PPAL_PORT', '3050'))
    database: str = os.getenv('DB_FIREBIRD_DISTRI_PPAL_NAME', '')
    user: str = os.getenv('DB_FIREBIRD_DISTRI_PPAL_USER', 'SYSDBA')
    password: str = os.getenv('DB_FIREBIRD_DISTRI_PPAL_PASSWORD', 'masterkey')

    def get_connection(self):
        host = self.host
        if host == 'localhost':
            host = os.getenv('DOCKER_HOST_ALIAS', 'host.docker.internal')
        return fdb.connect(
            host=host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            charset='UTF8',
        )

    def query(self, sql: str, params=None) -> list[dict]:
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(sql, params or [])
            columns = [desc[0] for desc in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            cursor.close()
            return rows
        finally:
            conn.close()
