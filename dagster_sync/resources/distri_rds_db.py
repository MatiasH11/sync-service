import os

import mysql.connector
from dagster import ConfigurableResource


class DistriRdsDbResource(ConfigurableResource):
    """Base de datos MySQL en AWS RDS (ventas, catálogo, clientes)."""

    host: str = os.getenv('AWS_DISTRI_DB_HOST', 'localhost')
    port: int = int(os.getenv('AWS_DISTRI_DB_PORT', '3306'))
    database: str = os.getenv('AWS_DISTRI_DB_NAME', 'distrisuper')
    user: str = os.getenv('AWS_DISTRI_DB_USER', 'root')
    password: str = os.getenv('AWS_DISTRI_DB_PASSWORD', '')

    def get_connection(self):
        host = self.host
        if host == 'localhost':
            host = os.getenv('DOCKER_HOST_ALIAS', 'host.docker.internal')
        return mysql.connector.connect(
            host=host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
        )

    def query(self, sql: str, params=None) -> list[dict]:
        conn = self.get_connection()
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(sql, params or ())
            rows = cursor.fetchall()
            cursor.close()
            return rows
        finally:
            conn.close()
