import os
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError

import fdb
from dagster import ConfigurableResource

# fdb usa libfbclient (C nativo) que crea sockets propios fuera del control de
# Python. socket.setdefaulttimeout() no tiene efecto. La única forma confiable
# de imponer un timeout es ejecutar fdb.connect() en un thread separado y
# abandonarlo si no responde a tiempo.
_CONNECT_TIMEOUT_SECONDS = 30


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

        def _connect():
            return fdb.connect(
                host=host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                charset='UTF8',
            )

        # No usar 'with' — ThreadPoolExecutor.__exit__ llama shutdown(wait=True),
        # que bloquea en t.join() esperando el thread de libfbclient (C nativo).
        # Si hay timeout, ese thread queda colgado y join() nunca retorna.
        # shutdown(wait=False) abandona el thread sin esperar y devuelve el control.
        executor = ThreadPoolExecutor(max_workers=1)
        future = executor.submit(_connect)
        try:
            conn = future.result(timeout=_CONNECT_TIMEOUT_SECONDS)
            executor.shutdown(wait=False)
            return conn
        except FutureTimeoutError:
            executor.shutdown(wait=False)
            raise TimeoutError(
                f'Firebird connection timed out after {_CONNECT_TIMEOUT_SECONDS}s '
                f'— posible conexión colgada previa en {host}:{self.port}'
            )

    def query(self, sql: str, params=None) -> list[dict]:
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, params or [])
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
            finally:
                cursor.close()
        finally:
            conn.close()
