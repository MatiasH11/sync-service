import io
import csv
import json
import os
from datetime import datetime

import psycopg2
from dagster import ConfigurableResource


class WarehouseResource(ConfigurableResource):
    """Warehouse de analytics (PostgreSQL). Destino unico de todos los raw assets."""

    host: str = os.getenv('WAREHOUSE_HOST', 'localhost')
    port: int = int(os.getenv('WAREHOUSE_PORT', '5433'))
    database: str = os.getenv('WAREHOUSE_DATABASE', 'analytics')
    user: str = os.getenv('WAREHOUSE_USER', 'dagster')
    password: str = os.getenv('WAREHOUSE_PASSWORD', 'dagster_local')

    def get_connection(self):
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.user,
            password=self.password,
        )

    def execute(self, sql: str, params=None):
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            conn.commit()
            cursor.close()
        finally:
            conn.close()

    def _copy_insert(self, cursor, table: str, columns: list[str], rows: list[dict]):
        """COPY rows into table using CSV format. Runs within an existing cursor."""
        buffer = io.StringIO()
        writer = csv.writer(buffer, delimiter='\t')
        for row in rows:
            writer.writerow([
                json.dumps(row.get(col)) if isinstance(row.get(col), (dict, list))
                else row.get(col)
                for col in columns
            ])
        buffer.seek(0)
        cols = ', '.join(f'"{c}"' for c in columns)
        cursor.copy_expert(
            f"COPY {table}({cols}) FROM STDIN WITH (FORMAT csv, DELIMITER '\t', NULL 'None')",
            buffer,
        )

    def truncate_and_insert(
        self, table: str, columns: list[str], rows: list[dict]
    ) -> int:
        """TRUNCATE + COPY en una sola transaccion atomica."""
        if not rows:
            return 0

        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(f'TRUNCATE TABLE {table}')
            self._copy_insert(cursor, table, columns, rows)
            conn.commit()
            cursor.close()
            return len(rows)
        finally:
            conn.close()

    def delete_month_and_insert(
        self,
        table: str,
        month_column: str,
        partition_start: datetime,
        columns: list[str],
        rows: list[dict],
    ) -> int:
        """DELETE rows for a calendar month then COPY insert atomically."""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                f"DELETE FROM {table} WHERE DATE_TRUNC('month', {month_column}) = %s",
                [partition_start],
            )
            if rows:
                self._copy_insert(cursor, table, columns, rows)
            conn.commit()
            cursor.close()
            return len(rows)
        finally:
            conn.close()

    def delete_source_month_and_insert(
        self,
        table: str,
        month_column: str,
        source_column: str,
        source_value: str,
        partition_start: datetime,
        columns: list[str],
        rows: list[dict],
    ) -> int:
        """DELETE rows for a calendar month + source identifier, then COPY insert atomically.

        Allows multiple independent sources to write to the same table
        without overwriting each other's data.
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                f"DELETE FROM {table}"
                f" WHERE DATE_TRUNC('month', {month_column}) = %s"
                f" AND {source_column} = %s",
                [partition_start, source_value],
            )
            if rows:
                self._copy_insert(cursor, table, columns, rows)
            conn.commit()
            cursor.close()
            return len(rows)
        finally:
            conn.close()
