from dagster import asset, AssetExecutionContext, RetryPolicy, MonthlyPartitionsDefinition

from dagster_sync.resources import PricesDbResource, WarehouseResource

PARTITIONS = MonthlyPartitionsDefinition(start_date='2023-01-01')

COLUMNS = ['codigoarticulo', 'precioactual', 'fechamodificacion', 'precio']


@asset(
    group_name='raw',
    partitions_def=PARTITIONS,
    retry_policy=RetryPolicy(max_retries=3, delay=60),
    description='Extrae historial de precios de Firebird → raw.raw_price_history (particionado por mes)',
)
def raw_price_history(
    context: AssetExecutionContext,
    prices_db: PricesDbResource,
    warehouse: WarehouseResource,
) -> None:
    start, end = context.partition_time_window

    context.log.info(
        f'Extracting price history for {context.partition_key} ({start} → {end})'
    )

    rows = prices_db.query(
        """
        SELECT
            a.CODIGOARTICULO,
            ap.PRECIO        AS PRECIOACTUAL,
            c.FECHAMODIFICACION,
            c.PRECIO
        FROM ARTICULOS a
        JOIN ARTICULOSPROVEEDOR ap
            ON ap.CODIGOARTICULO = a.CODIGOARTICULO
        JOIN CAMBIOSDEPRECIOPROVEEDOR c
            ON c.CODIGOARTICULOPROVEEDOR = ap.CODIGO
           AND c.FECHAMODIFICACION >= ?
           AND c.FECHAMODIFICACION < ?
        WHERE a.ACTIVO = 1
        GROUP BY a.CODIGOARTICULO, ap.PRECIO, c.FECHAMODIFICACION, c.PRECIO
        ORDER BY c.FECHAMODIFICACION DESC
        """,
        [start, end],
    )

    # Normalize Firebird uppercase keys to PostgreSQL lowercase
    normalized = [
        {
            'codigoarticulo':   row['CODIGOARTICULO'],
            'precioactual':     row['PRECIOACTUAL'],
            'fechamodificacion': row['FECHAMODIFICACION'],
            'precio':           row['PRECIO'],
        }
        for row in rows
    ]

    inserted = warehouse.delete_month_and_insert(
        table='raw.raw_price_history',
        month_column='fechamodificacion',
        partition_start=start,
        columns=COLUMNS,
        rows=normalized,
    )

    context.add_output_metadata({
        'partition': context.partition_key,
        'rows': inserted,
    })
    context.log.info(f'Loaded {inserted} price records for {context.partition_key}')
