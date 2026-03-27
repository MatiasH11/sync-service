from dagster import asset, AssetExecutionContext, RetryPolicy

from dagster_sync.resources import SalesDbResource, WarehouseResource

COLUMNS = ['codigovendedor', 'nombre']


@asset(
    group_name='raw',
    retry_policy=RetryPolicy(max_retries=3, delay=60),
    description='Extrae vendedores activos de MySQL → raw.raw_sellers',
)
def raw_sellers(
    context: AssetExecutionContext,
    sales_db: SalesDbResource,
    warehouse: WarehouseResource,
) -> None:
    rows = sales_db.query("""
        SELECT codigovendedor, razonsocialvend AS nombre
        FROM Vendedores
        WHERE activo = 1
    """)

    inserted = warehouse.truncate_and_insert('raw.raw_sellers', COLUMNS, rows)

    context.add_output_metadata({'total_vendedores': inserted})
    context.log.info(f'Loaded {inserted} sellers')
