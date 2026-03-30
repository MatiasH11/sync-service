from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, RetryPolicy, asset

from dagster_sync.resources import DistriRdsDbResource, WarehouseResource

COLUMNS = ['codigovendedor', 'nombre']


@asset(
    group_name='raw',
    retry_policy=RetryPolicy(max_retries=3, delay=60),
    op_tags={'resource': 'mysql'},
    description='Extrae vendedores activos de MySQL → raw.raw_sellers',
)
def raw_sellers(
    context: AssetExecutionContext,
    distri_rds: DistriRdsDbResource,
    warehouse: WarehouseResource,
) -> MaterializeResult:
    rows = distri_rds.query("""
        SELECT codigovendedor, razonsocialvend AS nombre
        FROM Vendedores
        WHERE activo = 1
    """)

    inserted = warehouse.truncate_and_insert('raw.raw_sellers', COLUMNS, rows)

    context.log.info(f'Loaded {inserted} sellers')

    return MaterializeResult(
        metadata={
            'rows_written':    MetadataValue.int(inserted),
            'source':          MetadataValue.text('MySQL — Vendedores'),
            'warehouse_table': MetadataValue.text('raw.raw_sellers'),
        }
    )
