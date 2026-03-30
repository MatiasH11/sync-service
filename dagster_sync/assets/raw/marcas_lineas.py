from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, RetryPolicy, asset

from dagster_sync.resources import DistriRdsDbResource, WarehouseResource
from dagster_sync.types import RawMarcaLineaRow

COLUMNS = list(RawMarcaLineaRow.__annotations__.keys())


@asset(
    group_name='raw',
    retry_policy=RetryPolicy(max_retries=3, delay=60),
    op_tags={'resource': 'mysql'},
    description='Extrae marcas y líneas de MySQL → raw.raw_marcas_lineas',
)
def raw_marcas_lineas(
    context: AssetExecutionContext,
    distri_rds: DistriRdsDbResource,
    warehouse: WarehouseResource,
) -> MaterializeResult:
    rows = distri_rds.query("""
        SELECT
            ml.id       AS codigo_marca_int,
            m.MARCA_ID  AS marca_id,
            m.MARCA     AS marca,
            l.LINEA_ID  AS linea_id,
            l.LINEA     AS linea
        FROM MarcasxLineas ml
        LEFT JOIN Marcas m ON ml.marca_id = m.marca_id
        LEFT JOIN Lineas l ON ml.linea_id = l.linea_id
    """)

    inserted = warehouse.truncate_and_insert('raw.raw_marcas_lineas', COLUMNS, rows)

    context.log.info(f'Loaded {inserted} marcas_lineas into raw.raw_marcas_lineas')

    return MaterializeResult(
        metadata={
            'rows_written':    MetadataValue.int(inserted),
            'source':          MetadataValue.text('MySQL — MarcasxLineas / Marcas / Lineas'),
            'warehouse_table': MetadataValue.text('raw.raw_marcas_lineas'),
        }
    )
