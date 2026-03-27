from dagster import asset, AssetExecutionContext, RetryPolicy

from dagster_sync.resources import DistriRdsDbResource, WarehouseResource
from dagster_sync.types import RawRubroRow

COLUMNS = list(RawRubroRow.__annotations__.keys())


@asset(
    group_name='raw',
    retry_policy=RetryPolicy(max_retries=3, delay=60),
    op_tags={'resource': 'mysql'},
    description='Extrae rubros y super-rubros de MySQL → raw.raw_rubros',
)
def raw_rubros(
    context: AssetExecutionContext,
    distri_rds: DistriRdsDbResource,
    warehouse: WarehouseResource,
) -> None:
    rows = distri_rds.query("""
        SELECT
            r.CODIGORUBRO                               AS codigo_rubro,
            r.CODIGOSUPERRUBRO                          AS codigo_super_rubro,
            COALESCE(u.DESCRIPCION, 'SIN SUPERRUBRO')   AS descripcion_super_rubro,
            COALESCE(u.UNIDADES, 0)                     AS unidades_super_rubro
        FROM RUBROS r
        LEFT JOIN UNIDADESXRUBRO u
            ON r.CODIGOSUPERRUBRO = u.CODIGOSUPERRUBRO
    """)

    inserted = warehouse.truncate_and_insert('raw.raw_rubros', COLUMNS, rows)

    context.add_output_metadata({'total_rubros': inserted})
    context.log.info(f'Loaded {inserted} rubros into raw.raw_rubros')
