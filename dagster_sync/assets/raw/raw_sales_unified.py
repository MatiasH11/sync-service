from dagster import AssetExecutionContext, AutomationCondition, MaterializeResult, MetadataValue, asset

from dagster_sync.resources import WarehouseResource

from .sales import PARTITIONS, raw_sales_dimds, raw_sales_dimppal, raw_sales_disds, raw_sales_disppal


@asset(
    name='raw_sales',
    deps=[raw_sales_dimds, raw_sales_dimppal, raw_sales_disds, raw_sales_disppal],
    partitions_def=PARTITIONS,
    automation_condition=AutomationCondition.eager(),
    group_name='raw',
    description=(
        'Centinela de linaje: señaliza que raw.raw_sales está completa para un mes '
        'cuando los 4 loaders (DIMDS, DIMPPAL, DISDS, DISPPAL) terminaron. '
        'El sensor raw_sales_dbt_sensor escucha este asset para lanzar dbt.'
    ),
)
def raw_sales(context: AssetExecutionContext, warehouse: WarehouseResource) -> MaterializeResult:
    start, _ = context.partition_time_window
    month = context.partition_key[:7]  # '2026-02'

    total_rows = warehouse.query_scalar(
        "SELECT COUNT(*) FROM raw.raw_sales WHERE DATE_TRUNC('month', fecha_comprobante) = %s",
        [start],
    ) or 0

    rows_by_source: dict[str, int] = {}
    for source in ('DIMDS', 'DIMPPAL', 'DISDS', 'DISPPAL'):
        count = warehouse.query_scalar(
            "SELECT COUNT(*) FROM raw.raw_sales"
            " WHERE DATE_TRUNC('month', fecha_comprobante) = %s AND db = %s",
            [start, source],
        ) or 0
        rows_by_source[source] = count

    context.log.info(
        f'raw.raw_sales lista para {month}: {total_rows} filas '
        f'({", ".join(f"{s}={n}" for s, n in rows_by_source.items())})'
    )

    return MaterializeResult(
        metadata={
            'partition_month':  MetadataValue.text(month),
            'total_rows':       MetadataValue.int(total_rows),
            'rows_dimds':       MetadataValue.int(rows_by_source['DIMDS']),
            'rows_dimppal':     MetadataValue.int(rows_by_source['DIMPPAL']),
            'rows_disds':       MetadataValue.int(rows_by_source['DISDS']),
            'rows_disppal':     MetadataValue.int(rows_by_source['DISPPAL']),
            'sources_loaded':   MetadataValue.int(4),
            'warehouse_table':  MetadataValue.text('raw.raw_sales'),
        }
    )
