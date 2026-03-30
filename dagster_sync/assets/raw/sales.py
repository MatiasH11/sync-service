from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, MonthlyPartitionsDefinition, RetryPolicy, asset

from dagster_sync.resources import DistriRdsDbResource, WarehouseResource
from dagster_sync.types import RawSaleRow
from dagster_sync.assets.raw._sales_query import SALES_TABLES, SalesTableConfig, build_sales_query

PARTITIONS = MonthlyPartitionsDefinition(start_date='2023-01-01', end_offset=1)

COLUMNS = list(RawSaleRow.__annotations__.keys())


def _make_raw_sales_asset(config: SalesTableConfig):
    db = config['db']

    @asset(
        name=f'raw_sales_{db.lower()}',
        group_name='raw',
        partitions_def=PARTITIONS,
        retry_policy=RetryPolicy(max_retries=3, delay=60),
        op_tags={'resource': 'mysql'},
        description=f'Extrae ventas de {db} MySQL → raw.raw_sales',
    )
    def _asset(
        context: AssetExecutionContext,
        distri_rds: DistriRdsDbResource,
        warehouse: WarehouseResource,
    ) -> MaterializeResult:
        start, end = context.partition_time_window
        month = context.partition_key[:7]

        context.log.info(f'[{db}] Extracting {month} ({start} → {end})')

        query = build_sales_query(config['cabeza'], config['cuerpo'], config['tipo_consumo'], db)
        rows: list[RawSaleRow] = distri_rds.query(query, (start, end))

        inserted = warehouse.delete_source_month_and_insert(
            table='raw.raw_sales',
            month_column='fecha_comprobante',
            source_column='db',
            source_value=db,
            partition_start=start,
            columns=COLUMNS,
            rows=rows,
        )

        if inserted == 0:
            context.log.warning(f'[{db}] No rows returned from source for {month}')

        context.log.info(f'[{db}] Loaded {inserted} rows for {month}')

        return MaterializeResult(
            metadata={
                'partition_month':  MetadataValue.text(month),
                'rows_written':     MetadataValue.int(inserted),
                'source_db':        MetadataValue.text(db),
                'source_tables':    MetadataValue.text(f'{config["cabeza"]} / {config["cuerpo"]}'),
                'warehouse_table':  MetadataValue.text('raw.raw_sales'),
            }
        )

    return _asset


_assets_by_db = {
    t['db']: _make_raw_sales_asset(t)
    for t in SALES_TABLES
}

raw_sales_dimds   = _assets_by_db['DIMDS']
raw_sales_dimppal = _assets_by_db['DIMPPAL']
raw_sales_disds   = _assets_by_db['DISDS']
raw_sales_disppal = _assets_by_db['DISPPAL']
