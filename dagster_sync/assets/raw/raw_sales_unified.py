from dagster import asset, AssetExecutionContext

from .sales import PARTITIONS, raw_sales_dimds, raw_sales_dimppal, raw_sales_disds, raw_sales_disppal


@asset(
    name='raw_sales',
    deps=[raw_sales_dimds, raw_sales_dimppal, raw_sales_disds, raw_sales_disppal],
    partitions_def=PARTITIONS,
    group_name='raw',
    description=(
        'Marcador de linaje particionado: raw.raw_sales está completa para el mes '
        'cuando los 4 loaders (DIMDS, DIMPPAL, DISDS, DISPPAL) terminaron. '
        'Usá este asset como punto de entrada para materializar ventas por mes.'
    ),
)
def raw_sales(context: AssetExecutionContext) -> None:
    context.log.info(f'raw.raw_sales lista para partición {context.partition_key}')
