from dagster import asset, AssetExecutionContext, AutomationCondition

from .sales import PARTITIONS, raw_sales_dimds, raw_sales_dimppal, raw_sales_disds, raw_sales_disppal


@asset(
    name='raw_sales',
    deps=[raw_sales_dimds, raw_sales_dimppal, raw_sales_disds, raw_sales_disppal],
    partitions_def=PARTITIONS,
    automation_condition=AutomationCondition.eager(),
    group_name='raw',
    description=(
        'Marcador de linaje particionado: raw.raw_sales está completa para el mes '
        'cuando los 4 loaders (DIMDS, DIMPPAL, DISDS, DISPPAL) terminaron. '
        'Se materializa automáticamente cuando todos sus upstream completan.'
    ),
)
def raw_sales(context: AssetExecutionContext) -> None:
    context.log.info(f'raw.raw_sales lista para partición {context.partition_key}')
