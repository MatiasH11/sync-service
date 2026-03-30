from dagster import asset, AssetExecutionContext, AutomationCondition

from .sales import PARTITIONS, raw_sales_dimds, raw_sales_dimppal, raw_sales_disds, raw_sales_disppal


@asset(
    name='raw_sales',
    deps=[raw_sales_dimds, raw_sales_dimppal, raw_sales_disds, raw_sales_disppal],
    partitions_def=PARTITIONS,
    # eager() es semántico al rol de este asset: es un centinela que debe
    # activarse en cuanto sus 4 dependencias completan para la misma partición.
    # No es un schedule externo — es la definición de cuándo el centinela existe.
    automation_condition=AutomationCondition.eager(),
    group_name='raw',
    description=(
        'Centinela de linaje: señaliza que raw.raw_sales está completa para un mes '
        'cuando los 4 loaders (DIMDS, DIMPPAL, DISDS, DISPPAL) terminaron. '
        'El sensor raw_sales_dbt_sensor escucha este asset para lanzar dbt.'
    ),
)
def raw_sales(context: AssetExecutionContext) -> None:
    context.log.info(f'raw.raw_sales lista para partición {context.partition_key}')
