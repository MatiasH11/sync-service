from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, RetryPolicy, asset

from dagster_sync.resources import DistriRdsDbResource, WarehouseResource

_PP_MONTHLY_COLUMNS = ['pp_year', 'pp_month', 'pp_discount_pct']
_PP_PROVIDER_COLUMNS = ['brand_code', 'provider_code', 'provider_name', 'brand_description', 'pp_discount_pct']


@asset(
    group_name='raw',
    retry_policy=RetryPolicy(max_retries=3, delay=60),
    op_tags={'resource': 'mysql'},
    description='Descuento PP promedio mensual desde distriap_distri.PROMEDIO_PRONTOPAGO_V2 → raw.raw_pp_monthly',
)
def raw_pp_monthly(
    context: AssetExecutionContext,
    distri_rds: DistriRdsDbResource,
    warehouse: WarehouseResource,
) -> MaterializeResult:
    """
    Carga el porcentaje de descuento por pronto pago promedio de cada mes.

    Fuente: distriap_distri.PROMEDIO_PRONTOPAGO_V2 (cross-DB query en MySQL AWS).
    Destino: raw.raw_pp_monthly (TRUNCATE + INSERT — reemplaza siempre el estado completo).

    Este valor se usa en int_sales_pp para calcular pp_price y pp_discount_pct.
    El fallback cuando no hay registro para un mes es 22 (definido en int_sales_pp).
    """
    context.log.info('Extracting monthly PP discounts from distriap_distri.PROMEDIO_PRONTOPAGO_V2')

    rows = distri_rds.query("""
        SELECT
            PP_YEAR        AS pp_year,
            PP_MONTH       AS pp_month,
            PROM_DESC_PP   AS pp_discount_pct
        FROM distriap_distri.PROMEDIO_PRONTOPAGO_V2
        ORDER BY PP_YEAR, PP_MONTH
    """)

    inserted = warehouse.truncate_and_insert('raw.raw_pp_monthly', _PP_MONTHLY_COLUMNS, rows)

    context.log.info(f'Loaded {inserted} monthly PP discount records')

    return MaterializeResult(
        metadata={
            'rows_written':    MetadataValue.int(inserted),
            'source_table':    MetadataValue.text('distriap_distri.PROMEDIO_PRONTOPAGO_V2'),
            'warehouse_table': MetadataValue.text('raw.raw_pp_monthly'),
        }
    )


@asset(
    group_name='raw',
    retry_policy=RetryPolicy(max_retries=3, delay=60),
    op_tags={'resource': 'mysql'},
    description='Descuentos PP por marca de proveedor desde DESC_PP_PROV → raw.raw_pp_provider',
)
def raw_pp_provider(
    context: AssetExecutionContext,
    distri_rds: DistriRdsDbResource,
    warehouse: WarehouseResource,
) -> MaterializeResult:
    """
    Carga los descuentos PP a nivel de marca de proveedor.

    Fuente: DESC_PP_PROV en MySQL AWS — keyed por CODIGOMARCA (marca interna).
    Destino: raw.raw_pp_provider (TRUNCATE + INSERT — reemplaza siempre el estado completo).
    Solo se cargan registros ACTIVO = 1.

    Este valor se usa en int_sales_pp para calcular pp_provider_cost por línea de venta.
    Marcas sin registro en esta tabla quedan con descuento PP = 0 (via COALESCE en dbt).
    """
    context.log.info('Extracting PP provider discounts from DESC_PP_PROV')

    rows = distri_rds.query("""
        SELECT
            CODIGOMARCA   AS brand_code,
            CODIGOPROV    AS provider_code,
            RAZONSOCIAL   AS provider_name,
            DESCRIPCION   AS brand_description,
            DESC_PP       AS pp_discount_pct
        FROM DESC_PP_PROV
        WHERE ACTIVO = 1
        ORDER BY CODIGOMARCA
    """)

    inserted = warehouse.truncate_and_insert('raw.raw_pp_provider', _PP_PROVIDER_COLUMNS, rows)

    context.log.info(f'Loaded {inserted} PP provider discount records')

    return MaterializeResult(
        metadata={
            'rows_written':    MetadataValue.int(inserted),
            'source_table':    MetadataValue.text('DESC_PP_PROV'),
            'warehouse_table': MetadataValue.text('raw.raw_pp_provider'),
        }
    )
