from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, RetryPolicy, asset

from dagster_sync.resources import ProductServiceResource, WarehouseResource
from dagster_sync.types import RawArticuloRow, ProductApiResponse

COLUMNS = list(RawArticuloRow.__annotations__.keys())


def _map_articulo(product: ProductApiResponse) -> RawArticuloRow:
    attrs = product.get('attributes', {})
    marca = attrs.get('marca') or {}

    return RawArticuloRow(
        codigo_articulo=product.get('id', ''),
        codigo_particular=attrs.get('codigoParticular', ''),
        descripcion=attrs.get('descripcion', ''),
        codigo_rubro=attrs.get('codigoRubro', ''),
        codigo_marca=attrs.get('codigoMarca', ''),
        descripcion_marca=marca.get('descripcion', ''),
    )


@asset(
    group_name='raw',
    retry_policy=RetryPolicy(max_retries=3, delay=60),
    description='Extrae artículos de product-service → raw.raw_articulos',
)
def raw_articulos(
    context: AssetExecutionContext,
    product_service: ProductServiceResource,
    warehouse: WarehouseResource,
) -> MaterializeResult:
    all_products = product_service.fetch_all_products()
    context.log.info(f'Fetched {len(all_products)} products from product-service')

    rows: list[RawArticuloRow] = [_map_articulo(p) for p in all_products]

    inserted = warehouse.truncate_and_insert('raw.raw_articulos', COLUMNS, rows)

    context.log.info(f'Loaded {inserted} articulos into raw.raw_articulos')

    return MaterializeResult(
        metadata={
            'rows_written':    MetadataValue.int(inserted),
            'source':          MetadataValue.text('product-service API'),
            'warehouse_table': MetadataValue.text('raw.raw_articulos'),
        }
    )
