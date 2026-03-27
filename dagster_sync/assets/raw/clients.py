import io
import csv

from dagster import asset, AssetExecutionContext, RetryPolicy

from dagster_sync.resources import ClientServiceResource, WarehouseResource
from dagster_sync.types import RawClientRow, ClientApiResponse

COLUMNS = list(RawClientRow.__annotations__.keys())


def _map_client(client: ClientApiResponse) -> RawClientRow:
    attrs = client.get('attributes', {})
    cuenta_principal = attrs.get('cuentaPrincipal') or {}
    coords = attrs.get('coordinates') or {}

    return RawClientRow(
        codigocliente=attrs.get('cdCliente', ''),
        codigoparticular=attrs.get('codigoParticular', ''),
        cuenta_principal_codigo=cuenta_principal.get('codigoCliente', ''),
        cuenta_principal_particular=cuenta_principal.get('codigoParticular', ''),
        razonsocial=attrs.get('razonSocial', ''),
        nombre_fantasia=attrs.get('nombreFantasia', ''),
        vendedor=attrs.get('vendedor', '') or '',
        zona=attrs.get('zona', ''),
        barrio=attrs.get('barrio', ''),
        localidad=attrs.get('localidad', ''),
        domicilio=attrs.get('domicilio', ''),
        telefono=attrs.get('telefono', ''),
        comentario=attrs.get('comentario', ''),
        gm=attrs.get('gm', False),
        ag=attrs.get('ag', False),
        es_excel=attrs.get('esExcel', False),
        es_agro=attrs.get('esAgro', False),
        es_plan_gomeria=attrs.get('esPlanGomeria', False),
        contrareembolso=attrs.get('contrareembolso', 0),
        contradeposito=attrs.get('contradeposito', 0),
        latitude=coords.get('latitude'),
        longitude=coords.get('longitude'),
        conditions=attrs.get('conditions') or {},
        discounts=attrs.get('discounts') or {},
        campos_dinamicos=attrs.get('camposDinamicos') or [],
        sucursales=attrs.get('sucursales') or [],
    )


@asset(
    group_name='raw',
    retry_policy=RetryPolicy(max_retries=3, delay=60),
    description='Extrae clientes de client-service → raw.raw_clients',
)
def raw_clients(
    context: AssetExecutionContext,
    client_service: ClientServiceResource,
    warehouse: WarehouseResource,
) -> None:
    all_clients = client_service.fetch_all_clients()
    context.log.info(f'Fetched {len(all_clients)} clients from client-service')

    rows: list[RawClientRow] = [_map_client(c) for c in all_clients]

    inserted = warehouse.truncate_and_insert('raw.raw_clients', COLUMNS, rows)

    context.add_output_metadata({'total_clientes': inserted})
    context.log.info(f'Loaded {inserted} clients into raw.raw_clients')
