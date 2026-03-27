from pathlib import Path

from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    build_schedule_from_partitioned_job,
)
from dagster import AssetKey
from dagster_dbt import DbtCliResource, DbtProject, DagsterDbtTranslator, dbt_assets

from dagster_sync.assets import (
    raw_clients,
    raw_sellers,
    raw_articulos,
    raw_rubros,
    raw_marcas_lineas,
    raw_price_history,
    raw_sales_dimds,
    raw_sales_dimppal,
    raw_sales_disds,
    raw_sales_disppal,
    raw_sales,
)
from dagster_sync.resources import (
    DistriRdsDbResource,
    PricesDbResource,
    WarehouseResource,
    ClientServiceResource,
    ProductServiceResource,
)

# --- dbt project ---

DBT_PROJECT_DIR = Path(__file__).parent.parent / 'dbt'

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR,
)
dbt_project.prepare_if_dev()

# Las fuentes dbt (sources.yml) necesitan mapearse a los assets Dagster existentes
# para evitar nodos duplicados en el grafo. raw_sales es especial: 4 assets Dagster
# escriben a la misma tabla, así que se deja sin mapeo directo.
_DBT_SOURCE_TO_DAGSTER_KEY: dict[str, AssetKey] = {
    'raw_articulos':     AssetKey('raw_articulos'),
    'raw_clients':       AssetKey('raw_clients'),
    'raw_sellers':       AssetKey('raw_sellers'),
    'raw_rubros':        AssetKey('raw_rubros'),
    'raw_marcas_lineas': AssetKey('raw_marcas_lineas'),
    'raw_price_history': AssetKey('raw_price_history'),
    'raw_sales':         AssetKey('raw_sales'),
}


class SyncDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props: dict) -> AssetKey:
        if dbt_resource_props['resource_type'] == 'source':
            name = dbt_resource_props['name']
            if name in _DBT_SOURCE_TO_DAGSTER_KEY:
                return _DBT_SOURCE_TO_DAGSTER_KEY[name]
        return super().get_asset_key(dbt_resource_props)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=SyncDbtTranslator(),
)
def sync_dbt_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(['build'], context=context).stream()

ALL_ASSETS = [
    raw_clients,
    raw_sellers,
    raw_articulos,
    raw_rubros,
    raw_marcas_lineas,
    raw_price_history,
    raw_sales_dimds,
    raw_sales_dimppal,
    raw_sales_disds,
    raw_sales_disppal,
    raw_sales,
    sync_dbt_assets,
]

ALL_RAW_SALES = [raw_sales_dimds, raw_sales_dimppal, raw_sales_disds, raw_sales_disppal, raw_sales]


# --- Jobs ---

full_sync_job = define_asset_job(
    name='full_sync',
    selection=[raw_clients, raw_sellers, raw_articulos, raw_rubros, raw_marcas_lineas],
    description='Sync completo: clientes, vendedores, artículos y dimensiones',
)

articulos_sync_job = define_asset_job(
    name='articulos_sync',
    selection=[raw_articulos, raw_rubros, raw_marcas_lineas],
    description='Sync de artículos (product-service) y dimensiones MySQL',
)

clients_sync_job = define_asset_job(
    name='clients_sync',
    selection=[raw_clients],
    description='Sync solo de clientes desde client-service',
)

sales_job = define_asset_job(
    name='sales_sync',
    selection=ALL_RAW_SALES,
    description='Sync mensual de ventas desde MySQL (4 fuentes)',
)

price_history_job = define_asset_job(
    name='price_history_sync',
    selection=[raw_price_history],
    description='Sync mensual de historial de precios desde Firebird',
)


# --- Schedules ---

daily_full_sync = ScheduleDefinition(
    job=full_sync_job,
    cron_schedule='0 3 * * *',
    name='daily_full_sync',
    description='Sync completo diario a las 3am',
)

clients_sync = ScheduleDefinition(
    job=clients_sync_job,
    cron_schedule='0 6,18 * * *',
    name='clients_sync',
    description='Sync de clientes dos veces por dia (6am y 6pm)',
)

articulos_sync = ScheduleDefinition(
    job=articulos_sync_job,
    cron_schedule='0 4 * * *',
    name='articulos_sync',
    description='Sync de artículos una vez por dia (4am)',
)

sales_sync = build_schedule_from_partitioned_job(
    sales_job,
    name='daily_sales_sync',
    description='Sync diario de ventas a las 1am (particion del mes actual)',
    hour_of_day=1,
    minute_of_hour=0,
)

price_history_sync = build_schedule_from_partitioned_job(
    price_history_job,
    name='daily_price_history_sync',
    description='Sync diario de precios a las 2am (particion del mes actual)',
    hour_of_day=2,
    minute_of_hour=0,
)


# --- Definitions ---

defs = Definitions(
    assets=ALL_ASSETS,
    schedules=[
        daily_full_sync,
        clients_sync,
        articulos_sync,
        sales_sync,
        price_history_sync,
    ],
    resources={
        'distri_rds': DistriRdsDbResource(),
        'prices_db': PricesDbResource(),
        'warehouse': WarehouseResource(),
        'client_service': ClientServiceResource(),
        'product_service': ProductServiceResource(),
        'dbt': DbtCliResource(
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROJECT_DIR),
        ),
    },
)
