from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    build_schedule_from_partitioned_job,
)

from dagster_sync.assets import (
    raw_clients,
    raw_sellers,
    raw_price_history,
    raw_sales_dimds,
    raw_sales_dimppal,
    raw_sales_disds,
    raw_sales_disppal,
)
from dagster_sync.resources import (
    SalesDbResource,
    PricesDbResource,
    WarehouseResource,
    ClientServiceResource,
)

ALL_ASSETS = [
    raw_clients,
    raw_sellers,
    raw_price_history,
    raw_sales_dimds,
    raw_sales_dimppal,
    raw_sales_disds,
    raw_sales_disppal,
]

ALL_RAW_SALES = [raw_sales_dimds, raw_sales_dimppal, raw_sales_disds, raw_sales_disppal]


# --- Jobs ---

full_sync_job = define_asset_job(
    name='full_sync',
    selection=[raw_clients, raw_sellers],
    description='Sync completo: clientes y vendedores',
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
        sales_sync,
        price_history_sync,
    ],
    resources={
        'sales_db': SalesDbResource(),
        'prices_db': PricesDbResource(),
        'warehouse': WarehouseResource(),
        'client_service': ClientServiceResource(),
    },
)
