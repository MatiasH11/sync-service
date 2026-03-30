from dagster import Definitions, RunConfig, define_asset_job
from dagster_dbt import DbtCliResource

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
from dagster_sync.assets.dbt import (
    DBT_PROJECT_DIR,
    DBT_PARTITIONS,
    DbtRunConfig,
    sync_dbt_dimension_assets,
    sync_dbt_sales_assets,
)
from dagster_sync.resources import (
    ClientServiceResource,
    DistriRdsDbResource,
    PricesDbResource,
    ProductServiceResource,
    WarehouseResource,
)
from dagster_sync.automation.schedules import (
    make_daily_dimensions_schedule,
    make_hourly_sales_schedule,
    make_price_history_schedule,
)
from dagster_sync.automation.sensors import make_raw_sales_dbt_sensor

# ---------------------------------------------------------------------------
# Jobs — raw ingestion (MySQL / Firebird → raw.*)
# ---------------------------------------------------------------------------

sales_job = define_asset_job(
    name='sales_sync',
    selection=[raw_sales_dimds, raw_sales_dimppal, raw_sales_disds, raw_sales_disppal, raw_sales],
    description=(
        '[INGESTION] MySQL → raw.raw_sales\n'
        'Extrae comprobantes y líneas de venta de las 4 fuentes MySQL '
        '(DIMDS, DIMPPAL, DISDS, DISPPAL) y los escribe en raw.raw_sales.\n'
        'Particionado por mes — cada run procesa exactamente 1 mes.\n'
        '\n'
        'AUTOMÁTICO: hourly_sales_schedule lo lanza cada hora para el mes actual y el anterior.\n'
        'MANUAL: lanzar con una partición específica para recargar un mes puntual.\n'
        'NO HACE: transformaciones dbt ni actualiza fct_sales '
        '(eso lo dispara el sensor raw_sales_dbt_sensor).'
    ),
)

dimensions_job = define_asset_job(
    name='dimensions_sync',
    selection=[raw_clients, raw_sellers, raw_articulos, raw_rubros, raw_marcas_lineas],
    description=(
        '[INGESTION] MySQL / APIs → raw.raw_clients, raw_sellers, raw_articulos, raw_rubros, raw_marcas_lineas\n'
        'Actualiza las tablas maestras de clientes, vendedores, artículos, rubros y marcas/líneas.\n'
        'Sin particionado — siempre reemplaza el estado actual completo.\n'
        '\n'
        'AUTOMÁTICO: daily_dimensions_schedule lo lanza una vez por día.\n'
        'MANUAL: lanzar cuando se necesite forzar una actualización inmediata del maestro.\n'
        'NO HACE: actualizar ventas ni precios históricos '
        '(usar sales_sync o price_history_sync para eso).'
    ),
)

price_history_job = define_asset_job(
    name='price_history_sync',
    selection=[raw_price_history],
    description=(
        '[INGESTION] Firebird → raw.raw_price_history\n'
        'Extrae el historial de precios proveedor desde la base Firebird legacy.\n'
        'Sin particionado — carga el historial completo disponible.\n'
        '\n'
        'AUTOMÁTICO: price_history_schedule lo lanza 2 veces por día.\n'
        'MANUAL: lanzar si se detectan precios desactualizados en fct_sales.\n'
        'NO HACE: recalcular fct_sales — después de correr este job, '
        'lanzar dbt_sales_full_refresh para reprocesar los precios históricos.'
    ),
)

# ---------------------------------------------------------------------------
# Jobs — dbt transformations (raw.* → staging / intermediate / analytics)
# ---------------------------------------------------------------------------

dbt_sales_job = define_asset_job(
    name='dbt_sales_build',
    selection=[sync_dbt_sales_assets],
    partitions_def=DBT_PARTITIONS,
    description=(
        '[TRANSFORM] raw.raw_sales → staging.stg_sales → intermediate.int_sales_* → analytics.fct_sales\n'
        'Ejecuta dbt run incremental para un mes específico: limpia, enriquece y carga '
        'los datos de ventas en fct_sales usando delete+insert sobre el mes de la partición.\n'
        'Particionado por mes — solo procesa y reescribe el mes seleccionado.\n'
        '\n'
        'AUTOMÁTICO: raw_sales_dbt_sensor lo lanza cada vez que raw_sales se materializa.\n'
        'MANUAL: lanzar con una o varias particiones para backfill o reprocesar un mes.\n'
        'NO HACE: extraer datos de fuentes — raw.raw_sales debe estar actualizado primero '
        '(correr sales_sync antes si los datos de origen cambiaron).\n'
        'NO HACE: tests de calidad en intermediate (usar dbt_sales_full_refresh para eso).'
    ),
)

dbt_sales_full_refresh_job = define_asset_job(
    name='dbt_sales_full_refresh',
    selection=[sync_dbt_sales_assets],
    config=RunConfig(ops={'sync_dbt_sales_assets': DbtRunConfig(full_refresh=True)}),
    description=(
        '[TRANSFORM — FULL REBUILD] raw.raw_sales → fct_sales (reconstrucción completa)\n'
        'Ejecuta dbt build --full-refresh: reconstruye fct_sales desde cero con todos los '
        'meses históricos y corre el suite completo de tests (staging + intermediate + marts).\n'
        'Sin particionado — procesa todo el histórico disponible en raw.raw_sales.\n'
        '\n'
        'MANUAL únicamente. Casos de uso:\n'
        '  - Se modificó la lógica SQL de algún modelo (stg_sales, int_sales_*, fct_sales)\n'
        '  - Se actualizó raw.raw_price_history y se necesita recalcular precios históricos\n'
        '  - Se detectó corrupción de datos en fct_sales\n'
        'ATENCIÓN: operación costosa (~30 min para 3 años). No usar en producción rutinariamente.'
    ),
)

dbt_dimensions_job = define_asset_job(
    name='dbt_dimensions_build',
    selection=[sync_dbt_dimension_assets],
    description=(
        '[TRANSFORM] raw.raw_* → staging.stg_clients / stg_sellers / stg_articulos / stg_rubros / '
        'stg_marcas_lineas / stg_price_history\n'
        'Reconstruye las vistas de staging para todas las dimensiones maestras.\n'
        'Sin particionado — siempre refleja el estado actual de las tablas raw.\n'
        '\n'
        'MANUAL o integrable en daily_dimensions_schedule.\n'
        'PREREQUISITO: debe correrse al menos una vez antes de dbt_sales_build, '
        'ya que int_sales_* depende de estas vistas (stg_clients, stg_articulos, etc.).\n'
        'NO HACE: actualizar raw.* — correr dimensions_sync antes si el maestro cambió.'
    ),
)

# ---------------------------------------------------------------------------
# Schedules y Sensors
# ---------------------------------------------------------------------------

hourly_sales_schedule     = make_hourly_sales_schedule(sales_job)
daily_dimensions_schedule = make_daily_dimensions_schedule(dimensions_job)
price_history_schedule    = make_price_history_schedule(price_history_job)
raw_sales_dbt_sensor      = make_raw_sales_dbt_sensor(dbt_sales_job)

# ---------------------------------------------------------------------------
# Definitions
# ---------------------------------------------------------------------------

defs = Definitions(
    assets=[
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
        sync_dbt_dimension_assets,
        sync_dbt_sales_assets,
    ],
    jobs=[
        sales_job,
        dimensions_job,
        price_history_job,
        dbt_sales_job,
        dbt_sales_full_refresh_job,
        dbt_dimensions_job,
    ],
    schedules=[
        hourly_sales_schedule,
        daily_dimensions_schedule,
        price_history_schedule,
    ],
    sensors=[
        raw_sales_dbt_sensor,
    ],
    resources={
        'distri_rds':      DistriRdsDbResource(),
        'prices_db':       PricesDbResource(),
        'warehouse':       WarehouseResource(),
        'client_service':  ClientServiceResource(),
        'product_service': ProductServiceResource(),
        'dbt': DbtCliResource(
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROJECT_DIR),
        ),
    },
)
