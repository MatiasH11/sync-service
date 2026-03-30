import json
from pathlib import Path

from dagster import AssetExecutionContext, AssetKey, Config, MonthlyPartitionsDefinition
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / 'dbt'

# Shared partition definition — must match raw_sales assets so Dagster can
# resolve upstream (raw_sales [2026-03]) → downstream (fct_sales [2026-03]).
DBT_PARTITIONS = MonthlyPartitionsDefinition(start_date='2023-01-01', end_offset=1)

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR,
)
dbt_project.prepare_if_dev()

_DBT_SOURCE_TO_DAGSTER_KEY: dict[str, str] = {
    'raw_articulos':     'raw_articulos',
    'raw_clients':       'raw_clients',
    'raw_sellers':       'raw_sellers',
    'raw_rubros':        'raw_rubros',
    'raw_marcas_lineas': 'raw_marcas_lineas',
    'raw_price_history': 'raw_price_history',
    'raw_pp_monthly':    'raw_pp_monthly',
    'raw_pp_provider':   'raw_pp_provider',
    'raw_sales':         'raw_sales',
}


class SyncDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props: dict) -> AssetKey:
        if dbt_resource_props['resource_type'] == 'source':
            name = dbt_resource_props['name']
            if name in _DBT_SOURCE_TO_DAGSTER_KEY:
                return AssetKey(_DBT_SOURCE_TO_DAGSTER_KEY[name])
        return super().get_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props: dict) -> str:
        fqn = dbt_resource_props.get('fqn', [])
        if len(fqn) >= 2:
            return fqn[-2]  # 'staging' | 'intermediate' | 'marts'
        return 'dbt'


# ---------------------------------------------------------------------------
# Dimension assets — NOT partitioned
# ---------------------------------------------------------------------------
# These models represent "always current" master data: clients, articles,
# sellers, rubros, brands and price history. They have no temporal partition
# concept — a full rebuild on every run is correct and cheap.

@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=SyncDbtTranslator(),
    select=(
        'stg_clients '
        'stg_sellers '
        'stg_articulos '
        'stg_rubros '
        'stg_marcas_lineas '
        'stg_price_history '
        'stg_pp_monthly '
        'stg_pp_provider'
    ),
    name='sync_dbt_dimension_assets',
)
def sync_dbt_dimension_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(['build'], context=context).stream()


# ---------------------------------------------------------------------------
# Sales pipeline assets — partitioned by month
# ---------------------------------------------------------------------------
# stg_sales → int_sales_* chain → fct_sales.
# Dagster passes min_month/max_month vars so dbt processes exactly the
# partition month in the fct_sales incremental model (delete+insert).

class DbtRunConfig(Config):
    """
    Configuration for dbt sales pipeline runs.

    full_refresh: pass --full-refresh to dbt build.
    Use when SQL logic changes and fct_sales must be rebuilt from scratch.
    Normal sensor-triggered runs always use full_refresh=False (incremental).
    """
    full_refresh: bool = False


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=SyncDbtTranslator(),
    select=(
        'stg_sales '
        'int_sales_accounts '
        'int_sales_gm '
        'int_sales_enriched '
        'int_sales_prices '
        'int_sales_calculated '
        'int_sales_breakdown '
        'int_sales_pp '
        'fct_sales'
    ),
    partitions_def=DBT_PARTITIONS,
    name='sync_dbt_sales_assets',
)
def sync_dbt_sales_assets(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
    config: DbtRunConfig,
):
    start, _ = context.partition_time_window
    month = start.strftime('%Y-%m')

    dbt_vars = {'min_month': month, 'max_month': month}
    vars_arg = json.dumps(dbt_vars)

    if config.full_refresh:
        # Full rebuild: run all models + all tests (used when SQL logic changes).
        # Intermediate view tests make sense here because we're validating the
        # entire historical dataset from scratch.
        yield from dbt.cli(
            ['build', '--vars', vars_arg, '--full-refresh'],
            context=context,
        ).stream()
    else:
        # Incremental production run: only build models, no tests.
        #
        # Why no tests:
        #   - dagster-dbt always appends the full decorator --select to any
        #     cli() call that receives context=context, making it impossible
        #     to scope dbt test to fct_sales only from within this asset.
        #   - Intermediate models are views: testing them scans 3 years of
        #     raw data on every incremental run (136s+ per test), with no
        #     real value since fct_sales already filters to the partition month.
        #
        # Quality is guaranteed by:
        #   - unique_key + delete+insert  → idempotency, no duplicates
        #   - on_schema_change='fail'     → prevents silent schema drift
        #   - dbt run exit code           → Dagster marks run failed if SQL errors
        #   - dbt_sales_full_refresh job  → runs dbt build (full tests) on demand
        yield from dbt.cli(
            ['run', '--vars', vars_arg],
            context=context,
        ).stream()
