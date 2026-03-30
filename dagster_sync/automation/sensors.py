from dagster import (
    AssetKey,
    AssetMaterialization,
    DefaultSensorStatus,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    asset_sensor,
)


def make_raw_sales_dbt_sensor(dbt_job):
    """
    Dispara dbt_job cuando raw_sales (marker) se materializa.

    raw_sales es el centinela particionado por mes. Cuando los 4 loaders
    (DIMDS, DIMPPAL, DISDS, DISPPAL) terminan para una partición, raw_sales
    se materializa. Este sensor detecta ese evento y lanza dbt_job con el
    mismo partition_key, de modo que dbt procesa exactamente ese mes.

    Flujo:
        raw_sales [2026-03] materializado
            → sensor detecta partition_key='2026-03-01'
            → lanza dbt_job con partition_key='2026-03-01'
            → dbt build --vars '{"min_month":"2026-03","max_month":"2026-03"}'
            → fct_sales: borra e inserta solo 2026-03
    """
    @asset_sensor(
        asset_key=AssetKey('raw_sales'),
        job=dbt_job,
        name='raw_sales_dbt_sensor',
        description=(
            'Lanza dbt_job con el mismo partition_key cuando raw_sales se materializa. '
            'Garantiza que fct_sales se actualiza exactamente para el mes que cambió.'
        ),
        default_status=DefaultSensorStatus.RUNNING,
    )
    def raw_sales_dbt_sensor(context: SensorEvaluationContext, asset_event):
        materialization: AssetMaterialization = (
            asset_event.dagster_event.event_specific_data.materialization
        )

        # El partition_key de raw_sales es el inicio del mes (ej: '2026-03-01').
        # Lo pasamos directamente a dbt_job para que procese el mismo mes.
        partition_key = materialization.partition

        if partition_key is None:
            yield SkipReason('raw_sales materializado sin partition_key — ignorado')
            return

        yield RunRequest(
            run_key=f'dbt-{partition_key}-{context.cursor}',
            partition_key=partition_key,
        )

    return raw_sales_dbt_sensor
