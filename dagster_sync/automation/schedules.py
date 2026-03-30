from datetime import timedelta

from dagster import DefaultScheduleStatus, RunRequest, schedule


# ---------------------------------------------------------------------------
# Raw sales — cada hora, últimos 2 meses
# ---------------------------------------------------------------------------
# Se emiten 2 RunRequest por tick: mes actual + mes anterior.
# El mes anterior cubre ajustes retroactivos o ventas tardías.

def _last_two_month_keys(execution_time) -> list[str]:
    current = execution_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    prev = (current - timedelta(days=1)).replace(day=1)
    return [current.strftime('%Y-%m-%d'), prev.strftime('%Y-%m-%d')]


def make_hourly_sales_schedule(sales_job):
    @schedule(
        cron_schedule='0 * * * *',
        job=sales_job,
        name='hourly_sales_schedule',
        description='Carga ventas cada hora para el mes actual y el anterior (4 fuentes MySQL)',
        default_status=DefaultScheduleStatus.RUNNING,
    )
    def hourly_sales_schedule(context):
        hour = context.scheduled_execution_time.hour
        for partition_key in _last_two_month_keys(context.scheduled_execution_time):
            yield RunRequest(
                partition_key=partition_key,
                run_key=f'sales-{partition_key}-h{hour}',
                tags={
                    'trigger':         'scheduled',
                    'schedule':        'hourly_sales',
                    'partition_month': partition_key[:7],
                },
            )

    return hourly_sales_schedule


# ---------------------------------------------------------------------------
# Dimensiones — una vez por día a las 03:00
# Incluye: clientes, vendedores, artículos, rubros, marcas/líneas
# ---------------------------------------------------------------------------

def make_daily_dimensions_schedule(dimensions_job):
    @schedule(
        cron_schedule='0 3 * * *',
        job=dimensions_job,
        name='daily_dimensions_schedule',
        description='Actualiza clientes, vendedores, artículos y dimensiones una vez por día',
        default_status=DefaultScheduleStatus.RUNNING,
    )
    def daily_dimensions_schedule(context):
        yield RunRequest(
            run_key=context.scheduled_execution_time.strftime('%Y-%m-%d'),
            tags={
                'trigger':  'scheduled',
                'schedule': 'daily_dimensions',
            },
        )

    return daily_dimensions_schedule


# ---------------------------------------------------------------------------
# Historial de precios — dos veces por día (4am y 4pm)
# ---------------------------------------------------------------------------

def make_price_history_schedule(price_history_job):
    @schedule(
        cron_schedule='0 4,16 * * *',
        job=price_history_job,
        name='price_history_schedule',
        description='Actualiza historial de precios desde Firebird dos veces por día',
        default_status=DefaultScheduleStatus.RUNNING,
    )
    def price_history_schedule(context):
        yield RunRequest(
            run_key=context.scheduled_execution_time.isoformat(),
            tags={
                'trigger':  'scheduled',
                'schedule': 'price_history',
            },
        )

    return price_history_schedule
