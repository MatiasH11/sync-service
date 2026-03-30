-- Staging: Descuento PP promedio mensual
--
-- Propósito: Exponer el porcentaje de descuento por pronto pago vigente
--            para cada mes calendario. Un solo valor aplica a todos los clientes
--            en ese mes, excepto el cliente particular '01129' que siempre es 0.
--
-- Fuente: raw.raw_pp_monthly ← distriap_distri.PROMEDIO_PRONTOPAGO_V2 (MySQL AWS)
-- Usado en: int_sales_pp para calcular pp_discount_pct y pp_price

{{ config(materialized='view') }}

select
    pp_year,
    pp_month,
    pp_discount_pct
from {{ source('raw', 'raw_pp_monthly') }}
