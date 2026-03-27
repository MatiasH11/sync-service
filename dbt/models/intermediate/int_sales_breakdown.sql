-- Intermediate: Breakdowns pre-calculados por tipo de venta
--
-- Propósito: Calcular los breakdowns GM / DS / PPAL y resolver la sucursal.
--            Los breakdowns son mutuamente excluyentes — cada venta cae en
--            exactamente uno de los tres grupos.
--
-- Grupos:
--   GM   → es_venta_gm = true
--   DS   → es_venta_gm = false AND tipo_consumo = 'DS'
--   PPAL → es_venta_gm = false AND tipo_consumo = 'PPAL'
--
-- Sucursales (mapeadas desde nro_punto_venta y codigo_deposito):
--   BA      → PDV 19, 1  |  PDV 8888 depósito 001
--   MDP     → PDV 18, 7  |  PDV 2222 depósito 003
--   PICO    → PDV 17, 6  |  PDV 2222 depósito 001
--   ROSARIO → PDV 109, 8 |  PDV 8888 depósito 002

{{ config(materialized='view') }}

with ventas as (

    select * from {{ ref('int_sales_calculated') }}

)

select
    *,

    -- Sucursal
    case
        when nro_punto_venta in (19, 1)                                     then 'BA'
        when nro_punto_venta = 8888 and codigo_deposito_articulo = '001'    then 'BA'
        when nro_punto_venta in (18, 7)                                     then 'MDP'
        when nro_punto_venta = 2222 and codigo_deposito_articulo = '003'    then 'MDP'
        when nro_punto_venta in (17, 6)                                     then 'PICO'
        when nro_punto_venta = 2222 and codigo_deposito_articulo = '001'    then 'PICO'
        when nro_punto_venta in (109, 8)                                    then 'ROSARIO'
        when nro_punto_venta = 8888 and codigo_deposito_articulo = '002'    then 'ROSARIO'
        else 'UNKNOWN'
    end as sucursal,

    -- Breakdowns GM
    case when es_venta_gm then cantidad_articulo else 0 end as gm_cantidad,
    case when es_venta_gm then precio_final      else 0 end as gm_monto,

    -- Breakdowns DS (no GM)
    case when not es_venta_gm and tipo_consumo = 'DS' then cantidad_articulo else 0 end as ds_cantidad,
    case when not es_venta_gm and tipo_consumo = 'DS' then precio_final      else 0 end as ds_monto,

    -- Breakdowns PPAL (no GM)
    case when not es_venta_gm and tipo_consumo = 'PPAL' then cantidad_articulo else 0 end as ppal_cantidad,
    case when not es_venta_gm and tipo_consumo = 'PPAL' then precio_final      else 0 end as ppal_monto

from ventas
