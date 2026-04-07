-- Intermediate: Precio de proveedor vigente para ventas GM
--
-- Propósito: Resolver el precio de lista del proveedor que estaba vigente
--            en la fecha de cada venta, para el cálculo GM en la capa de reporting.
--
-- Lógica (réplica de PriceHistoryRepository.getPriceAtTime):
--   CAMBIOSDEPRECIOPROVEEDOR registra cada cambio de precio. El campo `precio_cambio`
--   es el precio que ESTABA VIGENTE antes de esa modificación.
--
--   Se transforma el log de cambios en rangos de vigencia cerrados:
--     Cambio en fecha D con precio_cambio P → P vigente desde el cambio anterior hasta D.
--     El precio_actual_proveedor → vigente desde el último cambio hasta +∞.
--
--   Después se hace un simple range JOIN (hash/merge) en vez de LATERAL (nested loop).
--   Solo se aplica a ventas GM (es_venta_gm = true) para evitar trabajo innecesario.
--
-- Campos agregados:
--   gm_precio_proveedor — precio unitario de lista del proveedor a la fecha de la venta.
--                         NULL si no es venta GM o si el artículo no tiene historial.
--
-- Lineage: int_sales_gm → int_sales_gm_price → int_sales_enriched

{{ config(materialized='view') }}

with ventas as (

    select * from {{ ref('int_sales_gm') }}

),

price_changes as (

    select
        codigo_articulo,
        fecha_modificacion,
        precio_cambio,
        precio_actual_proveedor,
        lag(fecha_modificacion) over (
            partition by codigo_articulo order by fecha_modificacion asc
        ) as prev_fecha,
        row_number() over (
            partition by codigo_articulo order by fecha_modificacion desc
        ) as rn_last
    from {{ ref('stg_price_history') }}

),

price_ranges as (

    -- Rangos históricos: precio_cambio vigente desde el cambio anterior hasta este cambio
    select
        codigo_articulo,
        coalesce(prev_fecha, '1900-01-01'::timestamp) as vigente_desde,
        fecha_modificacion                             as vigente_hasta,
        precio_cambio                                  as precio_proveedor
    from price_changes

    union all

    -- Rango actual: precio_actual_proveedor vigente desde el último cambio en adelante
    select
        codigo_articulo,
        fecha_modificacion     as vigente_desde,
        '9999-12-31'::timestamp as vigente_hasta,
        precio_actual_proveedor as precio_proveedor
    from price_changes
    where rn_last = 1

)

select
    v.*,
    pr.precio_proveedor as gm_precio_proveedor

from ventas v
left join price_ranges pr
    on v.es_venta_gm
   and pr.codigo_articulo = v.codigo_articulo
   and v.fecha_comprobante >= pr.vigente_desde
   and v.fecha_comprobante <  pr.vigente_hasta
