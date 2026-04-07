-- Intermediate: Campos de Pronto Pago (PP)
--
-- Propósito: Agregar los tres campos PP a cada línea de venta, listos para
--            ser expuestos directamente en fct_sales.
--
-- Campos calculados:
--
--   pp_descuento_pct — % de descuento PP del cliente para el mes de la venta.
--     Fuente: stg_pp_monthly (PROMEDIO_PRONTOPAGO_V2), keyed por año/mes.
--     Regla: cliente particular '01129' (Estado) siempre recibe 0%.
--     Fallback: 22% si no hay registro para ese mes (mismo default que el proceso legacy).
--
--   pp_precio — precio de venta neto después de bonificación y descuento PP del cliente.
--     Calculado: precio_total_articulo × ((100 - descuento_comprobante) / 100)
--                                       × ((100 - pp_descuento_pct) / 100)
--     Para el cliente '01129': igual al neto sin PP (0% de descuento PP).
--
--   pp_costo_proveedor — costo neto del proveedor bajo pronto pago.
--     Fuente: stg_pp_provider (DESC_PP_PROV), keyed por codigo_articulo.
--     Calculado: costo_venta_articulo × ((100 - descuento_pp_articulo) / 100)
--     Fallback: 0% si el artículo no tiene descuento PP configurado (COALESCE).
--
-- También enriquece con provider_code/provider_name desde DESC_PP_PROV.
--
-- LEFT JOINs intencionales: ninguna venta se pierde si falta registro PP.

{{ config(materialized='view') }}

with ventas as (

    select * from {{ ref('int_sales_enriched') }}

),

pp_mensual as (

    select
        pp_year,
        pp_month,
        pp_discount_pct
    from {{ ref('stg_pp_monthly') }}

),

pp_proveedor as (

    select
        article_code,
        provider_code,
        provider_name,
        pp_discount_pct as article_pp_discount_pct
    from {{ ref('stg_pp_provider') }}

)

select
    v.*,

    pp.provider_code,
    pp.provider_name,

    case
        when v.codigo_particular_cliente = '01129' then 0
        else coalesce(pm.pp_discount_pct, 22)
    end as pp_descuento_pct,

    case
        when v.codigo_particular_cliente = '01129'
        then v.precio_total_articulo * ((100 - v.descuento_comprobante) / 100)
        else v.precio_total_articulo * ((100 - v.descuento_comprobante) / 100)
             * ((100 - coalesce(pm.pp_discount_pct, 22)) / 100)
    end as pp_precio,

    coalesce(pp.article_pp_discount_pct, 0) as pp_descuento_proveedor_pct,

    v.costo_venta_articulo
        * ((100 - coalesce(pp.article_pp_discount_pct, 0)) / 100) as pp_costo_proveedor

from ventas v
left join pp_mensual pm
    on extract(year  from v.fecha_comprobante)::int = pm.pp_year
    and extract(month from v.fecha_comprobante)::int = pm.pp_month
left join pp_proveedor pp
    on v.codigo_articulo = pp.article_code
