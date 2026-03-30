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
--   pp_precio — precio de venta después de aplicar el descuento PP del cliente.
--     Calculado: precio_final × ((100 - pp_descuento_pct) / 100)
--     Para el cliente '01129': igual a precio_final (0% de descuento).
--
--   pp_costo_proveedor — costo neto del proveedor bajo pronto pago.
--     Fuente: stg_pp_provider (DESC_PP_PROV), keyed por codigo_marca_int.
--     Calculado: costo_venta_articulo × ((100 - descuento_pp_marca) / 100)
--     Fallback: 0% si la marca no tiene descuento PP configurado (COALESCE).
--
-- LEFT JOINs intencionales: ninguna venta se pierde si falta registro PP.

{{ config(materialized='view') }}

with ventas as (

    select * from {{ ref('int_sales_breakdown') }}

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
        brand_code,
        pp_discount_pct as brand_pp_discount_pct
    from {{ ref('stg_pp_provider') }}

)

select
    v.*,

    -- % de descuento PP aplicable al cliente para el mes de esta venta.
    -- El cliente particular '01129' (Estado) no recibe descuento PP.
    -- Si el mes no tiene registro en PROMEDIO_PRONTOPAGO_V2, se usa 22 como fallback.
    case
        when v.codigo_particular_cliente = '01129' then 0
        else coalesce(pm.pp_discount_pct, 22)
    end as pp_descuento_pct,

    -- Precio de venta después de aplicar el descuento PP del cliente.
    -- Equivalente a TOTAL_PP en RENT_COMERCIAL.
    case
        when v.codigo_particular_cliente = '01129'
        then v.precio_final
        else v.precio_final * ((100 - coalesce(pm.pp_discount_pct, 22)) / 100)
    end as pp_precio,

    -- Costo de venta neto de PP: descuento que el proveedor otorga por pronto pago.
    -- Keyed por marca — marcas sin configuración reciben 0% (sin descuento).
    -- Equivalente a COMPRAS_PP en RENT_COMERCIAL.
    v.costo_venta_articulo
        * ((100 - coalesce(pp.brand_pp_discount_pct, 0)) / 100) as pp_costo_proveedor

from ventas v
left join pp_mensual pm
    on extract(year  from v.fecha_comprobante)::int = pm.pp_year
    and extract(month from v.fecha_comprobante)::int = pm.pp_month
left join pp_proveedor pp
    on v.codigo_marca_int = pp.brand_code
