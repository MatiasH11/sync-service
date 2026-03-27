-- Intermediate: Precio proveedor vigente al momento de la venta (ASOF join)
--
-- Propósito: Encontrar el precio proveedor del catálogo que estaba vigente
--            en el momento exacto de cada venta, usando el historial de cambios.
--
-- Lógica replicada de PriceHistoryRepository.getPriceAtTime() (TypeScript):
--   El código TS recorre los precios ordenados DESC por fecha:
--   1. Empieza con precioVigente (precio actual del catálogo)
--   2. Para cada cambio, si fechaModificacion < fechaVenta → para
--   3. Si no, usa ese precio
--   Resultado: usa el PRIMER cambio cuya fecha >= fechaVenta
--
-- NOTA: Esta lógica es técnicamente incorrecta (puede usar precios futuros)
--       pero se replica exactamente para mantener consistencia con producción.
--       La lógica "correcta" usaría fecha_modificacion <= fecha_comprobante.
--
-- Fallback hierarchy:
--   1. Primer cambio con fecha >= fecha_comprobante (lógica TS)
--   2. Precio actual del catálogo (precioVigente) si no hay cambio posterior

{{ config(materialized='view') }}

with ventas as (

    select * from {{ ref('int_sales_enriched') }}

),

historial as (

    select * from {{ ref('stg_price_history') }}

),

-- Precio actual del catálogo por artículo (precioVigente en TS)
precio_actual as (

    select distinct on (codigo_articulo)
        codigo_articulo,
        precio_actual_proveedor
    from historial
    order by codigo_articulo, fecha_modificacion desc

),

-- Primer cambio de precio con fecha >= fecha de la venta (lógica TS)
precio_en_venta as (

    select distinct on (v.fuente, v.numero_comprobante, v.tipo_comprobante, v.codigo_articulo)
        v.fuente,
        v.numero_comprobante,
        v.tipo_comprobante,
        v.codigo_articulo,
        v.fecha_comprobante,
        h.precio_cambio         as precio_matched,
        h.fecha_modificacion    as precio_vigente_desde
    from ventas v
    left join historial h
        on  v.codigo_articulo = h.codigo_articulo
        and h.fecha_modificacion >= v.fecha_comprobante
    order by
        v.fuente,
        v.numero_comprobante,
        v.tipo_comprobante,
        v.codigo_articulo,
        v.fecha_comprobante,
        h.fecha_modificacion asc   -- el más cercano al futuro primero

),

final as (

    select
        v.*,

        -- Precio proveedor al momento de la venta
        coalesce(
            pv.precio_matched,
            pa.precio_actual_proveedor
        )                           as precio_proveedor_vigente,

        pv.precio_vigente_desde,

        -- Flag: se encontró precio histórico o se usó el actual como fallback
        case
            when pv.precio_matched is not null
              or pa.precio_actual_proveedor is not null
            then true
            else false
        end as tiene_precio_historico

    from ventas v
    left join precio_en_venta pv
        on  v.fuente                = pv.fuente
        and v.numero_comprobante    = pv.numero_comprobante
        and v.tipo_comprobante      = pv.tipo_comprobante
        and v.codigo_articulo       = pv.codigo_articulo
    left join precio_actual pa
        on v.codigo_articulo = pa.codigo_articulo

)

select * from final
