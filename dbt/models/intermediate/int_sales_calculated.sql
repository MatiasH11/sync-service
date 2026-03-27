-- Intermediate: Cálculo del precio final con lógica GM
--
-- Propósito: Calcular precio_final según el escenario de cada venta.
--
-- Regla de negocio (replicada de GMDiscountCalculator.calculateGMPrice()):
--
--   Escenario 1 — Venta GM con descuento válido:
--     precio_final = precio_proveedor_vigente × ((100 - gm_descuento_pct) / 100) × cantidad
--     (el precio proveedor vigente es el precio base sobre el que se aplica el descuento)
--
--   Escenario 2 — Cuenta secundaria:
--     precio_final = 0
--     (la cuenta principal ya contabiliza esta venta, se evita doble conteo)
--
--   Escenario 3 — Venta estándar:
--     precio_final = precio_venta
--     (se usa el precio total del comprobante tal como viene de MySQL)

{{ config(materialized='view') }}

with ventas as (

    select * from {{ ref('int_sales_prices') }}

)

select
    *,

    case
        -- Escenario 1: GM con descuento aplicado
        when es_venta_gm
         and not es_cuenta_secundaria
         and gm_descuento_pct is not null
         and precio_proveedor_vigente is not null
        then precio_proveedor_vigente * ((100 - gm_descuento_pct) / 100) * cantidad_articulo

        -- Escenario 2: cuenta secundaria → 0 para evitar doble conteo
        when es_cuenta_secundaria
        then 0

        -- Escenario 3: venta estándar
        else precio_total_articulo

    end as precio_final,

    case
        when es_venta_gm
         and not es_cuenta_secundaria
         and gm_descuento_pct is not null
        then precio_proveedor_vigente * (gm_descuento_pct / 100) * cantidad_articulo
        else 0
    end as gm_descuento_monto,

    case
        when es_venta_gm
         and not es_cuenta_secundaria
         and gm_descuento_pct is not null
         and precio_proveedor_vigente is not null
        then 'GM_DESCONTADO'
        when es_cuenta_secundaria
        then 'SECUNDARIA_CERO'
        else 'PRECIO_ESTANDAR'
    end as metodo_precio

from ventas
