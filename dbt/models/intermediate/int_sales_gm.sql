-- Intermediate: Detección de ventas GM y enriquecimiento con vendedor
--
-- Propósito: Identificar si una venta es GM (Gran Minorista) y extraer
--            el porcentaje de descuento del campo barrio del cliente.
--
-- Regla de negocio (replicada de GMDiscountCalculator.ts):
--   Una venta es GM si cumple las 3 condiciones simultáneamente:
--   1. tipo_comprobante = 'RE' (Remito)
--   2. nro_punto_venta IN (17, 18, 19, 109)
--   3. El barrio del cliente contiene un descuento en formato "(XX.XX) descripcion"
--
--   Ejemplo: barrio = "(35.90) M-1614" → gm_descuento_pct = 35.90
--
-- INNER JOIN con clientes: filtra ventas cuya cuenta resuelta no existe
-- en el maestro de clientes (equivalente a SalesProcessor.ts líneas 141-142).

{{ config(materialized='view') }}

with ventas as (

    select * from {{ ref('int_sales_accounts') }}

),

clientes as (

    select
        codigo_cliente,
        razon_social,
        barrio,
        codigo_vendedor,
        codigo_zona,
        codigo_particular,
        es_gm
    from {{ ref('stg_clients') }}

),

vendedores as (

    select
        codigo_vendedor,
        nombre_vendedor
    from {{ ref('stg_sellers') }}

),

enriquecido as (

    select
        v.*,

        -- Datos de la cuenta principal (no de la cuenta secundaria)
        c.razon_social             as razon_social_cuenta,
        c.barrio                   as barrio_cliente,
        c.codigo_vendedor          as codigo_vendedor_cliente,
        c.codigo_zona              as codigo_zona_cliente,
        c.codigo_particular        as codigo_particular_cliente,
        c.es_gm                    as cliente_es_gm,

        -- Nombre del vendedor asignado a la cuenta
        vend.nombre_vendedor,

        -- Extracción del % de descuento GM desde el campo barrio
        -- Formato esperado: "(XX.XX) descripcion" → extrae XX.XX
        cast(
            (regexp_match(c.barrio, '^\s*\(([\d.]+)\)'))[1]
        as numeric) as gm_descuento_pct,

        -- Detección de venta GM: requiere remito + PDV GM + descuento en barrio
        case
            when v.tipo_comprobante = 'RE'
             and v.nro_punto_venta in (17, 18, 19, 109)
             and (regexp_match(c.barrio, '^\s*\(([\d.]+)\)'))[1] is not null
            then true
            else false
        end as es_venta_gm

    from ventas v
    -- INNER JOIN: descarta ventas de cuentas que no existen en el maestro
    inner join clientes c
        on v.codigo_cuenta_resuelta = c.codigo_cliente
    left join vendedores vend
        on c.codigo_vendedor = vend.codigo_vendedor

)

select * from enriquecido
