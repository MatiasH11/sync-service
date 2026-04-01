-- Intermediate: int_clients_enriched
--
-- Purpose: Enrich staging clients with seller name resolution and
--          JSONB field extraction (discounts, subscriber flag).
--          This is the single preparation step before the fct_clients mart.
--
-- Transformations applied:
--   1. LEFT JOIN with stg_sellers to resolve nombre_vendedor
--   2. Extract bonificacion from discounts->>'byBonus' (default 0)
--   3. Extract descuento_general from discounts->>'byGeneral' (default 0)
--   4. Extract es_suscriptor from campos_dinamicos array
--      (element where codigo=59 and valor='2')
--   5. All other stg_clients fields passed through as-is

{{ config(materialized='view') }}

with clientes as (

    select * from {{ ref('stg_clients') }}

),

vendedores as (

    select
        codigo_vendedor,
        nombre_vendedor
    from {{ ref('stg_sellers') }}

),

enriched as (

    select
        -- Identifiers
        c.codigo_cliente,
        c.codigo_particular,
        c.cuenta_principal_codigo,
        c.cuenta_principal_particular,

        -- Commercial data
        c.razon_social,
        c.nombre_fantasia,

        -- Seller (resolved via LEFT JOIN — null if no seller assigned)
        c.codigo_vendedor,
        v.nombre_vendedor,

        -- Geography
        c.codigo_zona,
        c.barrio,
        c.localidad,
        c.domicilio,
        c.telefono,
        c.latitude,
        c.longitude,

        -- Account type (passthrough from client-service)
        c.es_excel,

        -- Business flags (pre-computed by client-service, passed through as-is)
        c.es_gm,
        c.es_ag,
        c.es_agro,
        c.es_plan_gomeria,

        -- Subscriber flag: NOT a direct API field — extracted from campos_dinamicos JSONB.
        -- True when the array contains an element with codigo=59 and valor='2'.
        coalesce(
            (
                select true
                from jsonb_array_elements(
                    case jsonb_typeof(c.campos_dinamicos)
                        when 'array' then c.campos_dinamicos
                        else '[]'::jsonb
                    end
                ) as elem
                where (elem->>'codigo')::int = 59
                  and elem->>'valor' = '2'
                limit 1
            ),
            false
        ) as es_suscriptor,

        -- Logistics (passthrough from client-service)
        c.contrareembolso,
        c.contradeposito,

        -- Discounts: extracted from discounts JSONB, defaulting to 0 if missing or null
        coalesce(
            nullif(c.discounts->>'byBonus', '')::numeric(6,2),
            0
        ) as bonificacion,

        coalesce(
            nullif(c.discounts->>'byGeneral', '')::numeric(6,2),
            0
        ) as descuento_general

    from clientes c
    left join vendedores v
        on c.codigo_vendedor = v.codigo_vendedor

)

select * from enriched
