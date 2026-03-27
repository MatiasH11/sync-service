-- Intermediate: Resolución de cuentas principales
--
-- Propósito: Determinar si cada venta pertenece a una cuenta secundaria
--            y resolver el código de cuenta principal correspondiente.
--
-- Regla de negocio:
--   Si el cliente tiene cuenta_principal_codigo no vacío en stg_clients,
--   es una cuenta secundaria (cuenta Excel). Se usa la cuenta principal
--   para joins y agregaciones para evitar doble conteo.
--
-- Fuente: client-service (campo cuentaPrincipal en la API)

{{ config(materialized='view') }}

with ventas as (

    select * from {{ ref('stg_sales') }}

),

clientes as (

    select
        codigo_cliente,
        cuenta_principal_codigo
    from {{ ref('stg_clients') }}

),

resolucion as (

    select
        v.*,
        c.cuenta_principal_codigo,

        -- Es secundaria si tiene cuenta principal asociada
        case
            when c.cuenta_principal_codigo is not null
             and c.cuenta_principal_codigo != ''
            then true
            else false
        end as es_cuenta_secundaria,

        -- Código de cuenta a usar en todos los joins downstream
        -- Si es secundaria → usa la principal; si no → usa la propia
        coalesce(
            nullif(c.cuenta_principal_codigo, ''),
            v.codigo_cliente
        ) as codigo_cuenta_resuelta

    from ventas v
    left join clientes c
        on v.codigo_cliente = c.codigo_cliente

)

select * from resolucion
