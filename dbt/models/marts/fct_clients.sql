-- Mart: fct_clients — Client Master Table
--
-- Single source of truth for client data.
-- Replaces:
--   - api-vendedores direct MySQL queries (getByVendor, getExcelClients, getPlanClients)
--   - api-quantix Redis cache bulk read (clients:raw)
--
-- All lookups are pre-applied:
--   - Seller name resolved (no JOIN needed at API query time)
--   - Primary/secondary account mapping (es_excel + primary_account_*)
--   - Business flags passthrough: is_gm, is_ag, is_agro, is_plan_gomeria
--   - Subscriber flag extracted from campos_dinamicos JSONB (codigo=59, valor='2')
--   - Discount fields flattened: bonus_discount, general_discount (default 0)
--
-- Materialization: full table rebuild on every dimensions pipeline run.
-- No partitioning — clients are a slowly-changing dimension (~5k-10k rows).
--
-- Common query patterns:
--   WHERE is_active = true AND vendor_code = 'V01'
--   WHERE is_secondary_account = true AND primary_account_particular = '01234'
--   WHERE is_secondary_account = false  (api-quantix: all primary clients)
--   WHERE is_subscriber = true AND vendor_code = 'V01'

{{
    config(
        materialized = 'table',
        post_hook    = [
            "CREATE INDEX IF NOT EXISTS idx_fct_clients_particular        ON {{ this }} (particular_code)",
            "CREATE INDEX IF NOT EXISTS idx_fct_clients_vendor             ON {{ this }} (vendor_code)",
            "CREATE INDEX IF NOT EXISTS idx_fct_clients_primary_acct       ON {{ this }} (primary_account_particular)",
            "CREATE INDEX IF NOT EXISTS idx_fct_clients_secondary          ON {{ this }} (is_secondary_account)",
            "CREATE INDEX IF NOT EXISTS idx_fct_clients_active_vendor      ON {{ this }} (is_active, vendor_code)"
        ]
    )
}}

select distinct on (client_code)
    client_code,
    particular_code,
    business_name,
    trade_name,
    vendor_code,
    vendor_name,
    zone_code,
    neighborhood,
    city,
    address,
    phone,
    latitude,
    longitude,
    is_secondary_account,
    primary_account_code,
    primary_account_particular,
    true                    as is_active,
    is_gm,
    is_ag,
    is_agro,
    is_subscriber,
    is_plan_gomeria,
    bonus_discount,
    general_discount,
    cash_on_delivery,
    cash_on_deposit

from (

    select
        codigo_cliente                      as client_code,
        codigo_particular                   as particular_code,
        razon_social                        as business_name,
        nombre_fantasia                     as trade_name,
        codigo_vendedor                     as vendor_code,
        nombre_vendedor                     as vendor_name,
        codigo_zona                         as zone_code,
        barrio                              as neighborhood,
        localidad                           as city,
        domicilio                           as address,
        telefono                            as phone,
        latitude,
        longitude,
        es_excel                            as is_secondary_account,
        cuenta_principal_codigo             as primary_account_code,
        cuenta_principal_particular         as primary_account_particular,
        es_gm                               as is_gm,
        es_ag                               as is_ag,
        es_agro                             as is_agro,
        es_suscriptor                       as is_subscriber,
        es_plan_gomeria                     as is_plan_gomeria,
        bonificacion                        as bonus_discount,
        descuento_general                   as general_discount,
        contrareembolso::boolean            as cash_on_delivery,
        contradeposito::boolean             as cash_on_deposit

    from {{ ref('int_clients_enriched') }}

) renamed

order by client_code
