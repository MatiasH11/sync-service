-- Mart: fct_sales — Sales Fact Table
--
-- Single source of truth for sales analytics.
-- Replaces fct_sales (DuckDB) for api-vendedores
-- and RENT_COMERCIAL (MySQL) for api-quantix.
--
-- Design principles:
--   1. Comprobante is truth — expose what Flexxus recorded, untransformed
--   2. Enrichments are lookups — dimensions from masters to avoid consumer joins
--   3. Only process what the consumer can't — PP requires monthly external tables
--   4. Flags for complex detection — GM and secondary require multi-table logic
--   5. No pre-aggregations — breakdowns belong in the reporting layer
--
-- Column origins:
--   Comprobante  — historical truth from the invoice, never changes
--   Enriched     — joined from current master tables, may change if masters change
--   Calculated   — business logic applied by our pipeline
--
-- Lineage: stg_sales → int_sales_accounts → int_sales_gm → int_sales_enriched → int_sales_pp → fct_sales
--
-- Incremental strategy: delete+insert
--   Dagster passes min_month and max_month vars when running a partition.
--   Full rebuild: dbt build --full-refresh --select fct_sales

{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key           = ['source', 'voucher_type', 'voucher_number', 'voucher_line'],
        on_schema_change     = 'fail',
        post_hook            = [
            "CREATE INDEX IF NOT EXISTS idx_fct_sales_vendor_yearmonth  ON {{ this }} (vendor_code, year_month)",
            "CREATE INDEX IF NOT EXISTS idx_fct_sales_account_yearmonth ON {{ this }} (account_code, year_month)",
            "CREATE INDEX IF NOT EXISTS idx_fct_sales_yearmonth         ON {{ this }} (year_month)",
            "CREATE INDEX IF NOT EXISTS idx_fct_sales_sr_cte            ON {{ this }} (vendor_code, account_code, rubro_code, year_month)",
            "CREATE INDEX IF NOT EXISTS idx_fct_sales_invoice_datetime  ON {{ this }} (invoice_datetime)"
        ]
    )
}}

select

    -- -------------------------------------------------------------------------
    -- Comprobante
    -- -------------------------------------------------------------------------
    source,
    voucher_type,
    voucher_number,
    voucher_line,
    point_of_sale,
    deposit_code,

    -- -------------------------------------------------------------------------
    -- Temporal
    -- -------------------------------------------------------------------------
    invoice_datetime,
    year_month,

    -- -------------------------------------------------------------------------
    -- Classification (calculated)
    -- -------------------------------------------------------------------------
    branch,
    consumption_type,

    -- -------------------------------------------------------------------------
    -- Client
    -- -------------------------------------------------------------------------
    client_code,
    account_code,
    account_name,
    client_particular_code,
    vendor_code,
    vendor_name,
    zone_code,

    -- -------------------------------------------------------------------------
    -- Article
    -- -------------------------------------------------------------------------
    article_code,
    article_particular_code,
    article_description,
    rubro_code,
    rubro_description,
    rubro_min_units,
    brand_code,
    brand_id,
    brand_name,
    product_line_id,
    product_line_name,

    -- -------------------------------------------------------------------------
    -- Provider
    -- -------------------------------------------------------------------------
    provider_code,
    provider_name,

    -- -------------------------------------------------------------------------
    -- Metrics
    -- -------------------------------------------------------------------------
    article_unit_price,
    article_quantity,
    line_discount_pct,
    header_bonification_pct,
    line_total,
    sale_total,
    cost_total,

    -- -------------------------------------------------------------------------
    -- PP (Pronto Pago) — only processed totals in the table
    -- -------------------------------------------------------------------------
    pp_discount_pct,
    pp_sale_total,
    pp_provider_discount_pct,
    pp_cost_total,

    -- -------------------------------------------------------------------------
    -- Flags
    -- -------------------------------------------------------------------------
    is_gm_sale,
    is_secondary_account,
    is_valid_article,
    is_valid_for_units

from (

    select
        fuente                              as source,
        tipo_comprobante                    as voucher_type,
        numero_comprobante                  as voucher_number,
        nro_linea                           as voucher_line,
        nro_punto_venta                     as point_of_sale,
        codigo_deposito_articulo            as deposit_code,

        fecha_comprobante                   as invoice_datetime,
        to_char(fecha_comprobante, 'YYYY-MM') as year_month,

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
        end                                 as branch,
        tipo_consumo                        as consumption_type,

        codigo_cliente                      as client_code,
        codigo_cuenta_resuelta              as account_code,
        razon_social_cuenta                 as account_name,
        codigo_particular_cliente           as client_particular_code,
        codigo_vendedor_cliente             as vendor_code,
        nombre_vendedor                     as vendor_name,
        codigo_zona_cliente                 as zone_code,

        codigo_articulo                     as article_code,
        codigo_particular_articulo_master   as article_particular_code,
        descripcion_articulo_master         as article_description,
        codigo_super_rubro                  as rubro_code,
        descripcion_super_rubro             as rubro_description,
        unidades_min_rubro                  as rubro_min_units,
        codigo_marca_int                    as brand_code,
        marca_id                            as brand_id,
        marca                               as brand_name,
        linea_id                            as product_line_id,
        linea                               as product_line_name,

        provider_code,
        provider_name,

        precio_unitario_articulo            as article_unit_price,
        cantidad_articulo                   as article_quantity,
        descuento_articulo                  as line_discount_pct,
        descuento_comprobante               as header_bonification_pct,
        precio_total_articulo               as line_total,
        precio_total_articulo * ((100 - descuento_comprobante) / 100) as sale_total,
        costo_venta_articulo                as cost_total,

        pp_descuento_pct                    as pp_discount_pct,
        pp_precio                           as pp_sale_total,
        pp_descuento_proveedor_pct          as pp_provider_discount_pct,
        pp_costo_proveedor                  as pp_cost_total,

        es_venta_gm                         as is_gm_sale,
        es_cuenta_secundaria                as is_secondary_account,
        es_articulo_valido                  as is_valid_article,
        es_valido_para_unidades             as is_valid_for_units

    from {{ ref('int_sales_pp') }}

    {% if is_incremental() %}
    where to_char(fecha_comprobante, 'YYYY-MM')
          between '{{ var("min_month") }}' and '{{ var("max_month") }}'
    {% endif %}

) renamed
