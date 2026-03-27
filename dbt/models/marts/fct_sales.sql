-- Mart: fct_sales — Sales Fact Table
--
-- Single source of truth for sales analytics.
-- Replaces fct_sales (DuckDB) for api-vendedores
-- and RENT_COMERCIAL (MySQL) for api-quantix.
--
-- All business logic is pre-applied:
--   - Account resolution (secondary → main account)
--   - GM discount calculation with historical price lookup
--   - Pre-calculated breakdowns (GM / DS / PPAL)
--
-- Common query patterns:
--   WHERE year_month = '2026-03' AND vendor_code = 'V01'
--   GROUP BY year_month, account_code, brand_name

{{ config(materialized='table') }}

select

    -- -------------------------------------------------------------------------
    -- Comprobante
    -- -------------------------------------------------------------------------
    voucher_type,
    voucher_number,
    source,
    point_of_sale,
    deposit_code,
    branch,
    consumption_type,

    -- -------------------------------------------------------------------------
    -- Temporal
    -- -------------------------------------------------------------------------
    invoice_datetime,
    year_month,

    -- -------------------------------------------------------------------------
    -- Client (always the main account)
    -- -------------------------------------------------------------------------
    client_code,
    account_code,
    account_name,
    client_particular_code,
    vendor_code,
    vendor_name,
    zone_code,
    is_secondary_account,

    -- -------------------------------------------------------------------------
    -- Article
    -- -------------------------------------------------------------------------
    article_code,
    article_particular_code,
    article_description,
    rubro_code,
    rubro_description,
    min_units_rubro,
    brand_code,
    brand_id,
    brand_name,
    line_id,
    line_name,

    -- -------------------------------------------------------------------------
    -- Metrics
    -- -------------------------------------------------------------------------
    quantity,
    sale_price,             -- Total line price as recorded in MySQL
    provider_price,         -- Cost of goods (COSTOVENTA from comprobante)
    provider_price_at_sale, -- Provider catalog price at sale datetime (ASOF join)
    final_price,            -- Calculated: GM discounted / 0 if secondary / sale_price

    -- -------------------------------------------------------------------------
    -- GM (Gran Minorista)
    -- -------------------------------------------------------------------------
    is_gm_sale,
    gm_discount_pct,
    gm_discount_amount,

    -- -------------------------------------------------------------------------
    -- Pre-calculated breakdowns (mutually exclusive)
    -- -------------------------------------------------------------------------
    gm_quantity,
    gm_amount,
    ds_quantity,
    ds_amount,
    ppal_quantity,
    ppal_amount,

    -- -------------------------------------------------------------------------
    -- Flags
    -- -------------------------------------------------------------------------
    is_valid_article,       -- false if rubro_code IN (-1, 377)
    price_method            -- GM_DISCOUNTED | SECONDARY_ZEROED | STANDARD_PRICE

from (

    select
        tipo_comprobante                    as voucher_type,
        numero_comprobante                  as voucher_number,
        fuente                              as source,
        nro_punto_venta                     as point_of_sale,
        codigo_deposito_articulo            as deposit_code,
        sucursal                            as branch,
        tipo_consumo                        as consumption_type,

        fecha_comprobante                   as invoice_datetime,
        to_char(fecha_comprobante, 'YYYY-MM') as year_month,

        codigo_cliente                      as client_code,
        codigo_cuenta_resuelta              as account_code,
        razon_social_cuenta                 as account_name,
        codigo_particular_cliente           as client_particular_code,
        codigo_vendedor_cliente             as vendor_code,
        nombre_vendedor                     as vendor_name,
        codigo_zona_cliente                 as zone_code,
        es_cuenta_secundaria                as is_secondary_account,

        codigo_articulo                     as article_code,
        codigo_particular_articulo_master   as article_particular_code,
        descripcion_articulo_master         as article_description,
        codigo_super_rubro                  as rubro_code,
        descripcion_super_rubro             as rubro_description,
        unidades_min_rubro                  as min_units_rubro,
        codigo_marca_int                    as brand_code,
        marca_id                            as brand_id,
        marca                               as brand_name,
        linea_id                            as line_id,
        linea                               as line_name,

        cantidad_articulo                   as quantity,
        precio_total_articulo               as sale_price,
        costo_venta_articulo                as provider_price,
        precio_proveedor_vigente            as provider_price_at_sale,
        precio_final                        as final_price,

        es_venta_gm                         as is_gm_sale,
        gm_descuento_pct                    as gm_discount_pct,
        gm_descuento_monto                  as gm_discount_amount,

        gm_cantidad                         as gm_quantity,
        gm_monto                            as gm_amount,
        ds_cantidad                         as ds_quantity,
        ds_monto                            as ds_amount,
        ppal_cantidad                       as ppal_quantity,
        ppal_monto                          as ppal_amount,

        es_articulo_valido                  as is_valid_article,
        case metodo_precio
            when 'GM_DESCONTADO'    then 'GM_DISCOUNTED'
            when 'SECUNDARIA_CERO'  then 'SECONDARY_ZEROED'
            else 'STANDARD_PRICE'
        end                                 as price_method

    from {{ ref('int_sales_breakdown') }}

) renamed
