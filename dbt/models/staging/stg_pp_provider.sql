-- Staging: Descuento PP por artículo de proveedor
--
-- Propósito: Exponer el porcentaje de descuento por pronto pago que cada proveedor
--            otorga por artículo. Este descuento se aplica sobre el costo de venta
--            (cost_total) para calcular pp_provider_cost.
--
-- Keyed por article_code (CODIGOARTICULO en MySQL).
-- Artículos sin registro en esta tabla → descuento = 0 (via COALESCE en int_sales_pp).
--
-- Fuente: raw.raw_pp_provider ← DESC_PP_PROV (MySQL AWS, solo registros ACTIVO=1)
-- Usado en: int_sales_pp para calcular pp_provider_cost

{{ config(materialized='view') }}

select distinct on (article_code)
    article_code,
    brand_code,
    provider_code,
    provider_name,
    brand_description,
    pp_discount_pct
from {{ source('raw', 'raw_pp_provider') }}
order by article_code, pp_discount_pct desc
