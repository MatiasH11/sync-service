-- Staging: Descuento PP por marca de proveedor
--
-- Propósito: Exponer el porcentaje de descuento por pronto pago que cada proveedor
--            otorga por marca. Este descuento se aplica sobre el costo de venta
--            (provider_price) para calcular pp_provider_cost.
--
-- Keyed por brand_code (CODIGOMARCA en MySQL).
-- Marcas sin registro en esta tabla → descuento = 0 (via COALESCE en int_sales_pp).
--
-- Fuente: raw.raw_pp_provider ← DESC_PP_PROV (MySQL AWS, solo registros ACTIVO=1)
-- Usado en: int_sales_pp para calcular pp_provider_cost

{{ config(materialized='view') }}

select distinct on (brand_code)
    brand_code,
    provider_code,
    provider_name,
    brand_description,
    pp_discount_pct
from {{ source('raw', 'raw_pp_provider') }}
order by brand_code, pp_discount_pct desc
