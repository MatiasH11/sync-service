from datetime import datetime
from typing import Optional, TypedDict


class FctSaleRow(TypedDict):
    """
    Schema for analytics.fct_sales.

    Replaces:
      - fct_sales (DuckDB) consumed by api-vendedores
      - RENT_COMERCIAL (MySQL) consumed by api-quantix

    Design principles:
      1. Comprobante is truth — what Flexxus recorded, untransformed
      2. Enrichments are lookups — dimensions from masters to avoid consumer joins
      3. Only process what the consumer can't — PP requires monthly external tables
      4. Flags for complex detection — GM and secondary require multi-table logic
      5. No pre-aggregations — breakdowns belong in the reporting layer

    Lineage: stg_sales → int_sales_accounts → int_sales_gm
             → int_sales_enriched → int_sales_pp → fct_sales
    """

    # -------------------------------------------------------------------------
    # Comprobante
    # -------------------------------------------------------------------------
    source:             str     # DIMDS | DIMPPAL | DISDS | DISPPAL
    voucher_type:       str     # FA, FB, NCA, NCB, NDA, NDB, RE
    voucher_number:     int
    voucher_line:       int     # NROLINEA — line number within the invoice body
    point_of_sale:      int
    deposit_code:       str     # CODIGODEPOSITO

    # -------------------------------------------------------------------------
    # Temporal
    # -------------------------------------------------------------------------
    invoice_datetime:   datetime
    year_month:         str     # Calculated: YYYY-MM

    # -------------------------------------------------------------------------
    # Classification (calculated)
    # -------------------------------------------------------------------------
    branch:             str     # BA | MDP | PICO | ROSARIO | UNKNOWN
    consumption_type:   str     # DS | PPAL

    # -------------------------------------------------------------------------
    # Client
    # -------------------------------------------------------------------------
    client_code:            str             # Comprobante: original code from the invoice
    account_code:           str             # Enriched: resolved main account — use for all joins
    account_name:           str             # Enriched
    client_particular_code: Optional[str]   # Enriched: key for api-quantix
    vendor_code:            Optional[str]   # Enriched
    vendor_name:            Optional[str]   # Enriched
    zone_code:              Optional[str]   # Enriched

    # -------------------------------------------------------------------------
    # Article
    # -------------------------------------------------------------------------
    article_code:           str             # Comprobante
    article_particular_code: Optional[str]  # Enriched: coalesce(master, invoice)
    article_description:    Optional[str]   # Enriched: coalesce(master, invoice)
    rubro_code:             Optional[str]   # Enriched: -1 or 377 → is_valid_article = false
    rubro_description:      Optional[str]   # Enriched
    rubro_min_units:        int             # Enriched: SR achievement threshold; 0 if unknown
    brand_code:             Optional[int]   # Enriched: integer code from RDS
    brand_id:               Optional[str]   # Enriched: UUID from product-service
    brand_name:             Optional[str]   # Enriched
    product_line_id:        Optional[str]   # Enriched: UUID from product-service
    product_line_name:      Optional[str]   # Enriched

    # -------------------------------------------------------------------------
    # Provider
    # -------------------------------------------------------------------------
    provider_code:          Optional[str]   # Enriched: from DESC_PP_PROV
    provider_name:          Optional[str]   # Enriched: from DESC_PP_PROV

    # -------------------------------------------------------------------------
    # Metrics
    # -------------------------------------------------------------------------
    article_unit_price:     float           # PRECIOUNITARIO
    article_quantity:       int             # CANTIDAD
    line_discount_pct:      float           # DESCUENTO — line-level discount %
    header_bonification_pct: float          # DESCUENTOPORCENTAJE — voucher bonification %
    line_total:             float           # PRECIOTOTAL — before header bonification
    sale_total:             float           # line_total × (1 - header_bonification_pct/100) — net sale
    cost_total:             float           # COSTOVENTA

    # -------------------------------------------------------------------------
    # PP (Pronto Pago) — processed totals
    # -------------------------------------------------------------------------
    pp_discount_pct:        float   # Monthly PP %. Client '01129' = 0%. Fallback 22%.
    pp_sale_total:          float   # sale_total × (1 - header_bonification_pct/100) × (1 - pp_discount_pct/100). Equiv. TOTAL_PP.
    pp_cost_total:          float   # cost_total × (1 - article PP%/100). Equiv. COMPRAS_PP.

    # -------------------------------------------------------------------------
    # Flags
    # -------------------------------------------------------------------------
    is_gm_sale:         bool    # RE + PDV GM + barrio regex match
    is_secondary_account: bool  # Exclude from aggregations to avoid double counting
    is_valid_article:   bool    # False if rubro_code IN (-1, 377)
