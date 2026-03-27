from datetime import datetime
from typing import Optional, TypedDict


class FctSaleRow(TypedDict):
    """
    Schema for analytics.fct_sales.

    Replaces:
      - fct_sales (DuckDB) consumed by api-vendedores
      - RENT_COMERCIAL (MySQL) consumed by api-quantix

    All business logic is pre-applied:
      - Account resolution: secondary accounts map to their main account.
      - GM discount: calculated using provider_price_at_sale (ASOF join).
      - Pre-computed breakdowns: gm / ds / ppal amounts are mutually exclusive.
    """

    # -------------------------------------------------------------------------
    # Comprobante
    # -------------------------------------------------------------------------
    voucher_type:       str     # FA, FB, NCA, NCB, NDA, NDB, RE
    voucher_number:     int
    source:             str     # DIMDS | DIMPPAL | DISDS | DISPPAL
    point_of_sale:      int
    deposit_code:       str     # Article-level deposit code
    branch:             str     # BA | MDP | PICO | ROSARIO | UNKNOWN
    consumption_type:   str     # DS | PPAL

    # -------------------------------------------------------------------------
    # Temporal
    # -------------------------------------------------------------------------
    invoice_datetime:   datetime
    year_month:         str     # YYYY-MM — use for GROUP BY and range filters

    # -------------------------------------------------------------------------
    # Client (always resolved to main account)
    # -------------------------------------------------------------------------
    client_code:            str             # Original code from the invoice
    account_code:           str             # Resolved main account — use for all joins
    account_name:           str
    client_particular_code: Optional[str]   # Particular code from client-service
    vendor_code:            Optional[str]
    vendor_name:            Optional[str]
    zone_code:              Optional[str]
    is_secondary_account:   bool            # True if client was a secondary (Excel) account

    # -------------------------------------------------------------------------
    # Article
    # -------------------------------------------------------------------------
    article_code:           str
    article_particular_code: Optional[str]
    article_description:    Optional[str]
    rubro_code:             Optional[str]   # Super-rubro. -1 or 377 = invalid
    rubro_description:      Optional[str]
    min_units_rubro:        int             # Minimum units for super-rubro; 0 if unknown
    brand_code:             Optional[int]   # Integer code from RDS
    brand_id:               Optional[str]   # UUID from product-service
    brand_name:             Optional[str]
    line_id:                Optional[str]   # UUID from product-service
    line_name:              Optional[str]

    # -------------------------------------------------------------------------
    # Metrics
    # -------------------------------------------------------------------------
    quantity:               float
    sale_price:             float   # Total line price as recorded in MySQL
    provider_price:         float   # Cost of goods (COSTOVENTA from comprobante)
    provider_price_at_sale: float   # Provider catalog price at sale datetime (ASOF join)
    final_price:            float   # GM discounted / 0 if secondary / sale_price

    # -------------------------------------------------------------------------
    # GM (Gran Minorista)
    # -------------------------------------------------------------------------
    is_gm_sale:             bool
    gm_discount_pct:        Optional[float]     # Extracted from barrio field via regex
    gm_discount_amount:     float               # provider_price_at_sale × discount × qty

    # -------------------------------------------------------------------------
    # Pre-calculated breakdowns (mutually exclusive; sum to final_price)
    # -------------------------------------------------------------------------
    gm_quantity:            float
    gm_amount:              float
    ds_quantity:            float
    ds_amount:              float
    ppal_quantity:          float
    ppal_amount:            float

    # -------------------------------------------------------------------------
    # Flags
    # -------------------------------------------------------------------------
    is_valid_article:   bool    # False if rubro_code IN (-1, 377)
    price_method:       str     # GM_DISCOUNTED | SECONDARY_ZEROED | STANDARD_PRICE
