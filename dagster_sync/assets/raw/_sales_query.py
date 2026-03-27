from typing import TypedDict


class SalesTableConfig(TypedDict):
    cabeza: str
    cuerpo: str
    tipo_consumo: str
    db: str


EXCLUDED_ARTICLES = ['42537', '42538', '42357', '45436']

VALID_VOUCHER_TYPES = ('FA', 'FB', 'NCA', 'NCB', 'NDA', 'NDB', 'RE')

SALES_TABLES: list[SalesTableConfig] = [
    {'cabeza': 'DIMDS_CABEZACOMPROBANTES',  'cuerpo': 'DIMDS_CUERPOCOMPROBANTES',  'tipo_consumo': 'DS',   'db': 'DIMDS'},
    {'cabeza': 'DIMPPAL_CABEZACOMPROBANTES', 'cuerpo': 'DIMPPAL_CUERPOCOMPROBANTES', 'tipo_consumo': 'PPAL', 'db': 'DIMPPAL'},
    {'cabeza': 'DISDS_CABEZACOMPROBANTES',  'cuerpo': 'DISDS_CUERPOCOMPROBANTES',  'tipo_consumo': 'DS',   'db': 'DISDS'},
    {'cabeza': 'DISPPAL_CABEZACOMPROBANTES', 'cuerpo': 'DISPPAL_CUERPOCOMPROBANTES', 'tipo_consumo': 'PPAL', 'db': 'DISPPAL'},
]


def build_sales_query(cabeza: str, cuerpo: str, tipo_consumo: str, db: str) -> str:
    excluded = ', '.join(f"'{a}'" for a in EXCLUDED_ARTICLES)
    vouchers = ', '.join(f"'{t}'" for t in VALID_VOUCHER_TYPES)

    return f"""
        SELECT
            c.TIPOCOMPROBANTE                                       AS tipo_comprobante,
            c.NUMEROCOMPROBANTE                                     AS numero_comprobante,
            c.NROPUNTODEVENTA                                       AS nro_punto_venta,
            '{db}'                                                  AS db,
            '{tipo_consumo}'                                        AS tipo_consumo,
            TIMESTAMP(DATE(c.FECHACOMPROBANTE), TIME(c.HORA))       AS fecha_comprobante,
            c.CODIGOCLIENTE                                         AS codigo_cliente,
            c.RAZONSOCIAL                                           AS razon_social_cliente,
            c.DESCUENTOPORCENTAJE                                   AS descuento_comprobante,
            cu.CODIGOARTICULO                                       AS codigo_articulo,
            cu.CODIGOPARTICULAR                                     AS codigo_particular_articulo,
            cu.DESCRIPCION                                          AS descripcion_articulo,
            cu.CANTIDAD                                             AS cantidad_articulo,
            cu.PRECIOUNITARIO                                       AS precio_unitario_articulo,
            cu.PRECIOTOTAL                                          AS precio_total_articulo,
            cu.COSTOVENTA                                           AS costo_venta_articulo,
            cu.DESCUENTO                                            AS descuento_articulo,
            cu.CODIGODEPOSITO                                       AS codigo_deposito_articulo
        FROM {cabeza} c
        JOIN {cuerpo} cu
            ON  c.TIPOCOMPROBANTE   = cu.TIPOCOMPROBANTE
            AND c.NUMEROCOMPROBANTE = cu.NUMEROCOMPROBANTE
        WHERE TIMESTAMP(DATE(c.FECHACOMPROBANTE), TIME(c.HORA)) BETWEEN %s AND %s
          AND c.TIPOCOMPROBANTE IN ({vouchers})
          AND c.ANULADA = 0
          AND cu.CODIGOARTICULO NOT IN ({excluded})
    """
