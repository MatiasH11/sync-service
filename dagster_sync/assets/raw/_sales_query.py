from typing import TypedDict


class SalesTableConfig(TypedDict):
    cabeza: str
    cuerpo: str
    tipo_consumo: str
    db: str


EXCLUDED_ARTICLES = ['42537', '42538', '42357', '45436']

VALID_VOUCHER_TYPES = ('FA', 'FB', 'NCA', 'NCB', 'NDA', 'NDB', 'RE')

SALES_TABLES: list[SalesTableConfig] = [
    {'cabeza': 'DIMDS_CABEZACOMPROBANTES',   'cuerpo': 'DIMDS_CUERPOCOMPROBANTES',   'tipo_consumo': 'DS',   'db': 'DIMDS'},
    {'cabeza': 'DIMPPAL_CABEZACOMPROBANTES',  'cuerpo': 'DIMPPAL_CUERPOCOMPROBANTES',  'tipo_consumo': 'PPAL', 'db': 'DIMPPAL'},
    {'cabeza': 'DISDS_CABEZACOMPROBANTES',   'cuerpo': 'DISDS_CUERPOCOMPROBANTES',   'tipo_consumo': 'DS',   'db': 'DISDS'},
    {'cabeza': 'DISPPAL_CABEZACOMPROBANTES',  'cuerpo': 'DISPPAL_CUERPOCOMPROBANTES',  'tipo_consumo': 'PPAL', 'db': 'DISPPAL'},
]


def build_sales_query(cabeza: str, cuerpo: str, tipo_consumo: str, db: str) -> str:
    excluded = ', '.join(f"'{a}'" for a in EXCLUDED_ARTICLES)
    vouchers = ', '.join(f"'{t}'" for t in VALID_VOUCHER_TYPES)

    return f"""
        SELECT
            c.CODIGOCLIENTE                         AS codigocliente,
            c.CODIGOPARTICULAR                      AS cliente_codigo_particular,
            cu.CODIGOARTICULO                       AS codigoarticulo,
            cu.CODIGOPARTICULAR                     AS codigo_particular,
            cu.DESCRIPCION                          AS descripcion_articulo,
            cu.UNIDADES                             AS unidades,
            cu.PRECIO                               AS precio,
            NULL                                    AS precio_prov,
            c.TIPOCOMPROBANTE                       AS tipocomprobante,
            c.NUMEROCOMPROBANTE                     AS numerocomprobante,
            c.NROPUNTODEVENTA                       AS nropuntodeventa,
            c.CODIGODEPOSITO                        AS codigodeposito,
            '{tipo_consumo}'                        AS tipo_consumo,
            IFNULL(uxr.UNIDADES, 0)                 AS unidadesxrubro,
            IFNULL(cu.CODIGOSUPERRUBRO, -1)          AS codigosuperrubro,
            IFNULL(sr.DESCRIPCION, 'SIN RUBRO')     AS descripcion_rubro,
            c.FECHAHORACOMPROBANTE                  AS fechahoracomprobante,
            ml.id                                   AS marca_id,
            m.marca                                 AS marca,
            l.linea_id                              AS linea_id,
            l.linea                                 AS linea,
            IFNULL(cu.CODIGOMARCA, -1)              AS codigomarca,
            '{db}'                                  AS db
        FROM {cabeza} c
        JOIN {cuerpo} cu
            ON  c.TIPOCOMPROBANTE   = cu.TIPOCOMPROBANTE
            AND c.NROPUNTODEVENTA   = cu.NROPUNTODEVENTA
            AND c.NUMEROCOMPROBANTE = cu.NUMEROCOMPROBANTE
        LEFT JOIN MarcasxLineas ml   ON cu.CODIGOMARCA      = ml.id
        LEFT JOIN Marcas m           ON ml.marca_id          = m.marca_id
        LEFT JOIN Lineas l           ON ml.linea_id          = l.linea_id
        LEFT JOIN SuperRubros sr     ON cu.CODIGOSUPERRUBRO  = sr.CODIGOSUPERRUBRO
        LEFT JOIN UnidadesxRubro uxr ON cu.CODIGOSUPERRUBRO  = uxr.CODIGOSUPERRUBRO
        WHERE c.FECHAHORACOMPROBANTE BETWEEN %s AND %s
          AND c.TIPOCOMPROBANTE IN ({vouchers})
          AND c.ANULADA = 0
          AND cu.CODIGOARTICULO NOT IN ({excluded})
    """
