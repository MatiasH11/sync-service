from typing import TypedDict


class RawSaleRow(TypedDict):
    # Client
    codigocliente: str
    cliente_codigo_particular: str

    # Article
    codigoarticulo: str
    codigo_particular: str
    descripcion_articulo: str

    # Amounts
    unidades: float
    precio: float
    precio_prov: float | None

    # Voucher
    tipocomprobante: str
    numerocomprobante: str
    nropuntodeventa: int
    codigodeposito: str

    # Classification
    tipo_consumo: str
    unidadesxrubro: float
    codigosuperrubro: int
    descripcion_rubro: str

    # Datetime
    fechahoracomprobante: str

    # Brand / line
    marca_id: str | None
    marca: str | None
    linea_id: str | None
    linea: str | None
    codigomarca: int

    # Source table
    db: str
