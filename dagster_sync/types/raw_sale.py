from typing import TypedDict


class RawSaleRow(TypedDict):
    # Comprobante
    tipo_comprobante: str
    numero_comprobante: float
    nro_punto_venta: int
    db: str
    tipo_consumo: str

    # Temporal
    fecha_comprobante: str

    # Cliente (de CABEZA)
    codigo_cliente: str
    razon_social_cliente: str
    descuento_comprobante: float

    # Artículo (de CUERPO)
    codigo_articulo: str
    codigo_particular_articulo: str
    descripcion_articulo: str
    cantidad_articulo: float
    precio_unitario_articulo: float
    precio_total_articulo: float
    costo_venta_articulo: float
    descuento_articulo: float
    codigo_deposito_articulo: str
