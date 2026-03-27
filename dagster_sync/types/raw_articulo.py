from typing import TypedDict


class RawArticuloRow(TypedDict):
    codigo_articulo: str
    codigo_particular: str
    descripcion: str
    codigo_rubro: str
    codigo_marca: str
    descripcion_marca: str
