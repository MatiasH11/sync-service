from typing import TypedDict


class RawMarcaLineaRow(TypedDict):
    codigo_marca_int: int
    marca_id:         str
    marca:            str
    linea_id:         str
    linea:            str
