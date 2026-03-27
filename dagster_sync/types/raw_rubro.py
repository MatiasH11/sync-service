from typing import TypedDict


class RawRubroRow(TypedDict):
    codigo_rubro:            str
    codigo_super_rubro:      str
    descripcion_super_rubro: str
    unidades_super_rubro:    float
