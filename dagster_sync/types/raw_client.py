from typing import TypedDict


class RawClientRow(TypedDict):
    # Identificacion
    codigocliente: str
    codigoparticular: str
    cuenta_principal_codigo: str
    cuenta_principal_particular: str

    # Datos comerciales
    razonsocial: str
    nombre_fantasia: str
    vendedor: str
    zona: str
    barrio: str
    localidad: str
    domicilio: str
    telefono: str
    comentario: str

    # Flags
    gm: bool
    ag: bool
    es_excel: bool
    es_agro: bool
    es_plan_gomeria: bool
    contrareembolso: int
    contradeposito: int

    # Ubicacion
    latitude: float | None
    longitude: float | None

    # Objetos anidados (JSONB)
    conditions: dict
    discounts: dict
    campos_dinamicos: list
    sucursales: list
