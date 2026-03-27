from typing import TypedDict


class ClientApiConditions(TypedDict):
    condicion: str
    dias: int
    limiteMonto: float
    monto: float
    plan: int


class ClientApiDiscountByCondition(TypedDict):
    code: str
    description: str
    value: float
    other: str


class ClientApiDiscountByBrand(TypedDict):
    code: str
    description: str
    value: float


class ClientApiDiscounts(TypedDict):
    byBonus: float
    byCondition: list[ClientApiDiscountByCondition]
    byBrand: list[ClientApiDiscountByBrand]


class ClientApiCuentaPrincipal(TypedDict):
    codigoCliente: str
    codigoParticular: str


class ClientApiCoordinates(TypedDict):
    latitude: float | None
    longitude: float | None


class ClientApiCampoDinamico(TypedDict):
    codigo: int
    descripcion: str
    nombre: str
    valor: str


class ClientApiSucursal(TypedDict):
    id: str
    localidad: str
    cp: str
    zona: str | None
    direccion: str
    latitude: float | None
    longitude: float | None


class ClientApiAttributes(TypedDict):
    cdCliente: str
    codigoParticular: str
    razonSocial: str
    nombreFantasia: str
    vendedor: str
    zona: str
    barrio: str
    localidad: str
    domicilio: str
    telefono: str
    comentario: str
    claveWeb: str
    gm: bool
    ag: bool
    cuentaYOrdenAg: str
    esExcel: bool
    esAgro: bool
    esPlanGomeria: bool
    contrareembolso: int
    contradeposito: int
    cuentaPrincipal: ClientApiCuentaPrincipal | None
    conditions: ClientApiConditions
    discounts: ClientApiDiscounts
    coordinates: ClientApiCoordinates
    camposDinamicos: list[ClientApiCampoDinamico]
    sucursales: list[ClientApiSucursal]


class ClientApiResponse(TypedDict):
    type: str
    id: str
    attributes: ClientApiAttributes
