from typing import TypedDict


class ProductApiMarca(TypedDict):
    codigoMarca: str
    descripcion: str


class ProductApiCampoDinamico(TypedDict):
    codigo: int
    descripcion: str | None
    nombre: str
    valor: str


class ProductApiAttributes(TypedDict):
    codigoParticular: str
    descripcion: str
    codigoRubro: str
    precioCompra: float
    precioVenta1: float
    precioVenta2: float
    marca: ProductApiMarca
    precioArticuloProveedor: float
    camposDinamicos: list[ProductApiCampoDinamico]
    codigoMarca: str


class ProductApiResponse(TypedDict):
    type: str
    id: str
    attributes: ProductApiAttributes
