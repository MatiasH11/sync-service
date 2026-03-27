with source as (

    select * from {{ source('raw', 'raw_price_history') }}

),

renamed as (

    select
        codigoarticulo                          as codigo_articulo,
        precioactual::numeric                   as precio_actual_proveedor,
        fechamodificacion::timestamp            as fecha_modificacion,
        precio::numeric                         as precio_cambio

    from source

)

select * from renamed
