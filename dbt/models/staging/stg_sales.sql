with source as (

    select * from {{ source('raw', 'raw_sales') }}

),

renamed as (

    select
        -- Comprobante
        tipo_comprobante,
        numero_comprobante::bigint          as numero_comprobante,
        nro_punto_venta::int                as nro_punto_venta,
        db                                  as fuente,
        tipo_consumo,

        -- Temporal
        fecha_comprobante::timestamp        as fecha_comprobante,

        -- Cliente (de CABEZA)
        codigo_cliente,
        razon_social_cliente,
        descuento_comprobante::numeric      as descuento_comprobante,

        -- Artículo (de CUERPO)
        codigo_articulo,
        codigo_particular_articulo,
        descripcion_articulo,
        cantidad_articulo::numeric          as cantidad_articulo,
        precio_unitario_articulo::numeric   as precio_unitario_articulo,
        precio_total_articulo::numeric      as precio_total_articulo,
        costo_venta_articulo::numeric       as costo_venta_articulo,
        descuento_articulo::numeric         as descuento_articulo,
        codigo_deposito_articulo

    from source

)

select * from renamed
