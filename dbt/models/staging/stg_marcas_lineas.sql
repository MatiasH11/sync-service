with source as (

    select * from {{ source('raw', 'raw_marcas_lineas') }}

),

renamed as (

    select
        codigo_marca_int        as codigo_marca,
        marca_id,
        trim(marca)             as marca,
        linea_id,
        trim(linea)             as linea

    from source

)

select * from renamed
