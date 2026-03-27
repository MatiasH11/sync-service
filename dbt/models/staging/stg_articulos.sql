with source as (

    select * from {{ source('raw', 'raw_articulos') }}

),

renamed as (

    select
        codigo_articulo,
        codigo_particular,
        trim(descripcion)       as descripcion,
        codigo_rubro,
        codigo_marca,
        trim(descripcion_marca) as descripcion_marca

    from source

)

select * from renamed
