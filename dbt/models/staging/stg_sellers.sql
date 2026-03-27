with source as (

    select * from {{ source('raw', 'raw_sellers') }}

),

renamed as (

    select
        codigovendedor      as codigo_vendedor,
        trim(nombre)        as nombre_vendedor

    from source

)

select * from renamed
