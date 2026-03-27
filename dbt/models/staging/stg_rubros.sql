with source as (

    select * from {{ source('raw', 'raw_rubros') }}

),

renamed as (

    select
        codigo_rubro,
        codigo_super_rubro,
        descripcion_super_rubro,
        unidades_super_rubro    as unidades_min_rubro

    from source

)

select * from renamed
