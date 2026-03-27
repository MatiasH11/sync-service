with source as (

    select * from {{ source('raw', 'raw_clients') }}

),

renamed as (

    select
        -- Identificación
        codigocliente                           as codigo_cliente,
        codigoparticular                        as codigo_particular,
        cuenta_principal_codigo,
        cuenta_principal_particular,

        -- Datos comerciales
        razonsocial                             as razon_social,
        nombre_fantasia,
        vendedor                                as codigo_vendedor,
        zona                                    as codigo_zona,
        barrio,
        localidad,
        domicilio,
        telefono,
        comentario,

        -- Flags
        gm::boolean                             as es_gm,
        ag::boolean                             as es_ag,
        es_excel::boolean,
        es_agro::boolean,
        es_plan_gomeria::boolean,
        contrareembolso::boolean,
        contradeposito::boolean,

        -- Ubicación
        latitude,
        longitude,

        -- Objetos estructurados (JSONB — para uso en intermediate si se necesitan)
        conditions,
        discounts,
        campos_dinamicos,
        sucursales

    from source

)

select * from renamed
