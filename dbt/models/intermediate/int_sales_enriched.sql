-- Intermediate: Enriquecimiento con dimensiones de artículo
--
-- Propósito: Agregar datos de artículo, rubro y marca/línea a cada línea de venta.
--            Este join no existe en el pipeline viejo porque allí se hacía
--            directamente en la query MySQL raw. Acá es necesario porque
--            raw_sales solo trae codigo_articulo.
--
-- LEFT JOINs intencionales: una venta con artículo sin maestro no debe perderse,
-- aparecerá con campos de dimensión en NULL y es_articulo_valido = false.

{{ config(materialized='view') }}

with ventas as (

    select * from {{ ref('int_sales_gm') }}

),

articulos as (

    select
        codigo_articulo,
        codigo_particular,
        descripcion,
        codigo_rubro,
        codigo_marca
    from {{ ref('stg_articulos') }}

),

rubros as (

    select
        codigo_rubro,
        codigo_super_rubro,
        descripcion_super_rubro,
        unidades_min_rubro
    from {{ ref('stg_rubros') }}

),

marcas as (

    select
        codigo_marca,
        marca_id,
        marca,
        linea_id,
        linea
    from {{ ref('stg_marcas_lineas') }}

),

enriquecido as (

    select
        v.*,

        -- Artículo (de product-service)
        coalesce(a.codigo_particular, v.codigo_particular_articulo) as codigo_particular_articulo_master,
        coalesce(a.descripcion, v.descripcion_articulo)             as descripcion_articulo_master,

        -- Rubro
        r.codigo_super_rubro,
        r.descripcion_super_rubro,
        coalesce(r.unidades_min_rubro, 0)                           as unidades_min_rubro,

        -- Marca y línea
        a.codigo_marca                                              as codigo_marca_int,
        m.marca_id,
        m.marca,
        m.linea_id,
        m.linea,

        -- Validez general (montos): producción no filtra por rubro para montos,
        -- solo excluye artículos específicos (ya excluidos en la extracción).
        -- Aquí solo descartamos NULL y -1 (datos rotos / sin rubro).
        case
            when r.codigo_super_rubro is null
              or r.codigo_super_rubro::int = -1
            then false
            else true
        end as es_articulo_valido,

        -- Validez para unidades: además de lo anterior, excluir rubros sin
        -- registro en UNIDADESXRUBRO (réplica de IFNULL(u.CODIGOSUPERRUBRO, -1))
        case
            when r.codigo_super_rubro is null
              or r.codigo_super_rubro::int in (-1, 377)
              or r.descripcion_super_rubro = 'SIN SUPERRUBRO'
            then false
            else true
        end as es_valido_para_unidades

    from ventas v
    left join articulos a
        on v.codigo_articulo = a.codigo_articulo
    left join rubros r
        on a.codigo_rubro = r.codigo_rubro
    left join marcas m
        on a.codigo_marca::int = m.codigo_marca

)

select * from enriquecido
