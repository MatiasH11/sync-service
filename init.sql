-- Schemas para separar capas del pipeline
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS analytics;

-- =============================================
-- RAW TABLES (destino de la extraccion)
-- =============================================

CREATE TABLE IF NOT EXISTS raw.raw_sales (
    -- Comprobante
    tipo_comprobante          VARCHAR(15),
    numero_comprobante        DOUBLE PRECISION,
    nro_punto_venta           BIGINT,
    db                        VARCHAR(20),
    tipo_consumo              VARCHAR(10),
    -- Temporal
    fecha_comprobante         TIMESTAMP,
    -- Cliente (de CABEZA)
    codigo_cliente            VARCHAR(20),
    razon_social_cliente      TEXT,
    descuento_comprobante     DOUBLE PRECISION,
    -- Artículo (de CUERPO)
    nro_linea                 INTEGER,
    codigo_articulo           VARCHAR(20),
    codigo_particular_articulo VARCHAR(40),
    descripcion_articulo      VARCHAR(255),
    cantidad_articulo         DOUBLE PRECISION,
    precio_unitario_articulo  DOUBLE PRECISION,
    precio_total_articulo     DOUBLE PRECISION,
    costo_venta_articulo      DOUBLE PRECISION,
    descuento_articulo        DOUBLE PRECISION,
    codigo_deposito_articulo  VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS raw.raw_clients (
    -- Identificacion
    codigocliente               VARCHAR(20) PRIMARY KEY,
    codigoparticular            VARCHAR(20),
    cuenta_principal_codigo     VARCHAR(20),
    cuenta_principal_particular VARCHAR(20),
    -- Datos comerciales
    razonsocial                 VARCHAR(200),
    nombre_fantasia             VARCHAR(200),
    vendedor                    VARCHAR(20),
    zona                        VARCHAR(100),
    barrio                      VARCHAR(200),
    localidad                   VARCHAR(200),
    domicilio                   VARCHAR(200),
    telefono                    VARCHAR(50),
    comentario                  TEXT,
    -- Flags
    gm                          BOOLEAN,
    ag                          BOOLEAN,
    es_excel                    BOOLEAN,
    es_agro                     BOOLEAN,
    es_plan_gomeria             BOOLEAN,
    contrareembolso             INTEGER,
    contradeposito              INTEGER,
    -- Ubicacion
    latitude                    DOUBLE PRECISION,
    longitude                   DOUBLE PRECISION,
    -- Objetos anidados
    conditions                  JSONB,
    discounts                   JSONB,
    campos_dinamicos            JSONB,
    sucursales                  JSONB
);

CREATE TABLE IF NOT EXISTS raw.raw_articulos (
    codigo_articulo    VARCHAR(20) PRIMARY KEY,
    codigo_particular  VARCHAR(40),
    descripcion        TEXT,
    codigo_rubro       VARCHAR(20),
    codigo_marca       VARCHAR(20),
    descripcion_marca  TEXT,
    is_valid_for_units BOOLEAN
);

CREATE TABLE IF NOT EXISTS raw.raw_excel_clients (
    codigocliente VARCHAR(20),
    codigo_principal VARCHAR(20),
    valor VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS raw.raw_sellers (
    codigovendedor VARCHAR(20) PRIMARY KEY,
    nombre VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS raw.raw_price_history (
    codigoarticulo VARCHAR(20),
    precioactual DOUBLE PRECISION,
    fechamodificacion TIMESTAMP,
    precio DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS raw.raw_rubros (
    codigo_rubro             VARCHAR(20) PRIMARY KEY,
    codigo_super_rubro       VARCHAR(20),
    descripcion_super_rubro  VARCHAR(255),
    unidades_super_rubro     DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS raw.raw_marcas_lineas (
    codigo_marca_int  INTEGER PRIMARY KEY,
    marca_id          VARCHAR(20),
    marca             VARCHAR(255),
    linea_id          VARCHAR(20),
    linea             VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS raw.raw_pp_monthly (
    -- Descuento PP promedio mensual (fuente: distriap_distri.PROMEDIO_PRONTOPAGO_V2)
    -- Un registro por mes. Se recarga completo en cada sincronización.
    pp_year          SMALLINT       NOT NULL,
    pp_month         SMALLINT       NOT NULL,
    pp_discount_pct  NUMERIC(6, 2)  NOT NULL,
    PRIMARY KEY (pp_year, pp_month)
);

CREATE TABLE IF NOT EXISTS raw.raw_pp_provider (
    -- Descuento PP por artículo de proveedor (fuente: DESC_PP_PROV en MySQL AWS)
    -- Se recarga completo en cada sincronización (TRUNCATE + COPY).
    -- Solo se cargan registros ACTIVO = 1 con CODIGOARTICULO no nulo.
    article_code       VARCHAR(20),
    brand_code         VARCHAR(20),
    provider_code      VARCHAR(20),
    provider_name      VARCHAR(200),
    brand_description  VARCHAR(200),
    pp_discount_pct    NUMERIC(6, 2)
);

-- =============================================
-- INDICES para performance en queries
-- =============================================

CREATE INDEX IF NOT EXISTS idx_raw_sales_fecha
    ON raw.raw_sales (fecha_comprobante);

CREATE INDEX IF NOT EXISTS idx_raw_sales_cliente
    ON raw.raw_sales (codigo_cliente);

CREATE INDEX IF NOT EXISTS idx_raw_sales_articulo
    ON raw.raw_sales (codigo_articulo);

CREATE INDEX IF NOT EXISTS idx_raw_sales_db_fecha
    ON raw.raw_sales (db, fecha_comprobante);

CREATE INDEX IF NOT EXISTS idx_raw_sales_yearmonth
    ON raw.raw_sales (fecha_comprobante, codigo_cliente);

CREATE INDEX IF NOT EXISTS idx_raw_price_history_articulo
    ON raw.raw_price_history (codigoarticulo, fechamodificacion);
