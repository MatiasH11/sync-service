# Arquitectura: Estado Actual vs Migración

## Estado Actual

```
┌─────────────────────────────────────────────────────────────────────┐
│                        FUENTES DE DATOS                             │
│                                                                     │
│  ┌────────────┐   ┌───────────────┐   ┌──────────────────┐         │
│  │  Firebird   │   │  MySQL (AWS)  │   │  client-service  │         │
│  │  (ventas)   │   │  (maestros)   │   │  (clientes)      │         │
│  └──────┬──────┘   └───────┬───────┘   └────────┬─────────┘         │
└─────────┼──────────────────┼────────────────────┼───────────────────┘
          │                  │                    │
          │                  │                    │
    ┌─────┼──────────────────┼────────────────────┼─────────────┐
    │     ▼                  ▼                    ▼             │
    │                                                          │
    │              dbt-sync (Flask :4003)                       │
    │              Sidecar de api-vendedores                    │
    │                                                          │
    │   • Extrae Firebird + MySQL + client-service             │
    │   • Escribe Parquet → DuckDB                             │
    │   • Corre dbt (adapter duckdb)                           │
    │   • Atomic swap de archivos .duckdb                      │
    │   • Endpoints: POST /sync, POST /sync/clients            │
    │                                                          │
    └──────────────────────┬───────────────────────────────────┘
                           │
                           ▼
                ┌─────────────────────┐
                │  DuckDB (archivo)   │
                │  analytics.duckdb   │
                │                     │
                │  • fct_sales        │
                │  • fct_inflation    │
                │  Compartido via     │
                │  Docker volume      │
                └─────────┬───────────┘
                          │
                          │ (solo Versus lee de acá)
                          │
                          ▼
               ┌──────────────────────┐
               │  api-vendedores      │
               │  (Node/TS :4001)     │
               │                      │
               │  DuckDBService       │
               │  • getAggregatedSales│
               │  • getRubroAchieve.  │
               │  • getFilteredSales  │
               └──────────┬───────────┘
                          │
                          ▼
               ┌──────────────────────┐
               │  app-vendedores      │
               │  (React :5173)       │
               │  VERSUS UI           │
               └──────────────────────┘


    ┌──────────────────────────────────────────────────────────┐
    │                                                          │
    │    MySQL (AWS RDS) ───────────────────────────┐          │
    │    RENT_COMERCIAL                             │          │
    │    (tabla/vista pre-agregada)                  │          │
    │    Origen desconocido / ETL externo            │          │
    │                                               ▼          │
    │                                                          │
    │                                    api-quantix           │
    │    client-service ──► Redis ──►    (Python :8128)        │
    │    (clients:raw)                                         │
    │                                    • Cachea por día      │
    │                                      en Redis (Parquet)  │
    │                                    • Enriquece en Pandas │
    │                                    • GM/DS filter Python │
    │                                    • Secondary remap     │
    │                                                          │
    └──────────────────────────┬───────────────────────────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │  quantix (React)     │
                    │  QUANTIX UI          │
                    └──────────────────────┘
```

### Problemas del estado actual

```
 ❌  Dos pipelines separados para los mismos datos
     • Versus: Firebird → dbt-sync → DuckDB → Node API
     • Quantix: MySQL (RENT_COMERCIAL) → Python/Pandas

 ❌  Drift de datos
     • Lógica GM/DS duplicada (SQL en dbt vs Python en Quantix)
     • Resultados pueden diferir por diferencias sutiles

 ❌  DuckDB como archivo
     • Write-lock: solo un proceso puede escribir
     • Compartido via Docker volume (frágil)
     • No soporta queries concurrentes en escritura

 ❌  RENT_COMERCIAL
     • Origen/refresh desconocido
     • No tiene campos de inflación ni breakdowns pre-calculados
     • Quantix debe enriquecer en Python post-query

 ❌  Sin observabilidad
     • Si dbt-sync falla, no hay alertas
     • No se sabe cuándo fue el último sync exitoso
     • No hay forma de re-procesar un período específico
```

---

## Arquitectura Objetivo

```
┌─────────────────────────────────────────────────────────────────────┐
│                        FUENTES DE DATOS                             │
│                                                                     │
│  ┌────────────┐   ┌───────────────┐   ┌──────────────────┐         │
│  │  Firebird   │   │  MySQL (AWS)  │   │  client-service  │         │
│  │  (ventas)   │   │  (maestros)   │   │  (clientes)      │         │
│  └──────┬──────┘   └───────┬───────┘   └────────┬─────────┘         │
└─────────┼──────────────────┼────────────────────┼───────────────────┘
          │                  │                    │
          ▼                  ▼                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│                    DAGSTER  (sync-service)                           │
│                    UI: http://localhost:3000                         │
│                                                                     │
│   ┌─────────────────────────────────────────────────────────┐       │
│   │  ASSETS DE EXTRACCIÓN (Python)                          │       │
│   │                                                         │       │
│   │  raw_sales ─────────── Firebird/MySQL → raw.raw_sales   │       │
│   │  raw_clients ───────── client-service → raw.raw_clients │       │
│   │  raw_excel_clients ─── client-service → raw.raw_excel   │       │
│   │  raw_sellers ───────── MySQL → raw.raw_sellers          │       │
│   │  raw_price_history ─── Firebird → raw.raw_price_history │       │
│   └────────────────────────────┬────────────────────────────┘       │
│                                │                                    │
│                                ▼                                    │
│   ┌─────────────────────────────────────────────────────────┐       │
│   │  ASSETS DBT (dagster-dbt, auto-generados)               │       │
│   │                                                         │       │
│   │  staging.*          → stg_sales, stg_clients, ...       │       │
│   │  intermediate.*     → int_account_mapping,              │       │
│   │                       int_gm_detection,                 │       │
│   │                       int_price_at_sale, ...            │       │
│   │  analytics.*        → fct_sales, fct_inflation_monthly  │       │
│   └────────────────────────────┬────────────────────────────┘       │
│                                │                                    │
│   SCHEDULES:                   │                                    │
│   • Ventas incremental  ───── cada 30 min                           │
│   • Clientes            ───── cada 10 min                           │
│   • Full sync           ───── diario 3am                            │
│                                │                                    │
│   OBSERVABILIDAD:              │                                    │
│   • Lineage automático         │                                    │
│   • Retries (3x con backoff)   │                                    │
│   • Freshness policies         │                                    │
│   • Alertas Slack              │                                    │
│                                │                                    │
└────────────────────────────────┼────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│                   POSTGRESQL (RDS)                                   │
│                   Base de datos única compartida                     │
│                                                                     │
│   Schema: raw                                                       │
│   ├── raw_sales              (extracción directa)                   │
│   ├── raw_clients                                                   │
│   ├── raw_excel_clients                                             │
│   ├── raw_sellers                                                   │
│   └── raw_price_history                                             │
│                                                                     │
│   Schema: staging            (vistas dbt - normalización)           │
│   ├── stg_sales                                                     │
│   ├── stg_clients                                                   │
│   ├── stg_sellers                                                   │
│   ├── stg_excel_clients                                             │
│   └── stg_price_history                                             │
│                                                                     │
│   Schema: intermediate       (vistas dbt - lógica de negocio)       │
│   ├── int_account_mapping                                           │
│   ├── int_sales_with_vendor                                         │
│   ├── int_gm_detection                                              │
│   ├── int_branch_resolved                                           │
│   ├── int_price_at_sale                                              │
│   ├── int_gm_price_calculated                                       │
│   ├── int_sales_breakdown                                           │
│   ├── int_inflation_prices                                          │
│   └── int_inflation_by_brand                                        │
│                                                                     │
│   Schema: analytics          (tablas dbt - API-ready)               │
│   ├── fct_sales              ← TABLA UNIFICADA                      │
│   │   Índices:                                                      │
│   │   • (YEARMONTH, CODIGOVENDEDOR)                                 │
│   │   • (YEARMONTH, CODIGOMARCA)                                    │
│   │   • (YEARMONTH, MAIN_ACCOUNT_CODE)                              │
│   │   • (YEARMONTH, BRANCH)                                         │
│   └── fct_inflation_monthly                                         │
│                                                                     │
└───────────────────┬─────────────────────────┬───────────────────────┘
                    │                         │
                    │  Misma tabla,            │
                    │  mismos datos,           │
                    │  una sola fuente         │
                    │  de verdad               │
                    │                         │
                    ▼                         ▼
     ┌──────────────────────┐  ┌──────────────────────┐
     │                      │  │                      │
     │   api-vendedores     │  │   api-quantix        │
     │   (Node/TS)          │  │   (Python/FastAPI)   │
     │                      │  │                      │
     │   Sequelize/pg       │  │   SQLAlchemy/pg      │
     │   conecta directo    │  │   conecta directo    │
     │   a PostgreSQL       │  │   a PostgreSQL       │
     │                      │  │                      │
     │   • Ventas           │  │   • Rentabilidad     │
     │   • SR completion    │  │   • Márgenes         │
     │   • Inflación        │  │   • Costos           │
     │   • Reportes         │  │   • Bonificaciones   │
     │                      │  │                      │
     └──────────┬───────────┘  └──────────┬───────────┘
                │                         │
                ▼                         ▼
     ┌──────────────────────┐  ┌──────────────────────┐
     │   app-vendedores     │  │   quantix            │
     │   (React)            │  │   (React)            │
     │   VERSUS UI          │  │   QUANTIX UI         │
     └──────────────────────┘  └──────────────────────┘
```

### Qué se gana

```
 ✅  Una sola fuente de verdad
     • fct_sales en PostgreSQL alimenta ambos sistemas
     • Se elimina RENT_COMERCIAL
     • Se elimina DuckDB + Parquet

 ✅  Lógica unificada
     • GM/DS, cuentas secundarias, inflación: todo en dbt (SQL)
     • No más transformaciones en Python post-query
     • Ambos sistemas ven los mismos números

 ✅  Observabilidad completa
     • Dagster UI: qué corrió, cuándo, qué falló
     • Lineage visual automático desde dbt
     • Freshness policies + alertas

 ✅  PostgreSQL como motor
     • Queries concurrentes sin problemas
     • Conexión nativa desde Node (pg) y Python (psycopg2)
     • Backups, monitoring, escalado via RDS
     • Sin archivos compartidos via Docker volumes

 ✅  Arquitectura simple
     • 1 servicio de sync (Dagster)
     • 1 base de datos (PostgreSQL)
     • 1 cache (Redis, solo para auth y sessions)
     • 2 APIs consumiendo la misma fuente
```

### Qué se elimina

```
 🗑  dbt-sync (Flask sidecar)    → Reemplazado por Dagster
 🗑  DuckDB + archivos Parquet   → Reemplazado por PostgreSQL
 🗑  RENT_COMERCIAL (MySQL)      → Reemplazado por fct_sales (PostgreSQL)
 🗑  DuckDBService (Node)        → Reemplazado por queries PostgreSQL
 🗑  Transformaciones Pandas     → Movidas a dbt (SQL)
 🗑  Docker volume compartido    → Conexión directa a PostgreSQL
```

### Stack final

```
 ┌──────────────────────────────────────────────┐
 │  Infraestructura                             │
 │                                              │
 │  • PostgreSQL 16 (RDS)     ← datos           │
 │  • Redis 7                 ← auth/sessions   │
 │  • Dagster (2 containers)  ← orquestación    │
 │  • dbt-postgres            ← transformación  │
 │                                              │
 ├──────────────────────────────────────────────┤
 │  Aplicaciones                                │
 │                                              │
 │  • api-vendedores (Node)   ← API Versus      │
 │  • api-quantix (Python)    ← API Quantix     │
 │  • app-vendedores (React)  ← UI Versus       │
 │  • quantix (React)         ← UI Quantix      │
 │                                              │
 └──────────────────────────────────────────────┘
```
