# Retail Analytics Lakehouse

Este proyecto implementa un pipeline de datos estilo medallon (Bronze -> Silver -> Gold) usando PySpark para AWS (EMR/Databricks) sobre el bucket `s3://datalake-henry-dev-us-east-1`.

## Estructura

```
jobs/
  bronze_ingest.py
  silver_transform.py
  gold_analytics.py
conf/
  env.yaml (opcional)
tests/
  test_bronze.py
  test_silver.py
  test_gold.py
requirements.txt
```

## Requerimientos locales

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Ejecucion de los jobs

### Parametros comunes

Todos los scripts aceptan:

- `--input_bucket`: ruta base del Data Lake (por ejemplo `s3://datalake-henry-dev-us-east-1`)
- `--run_date`: fecha a procesar `YYYY-MM-DD`
- `--env`: etiqueta de ambiente (default `dev`)
- `--shuffle_partitions`: override opcional para `spark.sql.shuffle.partitions`

`gold_analytics.py` anade:

- `--default_cost_factor`: factor multiplicador para estimar costo cuando `unit_cost` esta ausente (default `0.7`)
- `--silver_prefix`: prefijo alterno para la capa silver (default `silver`)

### Flujo rapido local (sin tests)

Con PySpark instalado en tu maquina (por ejemplo via `pip install -r requirements.txt`) puedes ejecutar cada etapa directamente con
Python. Abre tu terminal dentro del repositorio y corre:

```bash
export INPUT_BUCKET="s3://datalake-henry-dev-us-east-1"
export RUN_DATE="2025-10-11"

# Bronze
python -m src.jobs.bronze_ingest --input_bucket "$INPUT_BUCKET" --run_date "$RUN_DATE"

# Silver
python -m src.jobs.silver_transform --input_bucket "$INPUT_BUCKET" --run_date "$RUN_DATE" --raw_prefix bronze --silver_prefix silver

# Gold
python -m src.jobs.gold_analytics --input_bucket "$INPUT_BUCKET" --run_date "$RUN_DATE" --default_cost_factor 0.7

```

Cada script creara las tablas/parquets en las rutas correspondientes bajo el bucket indicado. Si deseas repetir el proceso para
otro dia solo cambia la variable `RUN_DATE`.

### Ejemplos EMR

```bash
spark-submit \
  --deploy-mode cluster \
  --conf spark.sql.sources.partitionOverwriteMode=dynamic \
  --conf spark.sql.adaptive.enabled=true \
  s3://<code-bucket>/jobs/bronze_ingest.py \
  --input_bucket s3://datalake-henry-dev-us-east-1 \
  --run_date 2025-10-11

spark-submit ... jobs/silver_transform.py \
  --input_bucket s3://datalake-henry-dev-us-east-1 \
  --run_date 2025-10-11

spark-submit ... jobs/gold_analytics.py \
  --input_bucket s3://datalake-henry-dev-us-east-1 \
  --run_date 2025-10-11 \
  --default_cost_factor 0.7
```

### Databricks

Crear un job o notebook con los mismos parametros (usar `dbutils.widgets` o parametros de job).

## Registro en Glue Data Catalog

Los scripts intentan registrar o actualizar automaticamente las tablas en la base `retail_analytics_dev`:

- `bronze_patagonia`, `bronze_riohacha`, `bronze_openmeteo`
- `silver_transactions`
- `gold_kpi_*` y `gold_fact_sales_daily`

Si el entorno no cuenta con Hive o Glue se imprimira una advertencia.

## Pruebas

```bash
pytest
```

Las pruebas validan la estandarizacion de columnas en Silver, la logica de KPIs clave (revenue, mix, anomalias) y la incorporacion de clima.

## Notas de diseno

- Particionamiento: Bronze por `ingestion_date`, Silver por `txn_date` y `region`, Gold por `txn_date`/`region` o periodo.
- Calidad y observabilidad: logging con contadores basicos y paths escritos.
- Incrementalidad: procesar unicamente particiones de `run_date`, manteniendo `overwrite dynamic`.
- Optimizacion: ajuste dinamico de `spark.sql.shuffle.partitions`, uso de `approx_count_distinct`, broadcast en tablas pequenas y cache de datasets reutilizados.
