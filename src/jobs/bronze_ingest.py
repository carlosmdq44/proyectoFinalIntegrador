# src/jobs/bronze_ingest.py
"""
Bronze ingestion job: ingesta tolerante desde **Patagonia**, **Riohacha** y **OpenMeteo** hacia BRONZE.

💡 Cómo contarlo en tu defensa oral (TL;DR):
- **Objetivo del job**: tomar los crudos (JSON/Parquet) y normalizarlos mínimamente con metadatos estándar
  (`source`, `ingestion_date`), guardando en **Parquet** particionado (rendimiento + gobernanza).
- **Robustez**: lectura **permissive** con `badRecordsPath` para no romper ante registros corruptos.
- **Idempotencia**: particionamos por `ingestion_date` para re‑correr un día sin afectar otros.
- **Flexibilidad**: OpenMeteo intenta **Parquet** y cae a **JSON** si no está; Patagonia/Riohacha leen JSON multiline.
"""
from __future__ import annotations

import sys

from pyspark.sql import DataFrame

from .utils import (
    add_metadata_columns,
    build_logger,
    build_spark_session,
    configure_shuffle_partitions,
    ensure_run_date,
    log_quality_metrics,
    parse_args,
    register_table,
    repartition_if_necessary,
)

# Carpeta común para recolectar registros problemáticos (útil para auditoría/QA)
BRONZE_BAD_RECORDS = "bronze/_badrecords"


def read_json_source(
    spark,
    path: str,
    source: str,
    ingestion_date: str,
    bad_records_path: str,
) -> DataFrame:
    """Lee JSON de forma **tolerante** y anota metadatos (`source`, `ingestion_date`).

    - `multiline=true`: soporta JSON con saltos de línea (muy común en dumps).
    - `mode=PERMISSIVE`: no falla por registros mal formados; los deriva a `badRecordsPath`.
    - `add_metadata_columns`: agrega las columnas estándar para gobernanza y particionamiento.
    """
    df = (
        spark.read.option("multiline", "true")
        .option("mode", "PERMISSIVE")
        .option("badRecordsPath", bad_records_path)
        .json(path)
    )
    df = add_metadata_columns(df, source=source, ingestion_date=ingestion_date)
    return df


def read_parquet_source(spark, path: str, source: str, ingestion_date: str) -> DataFrame:
    """Lee Parquet y agrega metadatos estándar."""
    df = spark.read.parquet(path)
    df = add_metadata_columns(df, source=source, ingestion_date=ingestion_date)
    return df


def write_bronze(df: DataFrame, output_path: str) -> None:
    """Escritura particionada en BRONZE con compresión **snappy**.

    - `repartition_if_necessary`: evita demasiadas/escasas particiones de salida (tuning simple).
    - `partitionBy('ingestion_date','source')`: diseño de particionado estable para backfills y
      lectura selectiva (Athena/Trino/Glue).
    - `mode=overwrite`: idempotente por partición cuando se combina con particionamiento.
    """
    df_to_write = repartition_if_necessary(df)
    (
        df_to_write.write.mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .partitionBy("ingestion_date", "source")
        .save(output_path)
    )


def main(argv: list[str] | None = None) -> int:
    """Orquestación de la ingesta BRONZE.

    Pasos:
      1) Parseo de argumentos y setup de Spark/Logger.
      2) Construcción de rutas a **raw/** para cada fuente.
      3) Ajuste de `spark.sql.shuffle.partitions` según tamaño de entrada.
      4) Lectura/validación Patagonia & Riohacha (JSON → BRONZE/Parquet particionado).
      5) Lectura OpenMeteo (Parquet si existe; si no, JSON permissive) → BRONZE.
      6) Registro de tablas en el metastore para consulta directa desde SQL.
    """
    args = parse_args("Bronze ingestion job")
    run_date = ensure_run_date(args.run_date)
    logger = build_logger("bronze_ingest")

    spark = build_spark_session("bronze_ingest")
    base_bucket = args.input_bucket.rstrip("/")

    # Rutas de crudos (día particionado en raw/ingestion_date=YYYY-MM-DD)
    sources = {
        "patagonia": f"{base_bucket}/raw/patagonia/ingestion_date={run_date}/*",
        "riohacha": f"{base_bucket}/raw/riohacha/ingestion_date={run_date}/*",
    }
    # OpenMeteo puede venir como Parquet (preferido) o JSON (fallback)
    weather_path_parquet = f"{base_bucket}/raw/api/openmeteo/*.parquet"
    weather_path_json = f"{base_bucket}/raw/api/openmeteo/*"

    # Tuning de shuffle: usa tamaños reales de entrada para elegir particiones (evita skew/underutilization)
    configure_shuffle_partitions(
        spark,
        paths=[*sources.values(), weather_path_parquet, weather_path_json],
        override=args.shuffle_partitions,
    )

    # --- Patagonia & Riohacha --------------------------------------------------------
    for source, path in sources.items():
        logger.info("Reading %s from %s", source, path)
        bad_records = f"{base_bucket}/{BRONZE_BAD_RECORDS}/{source}/ingestion_date={run_date}"
        df = read_json_source(
            spark,
            path,
            source=source,
            ingestion_date=run_date,
            bad_records_path=bad_records,
        )
        # Guard: si está vacío, no forzamos escritura ni registramos tabla.
        if df.rdd.isEmpty():
            logger.warning("No data found for %s on %s", source, run_date)
            continue

        # Métricas de calidad mínimas: presencia de algún identificador de transacción
        log_quality_metrics(
            logger,
            f"bronze_{source}",
            df,
            required_columns=["txn_id", "transaction_id", "id"],
        )

        # Escritura en BRONZE particionada + registro de tabla
        output_path = f"{base_bucket}/bronze/{source}"
        logger.info("Writing %s bronze data to %s", source, output_path)
        write_bronze(df, output_path)
        register_table(spark, f"bronze_{source}", output_path)

    # --- OpenMeteo -------------------------------------------------------------------
    # Preferimos Parquet (schema estable y eficiente); si no está, caemos a JSON permissive.
    logger.info("Processing OpenMeteo (trying Parquet) from %s", weather_path_parquet)
    try:
        weather_df = read_parquet_source(
            spark, weather_path_parquet, source="openmeteo", ingestion_date=run_date
        )
    except Exception as e:
        logger.warning("Parquet not found or unreadable, falling back to JSON: %s", str(e))
        bad_records = f"{base_bucket}/{BRONZE_BAD_RECORDS}/openmeteo/ingestion_date={run_date}"
        logger.info("Processing OpenMeteo (JSON) from %s", weather_path_json)
        weather_df = read_json_source(
            spark,
            path=weather_path_json,
            source="openmeteo",
            ingestion_date=run_date,
            bad_records_path=bad_records,
        )

    if not weather_df.rdd.isEmpty():
        # Para clima, validamos la existencia de una **columna temporal** típica
        log_quality_metrics(logger, "bronze_openmeteo", weather_df, required_columns=["time", "timestamp"])
        output_path = f"{base_bucket}/bronze/openmeteo"
        write_bronze(weather_df, output_path)
        register_table(spark, "bronze_openmeteo", output_path)
    else:
        logger.warning("No OpenMeteo data found under %s or %s", weather_path_parquet, weather_path_json)

    logger.info("Bronze ingestion completed for %s", run_date)
    return 0


if __name__ == "__main__":
    sys.exit(main())

