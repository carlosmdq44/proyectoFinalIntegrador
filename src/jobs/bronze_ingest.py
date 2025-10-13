"""Bronze ingestion job for Patagonia, Riohacha, and OpenMeteo sources."""
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

BRONZE_BAD_RECORDS = "bronze/_badrecords"


def read_json_source(
    spark,
    path: str,
    source: str,
    ingestion_date: str,
    bad_records_path: str,
) -> DataFrame:
    df = (
        spark.read.option("multiline", "true")
        .option("mode", "PERMISSIVE")
        .option("badRecordsPath", bad_records_path)
        .json(path)
    )
    df = add_metadata_columns(df, source=source, ingestion_date=ingestion_date)
    return df


def read_parquet_source(spark, path: str, source: str, ingestion_date: str) -> DataFrame:
    df = spark.read.parquet(path)
    df = add_metadata_columns(df, source=source, ingestion_date=ingestion_date)
    return df


def write_bronze(df: DataFrame, output_path: str) -> None:
    df_to_write = repartition_if_necessary(df)
    (
        df_to_write.write.mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .partitionBy("ingestion_date", "source")
        .save(output_path)
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args("Bronze ingestion job")
    run_date = ensure_run_date(args.run_date)
    logger = build_logger("bronze_ingest")

    spark = build_spark_session("bronze_ingest")
    base_bucket = args.input_bucket.rstrip("/")

    sources = {
        "patagonia": f"{base_bucket}/raw/patagonia/ingestion_date={run_date}/*",
        "riohacha": f"{base_bucket}/raw/riohacha/ingestion_date={run_date}/*",
    }
    weather_path_parquet = f"{base_bucket}/raw/api/openmeteo/*.parquet"
    weather_path_json = f"{base_bucket}/raw/api/openmeteo/*"

    configure_shuffle_partitions(
        spark,
        paths=[*sources.values(), weather_path_parquet, weather_path_json],
        override=args.shuffle_partitions,
    )

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
        if df.rdd.isEmpty():
            logger.warning("No data found for %s on %s", source, run_date)
            continue
        log_quality_metrics(
            logger,
            f"bronze_{source}",
            df,
            required_columns=["txn_id", "transaction_id", "id"],
        )
        output_path = f"{base_bucket}/bronze/{source}"
        logger.info("Writing %s bronze data to %s", source, output_path)
        write_bronze(df, output_path)
        register_table(spark, f"bronze_{source}", output_path)

    # --- reemplazá todo el bloque de OpenMeteo por este ---
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
