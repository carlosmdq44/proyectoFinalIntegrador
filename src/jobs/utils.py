"""Utility helpers shared across ETL jobs."""
from __future__ import annotations

import argparse
import logging
import math
from datetime import datetime
from typing import Iterable, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession, functions as F  # type: ignore
from pyspark.sql.types import DecimalType  # type: ignore
from pyspark.sql.utils import AnalysisException

LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
DEFAULT_DATABASE = "retail_analytics_dev"


def build_logger(name: str) -> logging.Logger:
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    return logging.getLogger(name)


def build_spark_session(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")  # fuerza modo local dentro del contenedor
        # Networking del driver para Docker
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.host", "127.0.0.1")
        # (opcional) desactivar UI si molesta
        .config("spark.ui.enabled", "false")
        # S3A
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        # Usa credenciales de env (o cambiá por ProfileCredentialsProvider si montás ~/.aws)
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
    )
    return builder.getOrCreate()


def configure_shuffle_partitions(
        spark: SparkSession,
        paths: Sequence[str] | None = None,
        override: Optional[int] = None,
) -> int:
    """Configure shuffle partitions following rule max(200, input_mb/64)."""
    if override is not None and override > 0:
        spark.conf.set("spark.sql.shuffle.partitions", int(override))
        return int(override)

    total_mb = 0.0
    if paths:
        for path in paths:
            if not path:
                continue
            try:
                fs = (
                    spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                        spark._jvm.java.net.URI(path), spark._jsc.hadoopConfiguration()
                    )
                )
                hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(path)
                if fs.exists(hadoop_path):
                    size = fs.getContentSummary(hadoop_path).getLength()
                    total_mb += size / (1024 * 1024)
            except Exception:
                # Fall back silently if we cannot inspect the path (e.g., S3 without creds)
                continue
    partitions = max(200, math.ceil(total_mb / 64.0) if total_mb else 200)
    spark.conf.set("spark.sql.shuffle.partitions", partitions)
    return partitions


def parse_args(description: str, extra_args: Optional[Iterable] = None):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--input_bucket", required=True, help="Base S3 bucket for the data lake")
    parser.add_argument("--run_date", required=False, help="Execution date (YYYY-MM-DD)")
    parser.add_argument("--env", default="dev", help="Deployment environment tag")
    parser.add_argument(
        "--shuffle_partitions",
        type=int,
        default=None,
        help="Optional manual override for spark.sql.shuffle.partitions",
    )
    if extra_args:
        for extra in extra_args:
            extra(parser)
    return parser.parse_args()


def add_metadata_columns(df: DataFrame, source: str, ingestion_date: str) -> DataFrame:
    return (
        df.withColumn("source", F.lit(source))
        .withColumn("ingestion_date", F.lit(ingestion_date))
        .withColumn("load_ts", F.current_timestamp())
        .withColumn("file_name", F.input_file_name())
    )


def log_quality_metrics(
        logger: logging.Logger,
        label: str,
        df: DataFrame,
        required_columns: Optional[Sequence[str]] | None = None,
) -> None:
    count = df.count()
    partitions = df.rdd.getNumPartitions()
    logger.info("%s - records: %s partitions: %s", label, count, partitions)

    if required_columns:
        metrics = {}
        aggregations = []
        for column in required_columns:
            if column in df.columns:
                aggregations.append(
                    F.count(F.when(F.col(column).isNull(), 1)).alias(column)
                )
        if aggregations:
            null_counts = df.agg(*aggregations).collect()[0].asDict()
            metrics.update({k: v for k, v in null_counts.items() if v})
        if metrics:
            logger.info("%s - null counts: %s", label, metrics)


def first_existing_column(df: DataFrame, candidates: Sequence[str]) -> Optional[str]:
    for column in candidates:
        if column in df.columns:
            return column
    return None


def register_table(
        spark: SparkSession,
        table_name: str,
        location: str,
        database: str = DEFAULT_DATABASE,
) -> None:
    logger = build_logger("register_table")
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{database}`")
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS `{database}`.`{table_name}`
            USING PARQUET
            LOCATION '{location}'
            """
        )

        # Intentar descubrir particiones; si la tabla NO es particionada, lo ignoramos sin romper.
        try:
            spark.sql(f"MSCK REPAIR TABLE `{database}`.`{table_name}`")
        except AnalysisException as e:
            msg = str(e).upper()
            if "NOT_A_PARTITIONED_TABLE" in msg or "PARTITION" in msg:
                logger.warning(
                    "Skipping MSCK for non-partitioned table %s.%s (%s)",
                    database, table_name, e,
                )
            else:
                # Otros errores de metastore: log suave y seguimos
                logger.warning(
                    "MSCK failed for %s.%s (%s)",
                    database, table_name, e,
                )

    except Exception as exc:  # pragma: no cover - metastore optional during tests
        logger.warning("Unable to register table %s.%s: %s", database, table_name, exc)


def safe_decimal_column(col: F.Column, precision: int = 18, scale: int = 2) -> F.Column:
    return col.cast(DecimalType(precision, scale))


def ensure_run_date(run_date: Optional[str]) -> str:
    if not run_date:
        return datetime.utcnow().strftime("%Y-%m-%d")
    return run_date


def repartition_if_necessary(df: DataFrame, target_file_mb: int = 128) -> DataFrame:
    """Repartition dataframe to aim for roughly target file sizes."""
    try:
        plan = df._jdf.logicalPlan()
        stats = df.sparkSession._jsparkSession.sessionState().executePlan(plan).optimizedPlan().stats()
        size_in_bytes = getattr(stats, "sizeInBytes", None)
        if callable(size_in_bytes):
            size_in_bytes = size_in_bytes()
        if size_in_bytes is None:
            return df
    except Exception:
        return df
    if size_in_bytes <= 0:
        return df
    partitions = max(1, math.ceil((size_in_bytes / (1024 * 1024)) / target_file_mb))
    return df.repartition(partitions)
