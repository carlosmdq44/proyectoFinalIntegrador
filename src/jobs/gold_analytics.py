# src/jobs/gold_analytics.py
"""
Gold analytics job: genera KPIs y tablas de hechos a partir de SILVER.

💡 Cómo contarlo en tu defensa oral (TL;DR):
- **Objetivo del job**: consumir transacciones limpias de SILVER, adjuntar clima y derivar
  KPIs (por categoría, cliente, región, etc.) + la **tabla de hechos** `fact_sales_daily`.
- **Buenas prácticas**: 
  * *Idempotencia por partición* (`partitionOverwriteMode=dynamic`).
  * *Escritura particionada* (por `dt`, fecha de la transacción y dims clave para pruning).
  * *Esquemas tolerantes* (renombres ES→EN, columnas faltantes como NULL) para no romper flujos.
- **Salida**: Parquet en rutas `gold/kpis/*` y `gold/marts/fact_sales_daily` + `register_table` para consulta.
"""
from __future__ import annotations

import sys
from typing import List

from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.utils import AnalysisException

from .utils import (
    build_logger,
    build_spark_session,
    configure_shuffle_partitions,
    ensure_run_date,
    parse_args,
    register_table,
    repartition_if_necessary,
)

# 📁 Rutas destino (por KPI) y tabla de hechos
GOLD_KPI_PATHS = {
    "top_products_by_category": "gold/kpis/top_products_by_category",
    "customer_frequency_ticket": "gold/kpis/customer_frequency_ticket",
    "revenue_by_region": "gold/kpis/revenue_by_region",
    "new_vs_returning": "gold/kpis/new_vs_returning",
    "price_volume_corr": "gold/kpis/price_volume_corr",
    "margin_by_product": "gold/kpis/margin_by_product",
    "channel_payment_perf": "gold/kpis/channel_payment_perf",
    "anomalies": "gold/kpis/anomalies",
    "weather_impact": "gold/kpis/weather_impact",
}

FACT_PATH = "gold/marts/fact_sales_daily"


# -----------------------------
# Helpers de particionado / idempotencia
# -----------------------------
def with_dt(df: DataFrame, run_date: str) -> DataFrame:
    """Agrega columna de partición estándar `dt=YYYY-MM-DD` (día de corrida). Útil para DQ y backfills."""
    return df.withColumn("dt", F.lit(run_date))


def safe_partitions(df: DataFrame) -> DataFrame:
    """
    Evita NULLs en columnas que usaremos como partición. Previene `__HIVE_DEFAULT_PARTITION__`.
    Rellena con valores "sentinela" razonables (ej. "unknown").
    """
    fill_map = {}
    if "txn_date" in df.columns:
        fill_map["txn_date"] = "1970-01-01"
    if "region" in df.columns:
        fill_map["region"] = "unknown"
    if "category" in df.columns:
        fill_map["category"] = "unknown"
    return df.fillna(fill_map)


# -----------------------------
# Lectura de SILVER (schema ES → alias EN)
# -----------------------------
def load_transactions(spark, silver_base: str, run_date: str | None) -> DataFrame:
    """
    Lee transacciones estandarizadas desde SILVER.
    - Path esperado: `silver/transactions/run_date=YYYY-MM-DD/`.
    - Deriva `txn_date = to_date(fecha)` para usar como **fecha de la transacción**.
    - Crea alias comunes para KPIs: `unit_price`, `unit_cost` (si no existe) y renombres ES→EN.
    - Si la partición exacta no existe, intenta lectura global filtrando por `run_date`.
    """
    if run_date:
        path = f"{silver_base}/transactions/run_date={run_date}"
        try:
            df = (
                spark.read
                .option("mergeSchema", "true")
                .parquet(path)
            )
            print(f"[gold] Leyendo transactions desde partición: {path}")
        except AnalysisException:
            print(f"[gold] Partición no encontrada: {path}. Intento sin partición…")
            df = spark.read.option("mergeSchema", "true").parquet(f"{silver_base}/transactions")
            if "run_date" in df.columns:
                df = df.filter(F.col("run_date") == F.lit(run_date))
    else:
        df = spark.read.option("mergeSchema", "true").parquet(f"{silver_base}/transactions")

    # Compat: `fecha` → `txn_date` (DATE) si no existía.
    if "txn_date" not in df.columns and "fecha" in df.columns:
        df = df.withColumn("txn_date", F.to_date(F.col("fecha")))

    # Alias para KPIs
    if "unit_price" not in df.columns and "precio" in df.columns:
        df = df.withColumn("unit_price", F.col("precio"))

    # Renombres comunes ES → EN (evita ifs después en cada KPI)
    rename_map = {
        # ids
        "id": "txn_id",
        "producto_id": "product_id",
        "item_name": "product_name",
        "cliente_id": "customer_id",
        "sucursal_id": "store_id",
        # dims
        "categoria": "category",
        "canal": "channel",
        "metodo_pago": "payment_method",
        # metrics
        "cantidad": "quantity",
        "precio": "unit_price",
    }
    for src, tgt in rename_map.items():
        if src in df.columns and tgt not in df.columns:
            df = df.withColumnRenamed(src, tgt)

    # Costo unitario opcional; si no viene, lo completamos como NULL (lo estimamos en el KPI correspondiente).
    if "unit_cost" not in df.columns:
        df = df.withColumn("unit_cost", F.lit(None).cast("double"))

    return df


# -----------------------------
# KPIs
# -----------------------------
def top_products_by_category(df: DataFrame, base_bucket: str, run_date: str) -> DataFrame:
    """Top 10 productos por categoría (por día) + variación porcentual vs día previo por producto."""
    aggregated = df.groupBy("category", "product_id", "product_name", "txn_date").agg(
        F.sum("revenue").alias("daily_revenue"),
        F.sum("quantity").alias("daily_quantity"),
    )
    window_spec = Window.partitionBy("category", "txn_date").orderBy(F.desc("daily_revenue"))
    trend_window = Window.partitionBy("category", "product_id").orderBy("txn_date")

    ranked = aggregated.withColumn("rank", F.row_number().over(window_spec))
    ranked = ranked.withColumn("prev_revenue", F.lag("daily_revenue").over(trend_window))
    ranked = ranked.withColumn(
        "revenue_change_pct",
        F.when(
            F.col("prev_revenue").isNull() | (F.col("prev_revenue") == 0),
            F.lit(None).cast("double"),
        ).otherwise((F.col("daily_revenue") - F.col("prev_revenue")) / F.col("prev_revenue")),
    ).drop("prev_revenue")

    ranked = ranked.filter(F.col("rank") <= 10)
    output_path = f"{base_bucket}/{GOLD_KPI_PATHS['top_products_by_category']}"
    ranked_to_write = repartition_if_necessary(safe_partitions(with_dt(ranked, run_date)))
    (
        ranked_to_write.write.mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .partitionBy("dt", "txn_date", "category")
        .save(output_path)
    )
    return ranked


def customer_frequency_ticket(df: DataFrame, base_bucket: str, run_date: str) -> DataFrame:
    """Frecuencia mensual por cliente (transacciones, ticket promedio, items comprados) y región."""
    monthly = df.withColumn("txn_month", F.date_trunc("month", "txn_date"))
    metrics = monthly.groupBy("txn_month", "customer_id", "region").agg(
        F.approx_count_distinct("txn_id").alias("transaction_count"),
        F.sum("revenue").alias("total_revenue"),
        F.avg("revenue").alias("avg_ticket"),
        F.sum("quantity").alias("items_purchased"),
    )
    output_path = f"{base_bucket}/{GOLD_KPI_PATHS['customer_frequency_ticket']}"
    metrics_to_write = repartition_if_necessary(safe_partitions(with_dt(metrics, run_date)))
    (
        metrics_to_write.write.mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .partitionBy("dt", "txn_month")
        .save(output_path)
    )
    return metrics


def revenue_by_region(df: DataFrame, base_bucket: str, run_date: str) -> DataFrame:
    """Ingresos y volumen por región para ventanas **semanales** y **mensuales**."""
    weekly = df.withColumn("period_start", F.date_trunc("week", "txn_date"))
    weekly_metrics = weekly.groupBy("period_start", "region").agg(
        F.sum("revenue").alias("weekly_revenue"),
        F.sum("quantity").alias("weekly_quantity"),
        F.approx_count_distinct("txn_id").alias("weekly_orders"),
    ).withColumn("period_type", F.lit("weekly"))

    monthly = df.withColumn("period_start", F.date_trunc("month", "txn_date"))
    monthly_metrics = monthly.groupBy("period_start", "region").agg(
        F.sum("revenue").alias("monthly_revenue"),
        F.sum("quantity").alias("monthly_quantity"),
        F.approx_count_distinct("txn_id").alias("monthly_orders"),
    ).withColumn("period_type", F.lit("monthly"))

    weekly_metrics = weekly_metrics.select(
        "period_type", "period_start", "region", "weekly_revenue", "weekly_quantity", "weekly_orders"
    )
    monthly_metrics = monthly_metrics.select(
        "period_type", "period_start", "region", "monthly_revenue", "monthly_quantity", "monthly_orders"
    )

    combined = weekly_metrics.unionByName(monthly_metrics, allowMissingColumns=True)
    output_path = f"{base_bucket}/{GOLD_KPI_PATHS['revenue_by_region']}"
    combined_to_write = repartition_if_necessary(safe_partitions(with_dt(combined, run_date)))
    (
        combined_to_write.write.mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .partitionBy("dt", "period_type", "period_start")
        .save(output_path)
    )
    return combined


def new_vs_returning(df: DataFrame, base_bucket: str, run_date: str) -> DataFrame:
    """Mix de clientes **nuevos** vs **recurrentes** por mes (ratio incluido)."""
    monthly = df.withColumn("txn_month", F.date_trunc("month", "txn_date"))
    customer_months = monthly.select("customer_id", "txn_month").distinct()
    customer_flags = (
        customer_months.groupBy("customer_id")
        .agg(F.count("txn_month").alias("active_months"))
        .withColumn("is_returning", F.col("active_months") >= F.lit(2))
    )

    month_status = monthly.join(customer_flags, on="customer_id", how="left")
    mix = month_status.groupBy("txn_month").agg(
        F.sum(F.when(F.col("is_returning"), 1).otherwise(0)).alias("returning_customers"),
        F.sum(F.when(~F.col("is_returning"), 1).otherwise(0)).alias("new_customers"),
    ).withColumn(
        "returning_ratio",
        F.col("returning_customers") / F.greatest(F.col("returning_customers") + F.col("new_customers"), F.lit(1)),
    )
    output_path = f"{base_bucket}/{GOLD_KPI_PATHS['new_vs_returning']}"
    mix_to_write = repartition_if_necessary(safe_partitions(with_dt(mix, run_date)))
    (
        mix_to_write.write.mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .partitionBy("dt", "txn_month")
        .save(output_path)
    )
    return mix


def price_volume_corr(df: DataFrame, base_bucket: str, run_date: str) -> DataFrame:
    """Correlación **precio vs volumen** por categoría (+ promedios para contexto)."""
    corr_df = df.groupBy("category").agg(
        F.corr(F.col("unit_price"), F.col("quantity")).alias("price_volume_corr"),
        F.avg("unit_price").alias("avg_unit_price"),
        F.avg("quantity").alias("avg_quantity"),
    )
    output_path = f"{base_bucket}/{GOLD_KPI_PATHS['price_volume_corr']}"
    corr_to_write = repartition_if_necessary(with_dt(corr_df, run_date))
    (
        corr_to_write.write.mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .partitionBy("dt")
        .save(output_path)
    )
    return corr_df


def margin_by_product(df: DataFrame, base_bucket: str, default_cost_factor: float, run_date: str) -> DataFrame:
    """Margen por producto. Si `unit_cost` no viene, lo **estimamos** con `unit_price * default_cost_factor`."""
    if "unit_cost" not in df.columns:
        df = df.withColumn("unit_cost", F.lit(None).cast("double"))

    # Estimación de costo unitario cuando falta (p. ej. factor = 0.7 → costo ~70% del precio)
    cost = F.when(F.col("unit_cost").isNotNull(), F.col("unit_cost")) \
        .otherwise(F.col("unit_price") * F.lit(default_cost_factor))

    metrics = df.groupBy("product_id", "product_name", "category").agg(
        F.sum("revenue").alias("total_revenue"),
        F.sum("quantity").alias("items_sold"),
        F.sum(cost * F.col("quantity")).alias("total_cost"),
    )
    metrics = metrics.withColumn("margin", F.col("total_revenue") - F.col("total_cost"))
    output_path = f"{base_bucket}/{GOLD_KPI_PATHS['margin_by_product']}"
    metrics_to_write = repartition_if_necessary(safe_partitions(with_dt(metrics, run_date)))
    (
        metrics_to_write.write.mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .partitionBy("dt", "category")
        .save(output_path)
    )
    return metrics


def channel_payment_perf(df: DataFrame, base_bucket: str, run_date: str) -> DataFrame:
    """Performance por **canal** y **método de pago** (ingresos, items, órdenes) por día y región."""
    metrics = df.groupBy("channel", "payment_method", "region", "txn_date").agg(
        F.sum("revenue").alias("revenue"),
        F.sum("quantity").alias("items_sold"),
        F.approx_count_distinct("txn_id").alias("orders"),
    )
    output_path = f"{base_bucket}/{GOLD_KPI_PATHS['channel_payment_perf']}"
    metrics_to_write = repartition_if_necessary(safe_partitions(with_dt(metrics, run_date)))
    (
        metrics_to_write.write.mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .partitionBy("dt", "txn_date", "region")
        .save(output_path)
    )
    return metrics


def anomalies(df: DataFrame, base_bucket: str, run_date: str) -> DataFrame:
    """Detección simple de anomalías por z‑score (≥3σ) en revenue diario por región/categoría."""
    daily = df.groupBy("txn_date", "region", "category").agg(
        F.sum("revenue").alias("daily_revenue"),
        F.sum("quantity").alias("daily_quantity"),
    )
    window_spec = (
        Window.partitionBy("region", "category")
        .orderBy("txn_date")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    with_stats = daily.withColumn("mean_revenue", F.avg("daily_revenue").over(window_spec)).withColumn(
        "std_revenue", F.stddev("daily_revenue").over(window_spec)
    )
    anomalies_df = with_stats.withColumn(
        "z_score",
        (F.col("daily_revenue") - F.col("mean_revenue")) / F.col("std_revenue"),
    ).withColumn("is_anomaly", F.abs(F.col("z_score")) >= F.lit(3))
    output_path = f"{base_bucket}/{GOLD_KPI_PATHS['anomalies']}"
    anomalies_to_write = repartition_if_necessary(safe_partitions(with_dt(anomalies_df, run_date)))
    (
        anomalies_to_write.write.mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .partitionBy("dt", "txn_date", "region")
        .save(output_path)
    )
    return anomalies_df


def weather_impact(df: DataFrame, base_bucket: str, run_date: str) -> DataFrame:
    """Impacto del clima: correlación temperatura/precipitación vs revenue por región (promedios incluidos)."""
    # Asume que df ya tiene columnas de clima (`attach_weather`).
    metrics = df.groupBy("region").agg(
        F.corr("weather_avg_temp", "revenue").alias("corr_temp_revenue"),
        F.corr("weather_precipitation", "revenue").alias("corr_precip_revenue"),
        F.avg("weather_avg_temp").alias("avg_temp"),
        F.avg("weather_precipitation").alias("avg_precip"),
    )
    output_path = f"{base_bucket}/{GOLD_KPI_PATHS['weather_impact']}"
    metrics_to_write = repartition_if_necessary(with_dt(metrics, run_date))
    (
        metrics_to_write.write.mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .partitionBy("dt")
        .save(output_path)
    )
    return metrics


def fact_sales_daily(df: DataFrame, base_bucket: str, run_date: str) -> DataFrame:
    """
    Tabla de hechos **diaria** por (fecha, región, tienda, categoría), con KPIs habituales:
    - orders, items_sold, revenue, unique_customers
    - métricas de clima (promedios y código del día)
    - `avg_ticket` (= revenue / orders, con guardia de división por 0)
    - `channel_mix` y `payment_mix` como mapas {canal→revenue} y {método→revenue}
    """
    fact = df.groupBy("txn_date", "region", "store_id", "category`).agg(
        F.approx_count_distinct("txn_id").alias("orders"),
        F.sum("quantity").alias("items_sold"),
        F.sum("revenue").alias("revenue"),
        F.approx_count_distinct("customer_id").alias("unique_customers"),
        F.avg("weather_avg_temp").alias("weather_avg_temp"),
        F.avg("weather_precipitation").alias("weather_precipitation"),
        F.first("weather_code", ignorenulls=True).alias("weather_code"),
        F.avg("weather_wind_speed").alias("weather_wind_speed"),
        F.avg("weather_humidity").alias("weather_humidity"),
    )

    fact = fact.withColumn(
        "avg_ticket",
        F.when(F.col("orders") > 0, F.col("revenue").cast("double") / F.col("orders")).otherwise(F.lit(0.0)),
    )

    # Mezcla por canal (map) → útil para dashboards sin pivotar previamente.
    channel_mix = df.groupBy("txn_date", "region", "store_id", "category", "channel").agg(
        F.sum("revenue").alias("channel_revenue")
    )
    channel_mix = channel_mix.groupBy("txn_date", "region", "store_id", "category").agg(
        F.map_from_entries(F.collect_list(F.struct("channel", "channel_revenue"))).alias("channel_mix")
    )

    # Mezcla por método de pago (map)
    payment_mix = df.groupBy("txn_date", "region", "store_id", "category", "payment_method").agg(
        F.sum("revenue").alias("payment_revenue")
    )
    payment_mix = payment_mix.groupBy("txn_date", "region", "store_id", "category").agg(
        F.map_from_entries(F.collect_list(F.struct("payment_method", "payment_revenue"))).alias("payment_mix")
    )

    # Join de mixes
    fact = fact.join(channel_mix, ["txn_date", "region", "store_id", "category"], "left")
    fact = fact.join(payment_mix, ["txn_date", "region", "store_id", "category"], "left")

    # Escritura particionada
    output_path = f"{base_bucket}/{FACT_PATH}"
    fact_to_write = repartition_if_necessary(safe_partitions(with_dt(fact, run_date)))
    (
        fact_to_write.write.mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .partitionBy("dt", "txn_date", "region")
        .save(output_path)
    )
    return fact


# -----------------------------
# Main (orquestación del job)
# -----------------------------
def main(argv: List[str] | None = None) -> int:
    def additional(parser):
        # Parámetros específicos de GOLD
        parser.add_argument("--default_cost_factor", type=float, default=0.7)
        parser.add_argument("--silver_prefix", default="silver")

    # Parseo de args y utilidades comunes (logger, spark, run_date, etc.)
    args = parse_args("Gold analytics job", extra_args=[additional])
    run_date = ensure_run_date(args.run_date)
    logger = build_logger("gold_analytics")

    spark = build_spark_session("gold_analytics")

    # ✅ Idempotencia por partición: sólo sobreescribimos la partición escrita
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    base_bucket = args.input_bucket.rstrip("/")

    # Ajuste de particiones de shuffle según tamaño de entrada (mejora performance y evita skew)
    configure_shuffle_partitions(
        spark,
        paths=[f"{base_bucket}/{args.silver_prefix}/transactions/run_date={run_date}"],
        override=args.shuffle_partitions,
    )

    # 1) Carga de SILVER
    transactions = load_transactions(spark, f"{base_bucket}/{args.silver_prefix}", run_date)

    # 2) Carga y **adjunto** de clima (left join por fecha y, si existe, región)
    weather = load_weather(spark, f"{base_bucket}/{args.silver_prefix}", run_date)
    transactions = attach_weather(transactions, weather)

    # Cache para reutilizar el mismo DF en todos los KPIs
    transactions.cache()
    logger.info(
        "Loaded %s rows from silver transactions (with weather columns: %s)",
        transactions.count(),
        ", ".join([c for c in ["weather_avg_temp", "weather_precipitation", "weather_code"]
                   if c in transactions.columns])
    )

    # 3) Construcción de KPIs (cada función escribe su salida y devuelve un DF para logging/validación)
    outputs = {}
    outputs["top_products_by_category"] = top_products_by_category(transactions, base_bucket, run_date)
    outputs["customer_frequency_ticket"] = customer_frequency_ticket(transactions, base_bucket, run_date)
    outputs["revenue_by_region"] = revenue_by_region(transactions, base_bucket, run_date)
    outputs["new_vs_returning"] = new_vs_returning(transactions, base_bucket, run_date)
    outputs["price_volume_corr"] = price_volume_corr(transactions, base_bucket, run_date)
    outputs["margin_by_product"] = margin_by_product(
        transactions, base_bucket, default_cost_factor=args.default_cost_factor, run_date=run_date
    )
    outputs["channel_payment_perf"] = channel_payment_perf(transactions, base_bucket, run_date)
    outputs["anomalies"] = anomalies(transactions, base_bucket, run_date)
    outputs["weather_impact"] = weather_impact(transactions, base_bucket, run_date)

    # 4) Tabla de hechos
    fact_df = fact_sales_daily(transactions, base_bucket, run_date)

    # 5) Registro de tablas (metastore/Glue/Athena) para consulta directa
    for name, df in outputs.items():
        table_name = f"gold_kpi_{name}"
        register_table(spark, table_name, f"{base_bucket}/{GOLD_KPI_PATHS[name]}")
        logger.info("KPI %s generated with %s records", name, df.count())

    register_table(spark, "gold_fact_sales_daily", f"{base_bucket}/{FACT_PATH}")
    logger.info("Fact table generated with %s records", fact_df.count())

    return 0


# -----------------------------
# Lectura de weather desde SILVER y join con transacciones
# -----------------------------
def load_weather(spark, silver_base: str, run_date: str) -> DataFrame | None:
    """Lee la partición diaria de clima. Si no está, sigue sin clima (robusto a faltantes)."""
    path = f"{silver_base}/weather_daily/run_date={run_date}"
    try:
        df = (spark.read
              .option("mergeSchema", "true")
              .parquet(path))
        print(f"[gold] Leyendo weather desde partición: {path}")
        return df
    except AnalysisException:
        print(f"[gold] Weather no encontrado en {path}. Seguimos sin clima.")
        return None


def attach_weather(transactions: DataFrame, weather: DataFrame | None) -> DataFrame:
    """
    Adjunta columnas de clima a las transacciones por **fecha** y, si existe, por **región**.
    Si `weather` es None, crea columnas de clima como NULL para no romper KPIs que las usan.
    """
    if weather is None:
        df = transactions
        for name, dtype in [("weather_avg_temp", "double"),
                            ("weather_precipitation", "double"),
                            ("weather_code", "string"),
                            ("weather_wind_speed", "double"),
                            ("weather_humidity", "double")]:
            if name not in df.columns:
                df = df.withColumn(name, F.lit(None).cast(dtype))
        return df

    desired = ["weather_avg_temp", "weather_precipitation", "weather_code",
               "weather_wind_speed", "weather_humidity"]
    base_cols = ["weather_date"]
    # Si hay 'region' en weather, la usamos en el join.
    if "region" in weather.columns:
        base_cols = ["region", "weather_date"]

    existing_weather_cols = [c for c in desired if c in weather.columns]
    w = weather.select(*([c for c in base_cols if c in weather.columns] + existing_weather_cols)).alias("w")
    t = transactions.alias("t")

    # Detección rápida: ¿hay alguna región no nula en weather?
    join_on_region = "region" in w.columns and w.filter(F.col("region").isNotNull()).limit(1).count() > 0

    if join_on_region:
        cond = (F.col("t.region") == F.col("w.region")) & (F.col("t.txn_date") == F.col("w.weather_date"))
    else:
        # Fallback si weather.region está vacío: join solo por fecha
        cond = (F.col("t.txn_date") == F.col("w.weather_date"))

    df = (
        t.join(w, cond, how="left")
        .select(
            F.col("t.*"),
            *[F.col(f"w.{c}") for c in existing_weather_cols],
        )
    )

    # Garantizamos columnas de clima aunque falte alguna
    for c in desired:
        if c not in df.columns:
            dtype = "double" if c != "weather_code" else "string"
            df = df.withColumn(c, F.lit(None).cast(dtype))

    return df


if __name__ == "__main__":
    sys.exit(main())
