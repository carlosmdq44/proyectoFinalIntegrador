# src/jobs/silver_transform.py
"""Silver transformations: standardize_transactions & prepare_weather_features."""
from __future__ import annotations

import argparse, sys
from pyspark.sql import SparkSession, DataFrame, functions as F, types as T
from pyspark.sql.utils import AnalysisException
from pyspark.sql import functions as F, types as T

# --- Normalización de transacciones ----------------------------------------------------
STANDARD_COLS = [
    "id","fecha","sucursal_id","region","producto_id","item_name","categoria",
    "cantidad","precio","revenue","cliente_id","canal","metodo_pago","load_ts",
]

RENAME_MAP = {
    # ids
    "transaction_id":"id","txn_id":"id","id_transaccion":"id","id":"id",
    "customer_id":"cliente_id","client_id":"cliente_id","id_cliente":"cliente_id",
    "product_id":"producto_id","sku":"producto_id","id_producto":"producto_id",
    "store_id":"sucursal_id","tienda_id":"sucursal_id","shop_id":"sucursal_id",
    # fecha/tiempo
    "transaction_ts":"fecha","txn_ts":"fecha","datetime":"fecha",
    "txn_date":"fecha","date":"fecha","timestamp":"fecha",
    # nombres
    "product_name":"item_name","item":"item_name","item_name":"item_name",
    # cate/canal
    "category":"categoria","categoria":"categoria",
    "channel":"canal","canal":"canal",
    # métricas
    "quantity":"cantidad","qty":"cantidad","cantidad":"cantidad",
    "unit_price":"precio","price":"precio","precio":"precio",
    # método pago
    "payment_method":"metodo_pago","metodo_pago":"metodo_pago",
    # región
    "zona":"region","region":"region",
    # carga
    "load_ts":"load_ts",
}
RENAME_MAP.update({"revenue":"revenue","amount":"revenue","importe_total":"revenue"})

WEATHER_REGION_CANDIDATES = ["region", "city", "name", "location"]
WEATHER_TIME_CANDIDATES   = ["time", "timestamp", "datetime", "date"]

# columnas “horarias” típicas de Open-Meteo (arrays)
HOURLY_ARRAY_CANDIDATES = [
    ("temperature_2m", "weather_avg_temp"),
    ("precipitation", "weather_precipitation"),
    ("wind_speed_10m", "weather_wind_speed"),
    ("relative_humidity_2m", "weather_humidity"),
    ("weathercode", "weather_code"),
]

# columnas “diarias” (escalares) ya agregadas (strings o numéricos)
DAILY_SCALAR_CANDIDATES = {
    "temperature_2m_mean": "weather_avg_temp",
    "temperature_mean": "weather_avg_temp",
    "temp_mean": "weather_avg_temp",
    "avg_temp": "weather_avg_temp",
    "precipitation": "weather_precipitation",
    "precipitation_sum": "weather_precipitation",
    "precip": "weather_precipitation",
    "weathercode": "weather_code",
    "weather_code": "weather_code",
    "condition_code": "weather_code",
}

def _first_existing_column(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def _normalize_columns(df: DataFrame) -> DataFrame:
    exprs = []
    for c in df.columns:
        tgt = RENAME_MAP.get(c, RENAME_MAP.get(c.lower(), c))
        exprs.append(F.col(c).alias(tgt))
    return df.select(*exprs)

def standardize_transactions(df: DataFrame, source_name: str) -> DataFrame:
    df = _normalize_columns(df)
    types_map = {
        "id":T.StringType(),"fecha":T.TimestampType(),"sucursal_id":T.StringType(),
        "region":T.StringType(),"producto_id":T.StringType(),"item_name":T.StringType(),
        "categoria":T.StringType(),"cantidad":T.IntegerType(),"precio":T.DoubleType(),
        "cliente_id":T.StringType(),"canal":T.StringType(),"metodo_pago":T.StringType(),
        "load_ts":T.TimestampType(),"revenue":T.DoubleType(),
    }
    for name, dtype in types_map.items():
        if name not in df.columns:
            df = df.withColumn(name, F.lit(None).cast(dtype))
        else:
            df = df.withColumn(name, F.col(name).cast(dtype))
    df = df.withColumn(
        "fecha",
        F.when(F.col("fecha").cast("timestamp").isNotNull(), F.col("fecha").cast("timestamp"))
         .otherwise(F.to_timestamp("fecha"))
    )
    return df.select(*STANDARD_COLS)

# --- Clima ---------------------------------------------------------------------------
WEATHER_REGION_CANDIDATES = ["region","city","name","location"]
WEATHER_TIME_CANDIDATES   = ["time","timestamp","datetime","date"]
WEATHER_TEMP_CANDIDATES   = ["temperature_2m_mean","temperature_mean","temp_mean","avg_temp"]
WEATHER_PRECIP_CANDIDATES = ["precipitation","precipitation_sum","precip"]
WEATHER_CODE_CANDIDATES   = ["weathercode","weather_code","condition_code"]

def _first_existing_column(df: DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None

def prepare_weather_features(df: DataFrame) -> DataFrame:
    # 1) Region
    region_col = _first_existing_column(df, WEATHER_REGION_CANDIDATES) or "region"
    if region_col not in df.columns:
        df = df.withColumn("region", F.lit(None).cast("string"))

    # 2) Columna temporal
    time_col = _first_existing_column(df, WEATHER_TIME_CANDIDATES)

    # ---------- Caso A: dataset HORARIO (arrays) ----------
    # time es array<string> y las métricas también suelen venir como arrays del mismo largo
    if time_col and dict(df.dtypes).get(time_col) and dict(df.dtypes)[time_col].startswith("array"):
        # armamos el zip de arrays existentes
        zipped_cols = [F.col(time_col).alias("time")]
        present_hourly = []
        for src, _alias in HOURLY_ARRAY_CANDIDATES:
            if src in df.columns and dict(df.dtypes)[src].startswith("array"):
                zipped_cols.append(F.col(src).alias(src))
                present_hourly.append((src, _alias))

        zipped = F.arrays_zip(*zipped_cols)  # -> array<struct<time:..., temperature_2m:..., ...>>

        exploded = (
            df
            .select(F.col(region_col).alias("region"), F.explode(zipped).alias("z"))
            .withColumn("weather_ts", F.to_timestamp(F.col("z.time")))
            .withColumn("weather_date", F.to_date(F.col("weather_ts")))
        )

        # seleccionar columnas disponibles ya alineadas
        sel = [
            "region",
            "weather_date",
        ]
        agg_exprs = []

        for src, tgt in present_hourly:
            # promedio diario de las horas disponibles
            sel.append(F.col(f"z.{src}").alias(tgt))
            if tgt == "weather_code":
                agg_exprs.append(F.first(F.col(tgt)).alias(tgt))
            else:
                agg_exprs.append(F.avg(F.col(tgt)).alias(tgt))

        hourly_flat = exploded.select(*sel)

        return (
            hourly_flat
            .groupBy("region", "weather_date")
            .agg(*agg_exprs)
            .distinct()
        )

    # ---------- Caso B: dataset DIARIO (escalares) ----------
    # time es string/timestamp/date O no existe y usamos ingestion_date
    if time_col:
        df = df.withColumn("weather_date", F.to_date(F.col(time_col)))
    else:
        df = df.withColumn("weather_date", F.to_date("ingestion_date"))

    # mapear disponibles
    selects = [F.col(region_col).alias("region"), "weather_date"]
    agg_exprs = {}
    for src, tgt in DAILY_SCALAR_CANDIDATES.items():
        if src in df.columns:
            selects.append(F.col(src).alias(tgt))
            agg_exprs[tgt] = "first" if tgt == "weather_code" else "avg"

    daily = df.select(*selects)

    # si no hay métricas, igualmente devolvemos estructura mínima
    if not agg_exprs:
        return daily.select("region", "weather_date") \
                    .withColumn("weather_avg_temp", F.lit(None).cast("double")) \
                    .withColumn("weather_precipitation", F.lit(None).cast("double")) \
                    .withColumn("weather_code", F.lit(None).cast("string")) \
                    .distinct()

    # agregamos (first para códigos, avg para numéricos)
    agg_list = [
        (F.first(F.col(col)).alias(col) if fn == "first" else F.avg(F.col(col)).alias(col))
        for col, fn in agg_exprs.items()
    ]
    return (
        daily.groupBy("region", "weather_date")
        .agg(*agg_list)
        .distinct()
    )

# --- Utilidades de IO ----------------------------------------------------------------
def _safe_read_parquet(spark, path: str) -> DataFrame | None:
    try:
        df = spark.read.option("mergeSchema","true").parquet(path)
        print(f"[silver] OK leer: {path}")
        return df
    except AnalysisException:
        print(f"[silver] No existe: {path}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Silver transform (lee de BRONZE y escribe SILVER).")
    parser.add_argument("--input_bucket", required=True, help="s3a://datalake-henry-dev-us-east-1")
    parser.add_argument("--run_date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--raw_prefix", default="bronze")   # por defecto BRONZE
    parser.add_argument("--silver_prefix", default="silver")
    parser.add_argument("--src_patagonia", default="patagonia")
    parser.add_argument("--src_riohacha", default="riohacha")
    parser.add_argument("--src_openmeteo", default="openmeteo")
    parser.add_argument("--transactions_table", default="transactions")
    parser.add_argument("--weather_table", default="weather_daily")
    args = parser.parse_args()

    print(f"[silver] args: {args}")

    spark = (
        SparkSession.builder.appName("silver_transform")
        .config("spark.sql.sources.partitionOverwriteMode","dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    base_raw    = f"{args.input_bucket}/{args.raw_prefix}"
    base_silver = f"{args.input_bucket}/{args.silver_prefix}"

    # 1) Transactions
    def _safe_read_any(spark, path: str):
        from pyspark.sql.utils import AnalysisException
        try:
            return (spark.read
                    .option("mergeSchema", "true")
                    .option("recursiveFileLookup", "true")
                    .parquet(path))
        except AnalysisException:
            try:
                return (spark.read
                        .option("mergeSchema", "true")
                        .parquet(path + "/*"))
            except AnalysisException:
                return None

    pat = _safe_read_any(spark, f"{base_raw}/{args.src_patagonia}/ingestion_date={args.run_date}")
    rio = _safe_read_any(spark, f"{base_raw}/{args.src_riohacha}/ingestion_date={args.run_date}")

    trans_df = None
    if pat is not None and rio is not None:
        trans_df = pat.unionByName(rio, allowMissingColumns=True)
    elif pat is not None:
        trans_df = pat
    elif rio is not None:
        trans_df = rio

    wrote_any = False
    if trans_df is not None:
        trans_std = standardize_transactions(trans_df, source_name="mixed")
        out_trx = f"{base_silver}/{args.transactions_table}"
        (trans_std
            .withColumn("run_date", F.lit(args.run_date))
            .repartition(1)
            .write.mode("overwrite")
            .partitionBy("run_date")
            .parquet(out_trx))
        print(f"[silver] Escrito transactions -> {out_trx}/run_date={args.run_date}/")
        wrote_any = True
    else:
        print("[silver] WARNING: no hay datos de transacciones para esa fecha en BRONZE.")

    # 2) Weather (opcional)
    weather_path = f"{base_raw}/{args.src_openmeteo}/ingestion_date={args.run_date}"
    weather_raw = _safe_read_parquet(spark, weather_path)
    if weather_raw is not None:
        weather_feat = prepare_weather_features(weather_raw)
        out_w = f"{base_silver}/{args.weather_table}"
        (weather_feat
            .withColumn("run_date", F.lit(args.run_date))
            .repartition(1)
            .write.mode("overwrite")
            .partitionBy("run_date")
            .parquet(out_w))
        print(f"[silver] Escrito weather -> {out_w}/run_date={args.run_date}/")
        wrote_any = True
    else:
        print("[silver] INFO: no hay datos de clima para esa fecha (se omite).")

    spark.stop()
    if not wrote_any:
        raise SystemExit("[silver] ERROR: No se escribió nada en SILVER (no había datos en BRONZE).")
    return 0

if __name__ == "__main__":
    sys.exit(main())