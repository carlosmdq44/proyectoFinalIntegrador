# src/jobs/silver_transform.py
"""
Silver transformations: estandariza transacciones y prepara features de clima.

💡 Cómo explicarlo en tu defensa oral (TL;DR):
- **Objetivo del job**: leer datos de BRONZE (S3), normalizarlos a un schema común (transacciones) y derivar
  variables útiles del clima (por día y región). Escribimos el resultado en SILVER particionado por `run_date`.
- **Beneficio**: desacoplamos la variedad de fuentes/formatos de BRONZE y dejamos datos limpios, tipeados
  y con nombres consistentes para GOLD.
- **Casos contemplados para clima**: tanto datasets **horarios** (Open‑Meteo en arrays) como **diarios**
  (ya agregados). En ambos casos terminamos con (region, weather_date, métricas).
"""
from __future__ import annotations

import argparse, sys
from pyspark.sql import SparkSession, DataFrame, functions as F, types as T
from pyspark.sql.utils import AnalysisException
# Nota: ya importamos F y T arriba; esta segunda línea sería redundante, pero no afecta.
from pyspark.sql import functions as F, types as T

# --- Normalización de transacciones ----------------------------------------------------
# Columnas objetivo estandarizadas para cualquier fuente de ventas.
STANDARD_COLS = [
    "id","fecha","sucursal_id","region","producto_id","item_name","categoria",
    "cantidad","precio","revenue","cliente_id","canal","metodo_pago","load_ts",
]

# Mapa de renombre: múltiples sinónimos → una única columna estándar.
# 
# Explicación oral: distintas fuentes pueden llamar distinto a la misma cosa (p. ej. "txn_id" vs "transaction_id").
# Aquí resolvemos esa heterogeneidad.
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
# Revenue puede venir con varios nombres. Lo sumamos al mapa.
RENAME_MAP.update({"revenue":"revenue","amount":"revenue","importe_total":"revenue"})

# Candidatos de columnas para mapear región y tiempo en datasets de clima.
WEATHER_REGION_CANDIDATES = ["region", "city", "name", "location"]
WEATHER_TIME_CANDIDATES   = ["time", "timestamp", "datetime", "date"]

# Columnas típicas **HORARIAS** de Open‑Meteo (vienen como arrays paralelos a "time").
HOURLY_ARRAY_CANDIDATES = [
    ("temperature_2m", "weather_avg_temp"),
    ("precipitation", "weather_precipitation"),
    ("wind_speed_10m", "weather_wind_speed"),
    ("relative_humidity_2m", "weather_humidity"),
    ("weathercode", "weather_code"),
]

# Columnas **DIARIAS** (ya agregadas) en distintas variantes de nombre.
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

# Utilidad: devuelve la primera columna existente de una lista de candidatos.
# Esto nos permite ser tolerantes a esquemas distintos.
def _first_existing_column(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

# Normaliza/estandariza nombres de columnas usando RENAME_MAP.
# Si un nombre no está en el mapa, lo deja igual. Mantiene el orden.
def _normalize_columns(df: DataFrame) -> DataFrame:
    exprs = []
    for c in df.columns:
        # Buscamos tanto por el nombre exacto como por su versión en minúsculas.
        tgt = RENAME_MAP.get(c, RENAME_MAP.get(c.lower(), c))
        exprs.append(F.col(c).alias(tgt))
    return df.select(*exprs)

# Estandariza tipos y columnas de transacciones hacia STANDARD_COLS.
# - Asegura tipos (string/timestamp/double/etc.).
# - Si falta una columna, la crea como null casteado al tipo correcto (para unionByName futuro).
# - Normaliza formatos de fecha con to_timestamp.
def standardize_transactions(df: DataFrame, source_name: str) -> DataFrame:
    df = _normalize_columns(df)

    # Tipado destino coherente para analítica y uniones futuras.
    types_map = {
        "id":T.StringType(),"fecha":T.TimestampType(),"sucursal_id":T.StringType(),
        "region":T.StringType(),"producto_id":T.StringType(),"item_name":T.StringType(),
        "categoria":T.StringType(),"cantidad":T.IntegerType(),"precio":T.DoubleType(),
        "cliente_id":T.StringType(),"canal":T.StringType(),"metodo_pago":T.StringType(),
        "load_ts":T.TimestampType(),"revenue":T.DoubleType(),
    }

    for name, dtype in types_map.items():
        if name not in df.columns:
            # Crea columna nula con el tipo correcto (importante para unionByName y escritura consistente).
            df = df.withColumn(name, F.lit(None).cast(dtype))
        else:
            # Fuerza el tipo correcto evitando problemas de schema evolution en Parquet.
            df = df.withColumn(name, F.col(name).cast(dtype))

    # Normaliza la columna de fecha a timestamp (acepta strings de varios formatos).
    df = df.withColumn(
        "fecha",
        F.when(F.col("fecha").cast("timestamp").isNotNull(), F.col("fecha").cast("timestamp"))
         .otherwise(F.to_timestamp("fecha"))
    )
    
    # Orden y subconjunto final definido por STANDARD_COLS (consistencia de esquema en SILVER).
    return df.select(*STANDARD_COLS)

# --- Clima ---------------------------------------------------------------------------
# Candidatos repetidos (sección superior) mantenidos por claridad de lectura.
WEATHER_REGION_CANDIDATES = ["region","city","name","location"]
WEATHER_TIME_CANDIDATES   = ["time","timestamp","datetime","date"]
WEATHER_TEMP_CANDIDATES   = ["temperature_2m_mean","temperature_mean","temp_mean","avg_temp"]
WEATHER_PRECIP_CANDIDATES = ["precipitation","precipitation_sum","precip"]
WEATHER_CODE_CANDIDATES   = ["weathercode","weather_code","condition_code"]

# (Definición duplicada a propósito para mantener funciones cerca de constantes que usan.)
def _first_existing_column(df: DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None

# Convierte datasets de clima heterogéneos a un formato diario por región.
# Soporta:
#   A) **HORARIO**: columnas en arrays (time[], temperature_2m[], etc.).
#      → Explota arrays, calcula promedios diarios por región/fecha.
#   B) **DIARIO**: columnas escalares ya agregadas.
#      → Simplemente agrupa y toma avg/first según métrica.
# Resultado final: (region, weather_date, weather_avg_temp, weather_precipitation, weather_code, ...)
def prepare_weather_features(df: DataFrame) -> DataFrame:
    # 1) Región: buscamos la primera columna que haga de región.
    region_col = _first_existing_column(df, WEATHER_REGION_CANDIDATES) or "region"
    if region_col not in df.columns:
        # Si no existe, la creamos como null (evita fallas; puede completarse en GOLD por join con sucursal).
        df = df.withColumn("region", F.lit(None).cast("string"))

    # 2) Columna temporal (hora o fecha).
    time_col = _first_existing_column(df, WEATHER_TIME_CANDIDATES)

    # ---------- Caso A: dataset HORARIO (arrays) ----------
    # Heurística: si `time` es array<...>, asumimos formato horario con arrays paralelos por métrica.
    if time_col and dict(df.dtypes).get(time_col) and dict(df.dtypes)[time_col].startswith("array"):
        # Armamos una lista de arrays presentes para zipear (alinear por índice).
        zipped_cols = [F.col(time_col).alias("time")]
        present_hourly = []
        for src, _alias in HOURLY_ARRAY_CANDIDATES:
            if src in df.columns and dict(df.dtypes)[src].startswith("array"):
                zipped_cols.append(F.col(src).alias(src))
                present_hourly.append((src, _alias))

        # arrays_zip: combina arrays columna a columna en un array de structs {time, temp, precip, ...}
        zipped = F.arrays_zip(*zipped_cols)

        # Explode: pasa de una fila con arrays a N filas (una por hora). Cast de time → timestamp y derivamos date.
        exploded = (
            df
            .select(F.col(region_col).alias("region"), F.explode(zipped).alias("z"))
            .withColumn("weather_ts", F.to_timestamp(F.col("z.time")))
            .withColumn("weather_date", F.to_date(F.col("weather_ts")))
        )

        # Seleccionamos y renombramos las métricas disponibles ya alineadas.
        sel = [
            "region",
            "weather_date",
        ]
        agg_exprs = []

        for src, tgt in present_hourly:
            # Guardamos la métrica con el alias destino (tgt). Luego agregaremos por día.
            sel.append(F.col(f"z.{src}").alias(tgt))
            # Para códigos de clima elegimos el primero del día; para numéricos, el promedio.
            if tgt == "weather_code":
                agg_exprs.append(F.first(F.col(tgt)).alias(tgt))
            else:
                agg_exprs.append(F.avg(F.col(tgt)).alias(tgt))

        hourly_flat = exploded.select(*sel)

        # Agregamos por región/fecha y deduplicamos: obtenemos un registro por día y región.
        return (
            hourly_flat
            .groupBy("region", "weather_date")
            .agg(*agg_exprs)
            .distinct()
        )

    # ---------- Caso B: dataset DIARIO (escalares) ----------
    # Si hay columna temporal, la casteamos a date. Si no, usamos ingestion_date (si existe en BRONZE).
    if time_col:
        df = df.withColumn("weather_date", F.to_date(F.col(time_col)))
    else:
        df = df.withColumn("weather_date", F.to_date("ingestion_date"))

    # Mapear columnas presentes a nuestros nombres destino.
    selects = [F.col(region_col).alias("region"), "weather_date"]
    agg_exprs = {}
    for src, tgt in DAILY_SCALAR_CANDIDATES.items():
        if src in df.columns:
            selects.append(F.col(src).alias(tgt))
            # Códigos: first (no promedia). Numéricos: avg.
            agg_exprs[tgt] = "first" if tgt == "weather_code" else "avg"

    daily = df.select(*selects)

    # Si no hay métricas, devolvemos estructura mínima con columnas nulas (evita romper downstream).
    if not agg_exprs:
        return daily.select("region", "weather_date") \
                    .withColumn("weather_avg_temp", F.lit(None).cast("double")) \
                    .withColumn("weather_precipitation", F.lit(None).cast("double")) \
                    .withColumn("weather_code", F.lit(None).cast("string")) \
                    .distinct()

    # Construimos lista de agregaciones según el tipo definido arriba.
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
# Lectura segura de Parquet: si el path no existe, devuelve None en lugar de romper el job.
def _safe_read_parquet(spark, path: str) -> DataFrame | None:
    try:
        df = spark.read.option("mergeSchema","true").parquet(path)
        print(f"[silver] OK leer: {path}")
        return df
    except AnalysisException:
        print(f"[silver] No existe: {path}")
        return None

# ---- Main ---------------------------------------------------------------------------
# Orquesta el flujo: parsea argumentos, arma rutas S3, lee BRONZE, aplica transformaciones y escribe SILVER.
# Puntos clave para explicar:
# - `partitionOverwriteMode=dynamic`: permite sobrescribir solo la partición `run_date` del día sin tocar otras.
# - Escritura con `.partitionBy("run_date")`: buen patrón para downstream (Athena/Trino/Glue) y pruning eficiente.
# - `repartition(1)`: forzamos un único archivo por partición para la demo (en producción suele evitarse o parametrizarse).

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

    # Construimos rutas base para BRONZE y SILVER.
    base_raw    = f"{args.input_bucket}/{args.raw_prefix}"
    base_silver = f"{args.input_bucket}/{args.silver_prefix}"

    # 1) Transactions
    # Lectura "tolerante": intenta recursiveFileLookup para carpetas anidadas; si falla, prueba con wildcard.
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

    # Leemos Patagonia y Riohacha del día (partición ingestion_date=run_date en BRONZE).
    pat = _safe_read_any(spark, f"{base_raw}/{args.src_patagonia}/ingestion_date={args.run_date}")
    rio = _safe_read_any(spark, f"{base_raw}/{args.src_riohacha}/ingestion_date={args.run_date}")

    # Unimos lo disponible (unionByName tolera columnas faltantes si allowMissingColumns=True en versiones nuevas).
    trans_df = None
    if pat is not None and rio is not None:
        trans_df = pat.unionByName(rio, allowMissingColumns=True)
    elif pat is not None:
        trans_df = pat
    elif rio is not None:
        trans_df = rio

    wrote_any = False
    if trans_df is not None:
        # Estandarización a schema común.
        trans_std = standardize_transactions(trans_df, source_name="mixed")
        out_trx = f"{base_silver}/{args.transactions_table}"
        # Escribimos particionado por run_date (=fecha de corrida, no confundir con fecha de la venta).
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
    # Intentamos leer clima desde BRONZE (ingestion_date=run_date). Si no existe, informamos y seguimos.
    weather_path = f"{base_raw}/{args.src_openmeteo}/ingestion_date={args.run_date}"
    weather_raw = _safe_read_parquet(spark, weather_path)
    if weather_raw is not None:
        # Homogeneizamos a (region, weather_date, métricas)
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

    # Cierre ordenado del SparkSession.
    spark.stop()

    # Si no se escribió nada, devolvemos exit code distinto de 0 (útil para alertas en Airflow/Slack).
    if not wrote_any:
        raise SystemExit("[silver] ERROR: No se escribió nada en SILVER (no había datos en BRONZE).")
    return 0


# Entry point estándar de Python.
if __name__ == "__main__":
    sys.exit(main())
