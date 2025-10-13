from datetime import datetime

from pyspark.sql import functions as F

from src.jobs.silver_transform import prepare_weather_features, standardize_transactions


def with_metadata(df, source, ingestion_date):
    return (
        df.withColumn("source", F.lit(source))
        .withColumn("ingestion_date", F.lit(ingestion_date))
        .withColumn("load_ts", F.lit(datetime.utcnow()))
        .withColumn("file_name", F.lit(f"{source}.json"))
    )


def test_standardize_transactions_handles_schema_variation(spark):
    patagonia = spark.createDataFrame(
        [
            ("A1", "2025-10-11T10:00:00", "S1", "Patagonia", "SKU1", "Chaqueta", "Ropa", 2, 150.0, "C1", "online", "tarjeta"),
        ],
        [
            "transaction_id",
            "transaction_ts",
            "store_id",
            "region",
            "product_id",
            "product_name",
            "category",
            "quantity",
            "unit_price",
            "customer_id",
            "channel",
            "payment_method",
        ],
    )
    patagonia = with_metadata(patagonia, "patagonia", "2025-10-11")

    riohacha = spark.createDataFrame(
        [
            ("B9", "2025-10-11 12:34:00", "S9", "Caribe", "SKU9", "Sombrero", "Accesorios", 1, 80.0, "C9", "tienda", "efectivo"),
        ],
        [
            "id",
            "datetime",
            "tienda_id",
            "zona",
            "sku",
            "item_name",
            "categoria",
            "cantidad",
            "precio",
            "cliente_id",
            "canal",
            "metodo_pago",
        ],
    )
    riohacha = with_metadata(riohacha, "riohacha", "2025-10-11")

    patagonia_std = standardize_transactions(patagonia, "patagonia")
    riohacha_std = standardize_transactions(riohacha, "riohacha")

    assert set(patagonia_std.columns) == set(riohacha_std.columns)

    combined = patagonia_std.unionByName(riohacha_std)
    rows = combined.collect()
    assert rows[0]["revenue"] == rows[0]["quantity"] * float(rows[0]["unit_price"])
    assert rows[1]["region"] == "Caribe"
    assert rows[1]["payment_method"] == "efectivo"


def test_prepare_weather_features_aggregates_daily(spark):
    weather = spark.createDataFrame(
        [
            ("Patagonia", "2025-10-11 08:00:00", -5.0, 1.0, 45),
            ("Patagonia", "2025-10-11 18:00:00", -3.0, 0.5, 45),
        ],
        ["region", "time", "temperature_2m_mean", "precipitation", "weathercode"],
    )
    weather = with_metadata(weather, "openmeteo", "2025-10-11")

    result = prepare_weather_features(weather)
    data = result.collect()[0]
    assert data["region"] == "Patagonia"
    assert round(data["weather_avg_temp"], 2) == -4.0
    assert round(data["weather_precipitation"], 2) == 0.75
