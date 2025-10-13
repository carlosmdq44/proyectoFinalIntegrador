from datetime import date, datetime

from pyspark.sql import Row, functions as F

from src.jobs.gold_analytics import (
    anomalies,
    fact_sales_daily,
    margin_by_product,
    new_vs_returning,
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    IntegerType,
    DoubleType,
    TimestampType
)


def sample_transactions(spark):
    rows = [
        Row(
            txn_id="T1",
            txn_date=date(2025, 10, 10),
            region="R1",
            store_id="S1",
            customer_id="C1",
            product_id="SKU1",
            product_name="Prod 1",
            category="CatA",
            channel="online",
            quantity=1,
            unit_price=100.0,
            revenue=100.0,
            payment_method="card",
            load_ts=load_time,
            unit_cost=75.0,
            cost=75.0,
            source="patagonia",
            ingestion_date=date(2025, 10, 12),
            file_name="patagonia_20251012.json",
        ),
        Row(
            txn_id="T2",
            txn_date=date(2025, 9, 10),
            region="R1",
            store_id="S1",
            customer_id="C1",
            product_id="SKU1",
            product_name="Prod 1",
            category="CatA",
            channel="online",
            quantity=2,
            unit_price=100.0,
            revenue=200.0,
            payment_method="card",
            load_ts=load_time,
            unit_cost=None,
            cost=None,
            source="patagonia",
            ingestion_date=date(2025, 9, 11),
            file_name="patagonia_20250911.json",
        ),
        Row(
            txn_id="T3",
            txn_date=date(2025, 10, 10),
            region="R1",
            store_id="S1",
            customer_id="C2",
            product_id="SKU2",
            product_name="Prod 2",
            category="CatA",
            channel="tienda",
            quantity=1,
            unit_price=50.0,
            revenue=50.0,
            payment_method="efectivo",
            load_ts=load_time,
            unit_cost=32.0,
            cost=32.0,
            source="patagonia",
            ingestion_date=date(2025, 10, 12),
            file_name="patagonia_20251012.json",
        ),
        Row(
            txn_id="T4",
            txn_date=date(2025, 10, 10),
            region="R2",
            store_id="S9",
            customer_id="C3",
            product_id="SKU9",
            product_name="Prod 9",
            category="CatB",
            channel="online",
            quantity=5,
            unit_price=20.0,
            revenue=100.0,
            payment_method="card",
            load_ts=load_time,
            unit_cost=12.0,
            cost=60.0,
            source="riohacha",
            ingestion_date=date(2025, 10, 12),
            file_name="riohacha_20251012.json",
        ),
        Row(
            txn_id="T5",
            txn_date=date(2025, 10, 11),
            region="R1",
            store_id="S1",
            customer_id="C1",
            product_id="SKU1",
            product_name="Prod 1",
            category="CatA",
            channel="online",
            quantity=3,
            unit_price=100.0,
            revenue=300.0,
            payment_method="card",
            load_ts=load_time,
            unit_cost=70.0,
            cost=210.0,
            source="patagonia",
            ingestion_date=date(2025, 10, 12),
            file_name="patagonia_20251012.json",
        ),
    ]

    schema = StructType(
        [
            StructField("txn_id", StringType(), False),
            StructField("txn_date", DateType(), False),
            StructField("region", StringType(), True),
            StructField("store_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("revenue", DoubleType(), True),
            StructField("payment_method", StringType(), True),
            StructField("load_ts", TimestampType(), True),
            StructField("unit_cost", DoubleType(), True),
            StructField("cost", DoubleType(), True),
            StructField("source", StringType(), True),
            StructField("ingestion_date", DateType(), True),
            StructField("file_name", StringType(), True),
        ]
    )

    return spark.createDataFrame(rows, schema=schema)


def test_new_vs_returning_classifies_correctly(spark, tmp_path):
    df = sample_transactions(spark)
    output = new_vs_returning(df, str(tmp_path))
    mix = {row["txn_month"].strftime("%Y-%m"): (row["new_customers"], row["returning_customers"]) for row in
           output.collect()}
    assert mix["2025-09"] == (0, 1)
    assert mix["2025-10"] == (0, 1)


def test_margin_by_product_uses_default_cost(spark, tmp_path):
    df = sample_transactions(spark)
    result = margin_by_product(df, str(tmp_path), default_cost_factor=0.7)
    metrics = {row["product_id"]: row for row in result.collect()}
    sku1_margin = metrics["SKU1"]["margin"]
    assert sku1_margin < metrics["SKU1"]["total_revenue"]


def test_anomalies_detects_outliers(spark, tmp_path):
    df = sample_transactions(spark)
    # amplify one day revenue to trigger anomaly
    df = df.withColumn(
        "revenue",
        F.when(F.col("txn_id") == "T5", F.col("revenue") * 10).otherwise(F.col("revenue")),
    )
    result = anomalies(df, str(tmp_path))
    flags = {(row["txn_date"], row["region"], row["category"]): row["is_anomaly"] for row in result.collect()}
    assert any(flags.values())


def test_fact_sales_daily_maps_mix(spark, tmp_path):
    df = sample_transactions(spark)
    fact = fact_sales_daily(df, str(tmp_path))
    row = fact.filter((F.col("txn_date") == F.lit(date(2025, 10, 10))) & (F.col("region") == "R1")).collect()[0]
    assert "online" in row["channel_mix"]
    assert row["orders"] >= 1
