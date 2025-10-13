import json

from src.jobs.bronze_ingest import read_json_source


def test_read_json_source_adds_metadata(spark, tmp_path):
    data_dir = tmp_path / "raw" / "patagonia" / "ingestion_date=2025-10-11"
    data_dir.mkdir(parents=True)
    file_path = data_dir / "sample.json"
    with file_path.open("w", encoding="utf-8") as handle:
        handle.write(json.dumps({"transaction_id": "A1", "value": 10}))

    df = read_json_source(
        spark,
        str(file_path),
        source="patagonia",
        ingestion_date="2025-10-11",
        bad_records_path=str(tmp_path / "badrecords"),
    )

    row = df.collect()[0]
    assert row.source == "patagonia"
    assert row.ingestion_date == "2025-10-11"
    assert "sample.json" in row.file_name
