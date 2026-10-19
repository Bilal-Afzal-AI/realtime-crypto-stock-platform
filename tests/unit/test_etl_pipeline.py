"""Bronze -> silver -> gold, on a throwaway lake built in a temp folder."""

import json
from datetime import UTC, datetime

import pyarrow.parquet as pq
import pytest

from pipelines.etl import bronze_to_silver, lake, silver_to_gold

DATE = "2026-10-19"


def ms(hour: int, minute: int = 0, second: int = 0) -> int:
    return int(datetime(2026, 10, 19, hour, minute, second, tzinfo=UTC).timestamp() * 1000)


def rec(offset, price, size, ts, symbol="BTCUSDT", source="binance", partition=0, **over):
    r = {
        "source": source, "symbol": symbol, "price": price, "size": size,
        "trade_ts_ms": ts, "trade_id": str(1000 + offset), "ingest_ts_ms": ts + 300,
        "_kafka": {"topic": "crypto.trades", "partition": partition, "offset": offset,
                   "key": symbol, "timestamp_ms": ts + 300, "schema_id": 4},
    }
    r.update(over)
    return r


def write_bronze(lake_root, name, lines, source="binance", symbol="BTCUSDT", date=DATE):
    folder = lake_root / "bronze" / source / symbol / date
    folder.mkdir(parents=True, exist_ok=True)
    text = "\n".join(l if isinstance(l, str) else json.dumps(l) for l in lines) + "\n"
    (folder / name).write_text(text, encoding="utf-8")


def run_silver(lake_root):
    return [bronze_to_silver.process_partition(p["source"], p["symbol"], p["date"], lake_root)
            for p in bronze_to_silver.find_partitions_with_new_files(lake_root)]


@pytest.fixture
def lake_root(tmp_path):
    return tmp_path / "lake"


def silver_table(lake_root, source="binance", symbol="BTCUSDT"):
    return pq.read_table(lake_root / "silver" / source / symbol / DATE / "trades.parquet")


def test_cleans_types_and_timestamps(lake_root):
    write_bronze(lake_root, "trades_a.json", [rec(1, 100.5, 0.25, ms(10, 5))])
    run_silver(lake_root)
    t = silver_table(lake_root)
    assert str(t.schema.field("trade_ts").type) == "timestamp[ms, tz=UTC]"
    assert str(t.schema.field("price").type) == "double"
    row = t.to_pylist()[0]
    assert row["trade_ts"] == datetime(2026, 10, 19, 10, 5, tzinfo=UTC)
    assert (row["price"], row["size"], row["kafka_offset"]) == (100.5, 0.25, 1)


def test_duplicates_within_and_across_files_are_removed(lake_root):
    write_bronze(lake_root, "trades_a.json", [rec(1, 100, 1, ms(10)), rec(2, 101, 1, ms(10, 1)),
                                              rec(2, 101, 1, ms(10, 1))])
    write_bronze(lake_root, "trades_b.json", [rec(2, 101, 1, ms(10, 1)), rec(3, 102, 1, ms(10, 2))])
    stats = run_silver(lake_root)[0]
    assert [r["kafka_offset"] for r in silver_table(lake_root).to_pylist()] == [1, 2, 3]
    assert stats["duplicates_dropped"] == 2


def test_same_price_and_time_but_different_offsets_are_both_kept(lake_root):
    # Two genuine trades that look identical must not be merged.
    write_bronze(lake_root, "trades_a.json", [rec(1, 100, 1, ms(10)), rec(2, 100, 1, ms(10))])
    run_silver(lake_root)
    assert silver_table(lake_root).num_rows == 2


def test_malformed_records_are_quarantined_not_fatal(lake_root):
    good = rec(1, 100, 1, ms(10))
    write_bronze(lake_root, "trades_a.json", [
        good,
        "{not json",
        rec(2, -5, 1, ms(10)),                         # bad price
        {**rec(3, 100, 1, ms(10)), "price": "abc"},    # bad type
        {k: v for k, v in rec(4, 100, 1, ms(10)).items() if k != "size"},  # missing field
        rec(5, 100, 1, ms(10), trade_ts_ms=5),         # absurd timestamp
        rec(6, 100, 1, ms(10), symbol="ETHUSDT"),      # wrong folder
        "[1,2,3]",                                     # not an object
    ])
    stats = run_silver(lake_root)[0]
    assert silver_table(lake_root).num_rows == 1
    assert stats["rejected"] == 7
    q = lake_root / "silver" / "_quarantine" / "binance" / "BTCUSDT" / DATE / "trades_a.json.rejects.jsonl"
    reasons = [json.loads(l)["reason"] for l in q.read_text().splitlines()]
    assert reasons == ["invalid_json", "bad_value:price", "bad_type:price", "missing_field:size",
                       "bad_timestamp:trade_ts_ms", "symbol_mismatch", "not_an_object"]


def test_silver_is_idempotent(lake_root):
    write_bronze(lake_root, "trades_a.json", [rec(1, 100, 1, ms(10)), "{bad", rec(2, 101, 1, ms(11))])
    run_silver(lake_root)
    path = lake_root / "silver" / "binance" / "BTCUSDT" / DATE / "trades.parquet"
    first_bytes, first_mtime = path.read_bytes(), path.stat().st_mtime_ns

    assert run_silver(lake_root) == []                      # nothing new -> nothing to do
    assert path.stat().st_mtime_ns == first_mtime           # and the file was not even touched

    # Force a full rebuild by forgetting what was processed: identical result.
    (path.parent / lake.SILVER_MANIFEST).unlink()
    stats = run_silver(lake_root)[0]
    assert stats["rows_added"] == 0 and stats["duplicates_dropped"] == 2
    assert path.read_bytes() == first_bytes


def test_incremental_run_equals_one_big_run(lake_root, tmp_path):
    batch1 = [rec(1, 100, 1, ms(10)), rec(3, 103, 1, ms(10, 3))]
    batch2 = [rec(2, 102, 1, ms(10, 2)), rec(3, 103, 1, ms(10, 3)), rec(4, 104, 1, ms(10, 4))]
    write_bronze(lake_root, "trades_a.json", batch1)
    run_silver(lake_root)
    write_bronze(lake_root, "trades_b.json", batch2)       # arrives later, overlaps, out of order
    run_silver(lake_root)

    once = tmp_path / "once"
    write_bronze(once, "trades_a.json", batch1)
    write_bronze(once, "trades_b.json", batch2)
    run_silver(once)
    assert silver_table(lake_root).equals(silver_table(once))


def test_gold_ohlcv_values(lake_root):
    write_bronze(lake_root, "trades_a.json", [
        rec(1, 100, 1.0, ms(10, 0, 1)),
        rec(2, 110, 2.0, ms(10, 20)),
        rec(3, 90, 0.5, ms(10, 40)),
        rec(4, 105, 1.5, ms(10, 59, 59)),
        rec(5, 200, 3.0, ms(11, 0, 0)),
    ])
    run_silver(lake_root)
    for p in silver_to_gold.find_stale_partitions(lake_root):
        silver_to_gold.build_partition(p["source"], p["symbol"], p["date"], lake_root)
    g = pq.read_table(lake_root / "gold" / "ohlcv_1h" / "binance" / "BTCUSDT" / DATE / "ohlcv_1h.parquet")
    rows = g.to_pylist()
    assert [r["hour_start"].hour for r in rows] == [10, 11]
    h10 = rows[0]
    assert (h10["open"], h10["high"], h10["low"], h10["close"]) == (100, 110, 90, 105)
    assert h10["volume"] == pytest.approx(5.0) and h10["trade_count"] == 4
    assert (rows[1]["open"], rows[1]["close"], rows[1]["trade_count"]) == (200, 200, 1)


def test_gold_is_idempotent_and_picks_up_late_data(lake_root):
    write_bronze(lake_root, "trades_a.json", [rec(1, 100, 1, ms(10)), rec(2, 101, 1, ms(10, 30))])
    run_silver(lake_root)

    def build_all():
        out = silver_to_gold.find_stale_partitions(lake_root)
        for p in out:
            silver_to_gold.build_partition(p["source"], p["symbol"], p["date"], lake_root)
        return out

    gold = lake_root / "gold" / "ohlcv_1h" / "binance" / "BTCUSDT" / DATE / "ohlcv_1h.parquet"
    assert len(build_all()) == 1
    first = gold.read_bytes()
    assert build_all() == []                               # nothing changed -> nothing stale
    assert gold.read_bytes() == first

    write_bronze(lake_root, "trades_b.json", [rec(3, 120, 1, ms(10, 50))])   # late trade, same hour
    run_silver(lake_root)
    assert len(build_all()) == 1                           # silver changed -> rebuilt
    h10 = pq.read_table(gold).to_pylist()[0]
    assert (h10["close"], h10["high"], h10["trade_count"]) == (120, 120, 3)


def test_gold_check_rejects_inconsistent_candles(lake_root):
    write_bronze(lake_root, "trades_a.json", [rec(1, 100, 1, ms(10)), rec(2, 101, 1, ms(10, 5))])
    run_silver(lake_root)
    silver = silver_table(lake_root)
    good = silver_to_gold.compute_ohlcv(silver)
    silver_to_gold.check_ohlcv(good, silver)               # passes
    broken = good.set_column(good.schema.get_field_index("high"), "high",
                             good["low"].combine_chunks().take([0]).cast("double"))
    with pytest.raises(ValueError):
        silver_to_gold.check_ohlcv(broken, silver)
