from decimal import Decimal

from warehouse import loader


def test_float_prices_become_exact_decimals():
    # repr() gives the shortest round-trip text, so the value we stored is the value we meant.
    assert loader.to_decimal(82714.76) == Decimal("82714.76000000")
    assert loader.to_decimal(0.1) + loader.to_decimal(0.2) == loader.to_decimal(0.3)


def test_float_noise_is_rounded_away_at_8_places():
    assert loader.to_decimal(4.041800000000001) == Decimal("4.04180000")
    assert loader.to_decimal(1e-9) == Decimal("0E-8")


def test_unknown_symbol_metadata_is_inferred():
    assert loader._infer_symbol_row("binance", "SOLUSDT") == ("binance", "SOLUSDT", "crypto", "USDT")
    assert loader._infer_symbol_row("finnhub", "NVDA") == ("finnhub", "NVDA", "equity", "USD")
    assert loader._infer_symbol_row("finnhub", "BINANCE:SOLUSDT") == (
        "finnhub", "BINANCE:SOLUSDT", "crypto", "USDT")
