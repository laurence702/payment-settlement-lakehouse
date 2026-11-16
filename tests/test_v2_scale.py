from settlement.config import VOLUME_PRESETS, get_settings
from settlement.serve import MARTS, _s3_expr


def test_volume_presets_cover_targets():
    assert VOLUME_PRESETS["50k"] == 50_000
    assert VOLUME_PRESETS["500k"] == 500_000
    assert VOLUME_PRESETS["1M"] == 1_000_000
    assert VOLUME_PRESETS["1m"] == 1_000_000


def test_s3_expr_generates_valid_collection_query():
    settings = get_settings()
    expr = _s3_expr(settings, "fct_transactions.parquet")
    assert "s3(s3_lakehouse" in expr
    assert "fct_transactions.parquet" in expr
    assert "format = 'Parquet'" in expr


def test_marts_spec_covers_all_required_tables():
    mart_names = [m.name for m in MARTS]
    assert "fct_transactions" in mart_names
    assert "fct_settlements" in mart_names
    assert "mart_settlement_reconciliation" in mart_names
    assert "mart_merchant_daily" in mart_names
    assert "mart_channel_performance" in mart_names
