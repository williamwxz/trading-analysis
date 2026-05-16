import pytest
from flink_pnl.sink_config import SinkConfig


@pytest.mark.unit
def test_all_false_by_default():
    cfg = SinkConfig.from_env({})
    assert cfg.price is False
    assert cfg.prod is False
    assert cfg.bt is False
    assert cfg.real_trade is False


@pytest.mark.unit
def test_enable_price_only():
    cfg = SinkConfig.from_env({"ENABLE_PRICE_SINK": "true"})
    assert cfg.price is True
    assert cfg.prod is False
    assert cfg.bt is False
    assert cfg.real_trade is False


@pytest.mark.unit
def test_enable_all():
    cfg = SinkConfig.from_env({
        "ENABLE_PRICE_SINK": "true",
        "ENABLE_PROD_SINK": "true",
        "ENABLE_BT_SINK": "true",
        "ENABLE_REAL_TRADE_SINK": "true",
    })
    assert cfg.price is True
    assert cfg.prod is True
    assert cfg.bt is True
    assert cfg.real_trade is True


@pytest.mark.unit
def test_case_insensitive():
    cfg = SinkConfig.from_env({"ENABLE_PROD_SINK": "TRUE"})
    assert cfg.prod is True


@pytest.mark.unit
def test_false_string():
    cfg = SinkConfig.from_env({"ENABLE_PROD_SINK": "false"})
    assert cfg.prod is False


@pytest.mark.unit
def test_from_env_reads_os_environ(monkeypatch):
    monkeypatch.setenv("ENABLE_PROD_SINK", "true")
    cfg = SinkConfig.from_env()
    assert cfg.prod is True
    assert cfg.price is False


@pytest.mark.unit
def test_invalid_values_default_to_false():
    cfg = SinkConfig.from_env({"ENABLE_PRICE_SINK": "yes", "ENABLE_PROD_SINK": "1", "ENABLE_BT_SINK": ""})
    assert cfg.price is False
    assert cfg.prod is False
    assert cfg.bt is False
