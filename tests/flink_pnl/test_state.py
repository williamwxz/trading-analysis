from datetime import datetime

import pytest

from libs.computation.anchor_state import AnchorRecord, AnchorState
from flink_pnl.state import StateMap, build_state_from_bootstrap


@pytest.mark.unit
def test_groups_by_underlying():
    anchor = AnchorState()
    anchor.set("strat_btc_5m", AnchorRecord(
        pnl=1.0, price=50000.0, position=1.0,
        underlying="BTC", strategy_instance_id="sid1",
    ))
    anchor.set("strat_eth_1h", AnchorRecord(
        pnl=0.5, price=3000.0, position=-1.0,
        underlying="ETH", strategy_instance_id="sid2",
    ))

    result: StateMap = build_state_from_bootstrap(anchor)

    assert "BTC" in result
    assert "ETH" in result
    assert "strat_btc_5m" in result["BTC"]
    assert "strat_eth_1h" in result["ETH"]
    assert result["BTC"]["strat_btc_5m"].pnl == 1.0
    assert result["ETH"]["strat_eth_1h"].position == -1.0


@pytest.mark.unit
def test_empty_anchor_state_returns_empty_map():
    result = build_state_from_bootstrap(AnchorState())
    assert result == {}


@pytest.mark.unit
def test_multiple_strategies_same_underlying():
    anchor = AnchorState()
    anchor.set("strat_btc_5m", AnchorRecord(pnl=1.0, price=50000.0, position=1.0, underlying="BTC", strategy_instance_id="s1"))
    anchor.set("strat_btc_1h", AnchorRecord(pnl=2.0, price=50000.0, position=-1.0, underlying="BTC", strategy_instance_id="s2"))

    result = build_state_from_bootstrap(anchor)

    assert len(result["BTC"]) == 2
    assert "strat_btc_5m" in result["BTC"]
    assert "strat_btc_1h" in result["BTC"]
