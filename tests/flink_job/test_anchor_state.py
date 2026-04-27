import pytest
from flink_job.anchor_state import AnchorState, AnchorRecord


@pytest.mark.unit
def test_anchor_state_returns_default_when_empty():
    state = AnchorState()
    rec = state.get("strat_A")
    assert rec.anchor_pnl == 0.0
    assert rec.anchor_price == 0.0
    assert rec.anchor_position == 0.0


@pytest.mark.unit
def test_anchor_state_stores_and_retrieves():
    state = AnchorState()
    state.update("strat_A", AnchorRecord(anchor_pnl=10.5, anchor_price=93000.0, anchor_position=1.0))
    rec = state.get("strat_A")
    assert rec.anchor_pnl == 10.5
    assert rec.anchor_price == 93000.0
    assert rec.anchor_position == 1.0


@pytest.mark.unit
def test_anchor_state_carry_forward_on_missing_price():
    """When anchor_price is 0 (no prior data), PnL stays at 0."""
    state = AnchorState()
    rec = state.get("new_strat")
    pnl = rec.anchor_pnl + rec.anchor_position * (
        93000.0 - rec.anchor_price
    ) / max(rec.anchor_price, 1e-10)
    assert pnl == 0.0  # position is 0, so no PnL


@pytest.mark.unit
def test_anchor_state_pnl_formula():
    """cumulative_pnl = anchor_pnl + position * (close - anchor_price) / anchor_price"""
    state = AnchorState()
    state.update("strat_B", AnchorRecord(anchor_pnl=5.0, anchor_price=100.0, anchor_position=2.0))
    rec = state.get("strat_B")
    close_price = 110.0
    pnl = rec.anchor_pnl + rec.anchor_position * (close_price - rec.anchor_price) / rec.anchor_price
    assert pnl == pytest.approx(5.0 + 2.0 * 10.0 / 100.0)  # 5.2
