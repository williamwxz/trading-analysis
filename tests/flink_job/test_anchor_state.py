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
    """When anchor_price is 0 (cold start), compute_pnl returns 0 and sets new anchor."""
    state = AnchorState()
    # cold start: anchor_price == 0.0, position == 1.0 — PnL should stay 0
    pnl = state.compute_pnl("new_strat", close_price=93000.0, position=1.0)
    assert pnl == 0.0
    # anchor should now be set to the new close price
    rec = state.get("new_strat")
    assert rec.anchor_price == 93000.0
    assert rec.anchor_position == 1.0


@pytest.mark.unit
def test_anchor_state_pnl_formula():
    """cumulative_pnl = anchor_pnl + position * (close - anchor_price) / anchor_price"""
    state = AnchorState()
    state.update("strat_B", AnchorRecord(anchor_pnl=5.0, anchor_price=100.0, anchor_position=2.0))
    pnl = state.compute_pnl("strat_B", close_price=110.0, position=2.0)
    # 5.0 + 2.0 * (110 - 100) / 100 = 5.0 + 0.2 = 5.2
    assert pnl == pytest.approx(5.2)
    # state should be updated to new anchor
    rec = state.get("strat_B")
    assert rec.anchor_pnl == pytest.approx(5.2)
    assert rec.anchor_price == 110.0
    assert rec.anchor_position == 2.0
