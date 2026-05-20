import pytest
from libs.computation.anchor_state import AnchorState, AnchorRecord


@pytest.mark.unit
def test_anchor_state_returns_default_when_empty():
    state = AnchorState()
    rec = state.get("strat_A")
    assert rec.pnl == 0.0
    assert rec.price == 0.0
    assert rec.position == 0.0


@pytest.mark.unit
def test_anchor_state_stores_and_retrieves():
    state = AnchorState()
    state.set("strat_A", AnchorRecord(pnl=10.5, price=93000.0, position=1.0))
    rec = state.get("strat_A")
    assert rec.pnl == 10.5
    assert rec.price == 93000.0
    assert rec.position == 1.0


@pytest.mark.unit
def test_anchor_state_raises_on_missing_strategy():
    state = AnchorState()
    with pytest.raises(RuntimeError, match="No anchor state for"):
        state.compute_pnl("new_strat", current_price=93000.0, position=1.0)


@pytest.mark.unit
def test_anchor_state_pnl_formula():
    state = AnchorState()
    state.set("strat_B", AnchorRecord(pnl=5.0, price=100.0, position=2.0))
    pnl = state.compute_pnl("strat_B", current_price=110.0, position=2.0)
    # 5.0 + 2.0 * (110 - 100) / 100 = 5.2
    assert pnl == pytest.approx(5.2)
    rec = state.get("strat_B")
    assert rec.pnl == pytest.approx(5.2)
    assert rec.price == 110.0
    assert rec.position == 2.0
