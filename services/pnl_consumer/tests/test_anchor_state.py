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


@pytest.mark.unit
def test_anchor_state_update_preserves_unchanged_metadata():
    """update() must merge into the existing record, not replace it.

    Regression for the bootstrap-walk bug: the walk loop only carries
    pnl/price/position/bar_ts/revision_ts; metadata like underlying and
    strategy_instance_id were being clobbered to dataclass defaults, which
    silently broke carry-forward in the live loop (returning None because
    rec.strategy_instance_id was empty).
    """
    state = AnchorState()
    state.set(
        "strat_C",
        AnchorRecord(
            pnl=1.0,
            price=100.0,
            position=1.0,
            strategy_instance_id="inst_C",
            underlying="BTC",
            strategy_id=42,
            strategy_name="trend",
            config_timeframe="1h",
            weighting=0.5,
            final_signal=1.0,
            benchmark=0.02,
        ),
    )

    state.update("strat_C", pnl=2.0, price=110.0, position=-1.0)

    rec = state.get("strat_C")
    # advanced fields
    assert rec.pnl == 2.0
    assert rec.price == 110.0
    assert rec.position == -1.0
    # preserved metadata — must not regress to defaults
    assert rec.strategy_instance_id == "inst_C"
    assert rec.underlying == "BTC"
    assert rec.strategy_id == 42
    assert rec.strategy_name == "trend"
    assert rec.config_timeframe == "1h"
    assert rec.weighting == 0.5
    assert rec.final_signal == 1.0
    assert rec.benchmark == 0.02


@pytest.mark.unit
def test_anchor_state_update_creates_from_defaults_when_missing():
    """update() on an unseen key creates a record from defaults + given fields."""
    state = AnchorState()
    state.update("strat_new", pnl=3.0, price=200.0, position=1.0)
    rec = state.get("strat_new")
    assert rec.pnl == 3.0
    assert rec.price == 200.0
    assert rec.position == 1.0
    assert rec.strategy_instance_id == ""
    assert rec.underlying == ""
