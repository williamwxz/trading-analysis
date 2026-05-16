from __future__ import annotations

from libs.computation.anchor_state import AnchorRecord, AnchorState

# underlying ("BTC") → strategy_table_name → AnchorRecord
StateMap = dict[str, dict[str, AnchorRecord]]


def build_state_from_bootstrap(anchor: AnchorState) -> StateMap:
    """Convert a flat AnchorState into a nested StateMap keyed by underlying.

    The bootstrap produces AnchorState keyed by strategy_table_name. This
    converts it to underlying → stn → AnchorRecord so the process function
    can look up all strategies for a given underlying in O(1).

    Strategies whose AnchorRecord.underlying is empty are skipped — they
    have no metadata yet and cannot be carry-forwarded meaningfully.
    """
    result: StateMap = {}
    for stn in anchor.keys():
        rec = anchor.get(stn)
        if not rec.underlying:
            continue
        if rec.underlying not in result:
            result[rec.underlying] = {}
        result[rec.underlying][stn] = rec
    return result
