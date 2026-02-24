from __future__ import annotations

import numpy as np
import pandas as pd

from ingestion.contracts.tick import IngestionTick
from quant_engine.data.derivatives.option_chain.chain_handler import OptionChainDataHandler
from quant_engine.data.derivatives.option_chain.snapshot import OptionChainSnapshot
from quant_engine.strategy.base import GLOBAL_PRESETS

# Row-flag bit constants (mirrored from helpers.py for test isolation)
_ROW_FLAG_MISSING_BID_ASK: int = 1 << 0
_ROW_FLAG_CROSSED_OR_LOCKED: int = 1 << 1
_ROW_FLAG_OI_BELOW_FLOOR: int = 1 << 2


CHAIN_COLS = {
    "instrument_name",
    "expiration_timestamp",
    "expiry_ts",
    "strike",
    "option_type",
    "cp",
    "state",
    "is_active",
    "instrument_id",
    "settlement_currency",
    "base_currency",
    "quote_currency",
    "contract_size",
    "tick_size",
    "min_trade_amount",
    "kind",
    "instrument_type",
    "price_index",
    "counter_currency",
    "settlement_period",
    "tick_size_steps",
}
QUOTE_COLS = {
    "instrument_name",
    "bid_price",
    "ask_price",
    "mid_price",
    "last",
    "mark_price",
    "open_interest",
    "volume_24h",
    "volume_usd_24h",
    "mark_iv",
    "high",
    "low",
    "market_ts",
    "price_change",
}
UNDERLYING_COLS = {
    "instrument_name",
    "underlying_price",
    "underlying_index",
    "estimated_delivery_price",
    "interest_rate",
}
MARKET_COLS = CHAIN_COLS | QUOTE_COLS | UNDERLYING_COLS


def _make_frame(data_ts: int, market_ts: int, expiry_ts: int) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "instrument_name": "BTC-1-EXP-C",
                "expiration_timestamp": expiry_ts,
                "strike": 10_000,
                "option_type": "call",
                "bid_price": 1.0,
                "ask_price": 1.1,
                "market_ts": market_ts,
                "underlying_price": 30_000.0,
            },
            {
                "instrument_name": "BTC-1-EXP-P",
                "expiration_timestamp": expiry_ts,
                "strike": 10_000,
                "option_type": "put",
                "bid_price": 0.9,
                "ask_price": 1.0,
                "market_ts": market_ts,
                "underlying_price": 30_000.0,
            },
        ]
    )


def test_option_chain_timestamp_semantics_coords_and_selection() -> None:
    data_ts = 1_700_000_000_000
    market_ts = data_ts - 5_000
    expiry_ts = data_ts + 10_000
    handler = OptionChainDataHandler(symbol="BTC", interval="1m", preset="option_chain")
    frame = _make_frame(data_ts, market_ts, expiry_ts)
    tick = IngestionTick(
        timestamp=data_ts,
        data_ts=data_ts,
        domain="option_chain",
        symbol="BTC",
        payload={"data_ts": data_ts, "frame": frame},
        source_id=getattr(handler, "source_id", None),
    )
    handler.on_new_tick(tick)

    market_df = handler.market_frame()
    assert set(market_df.columns).issubset(MARKET_COLS)
    assert "underlying_price" in market_df.columns
    market_df_no_u = handler.market_frame(include_underlying=False)
    assert "underlying_price" not in market_df_no_u.columns
    market_df_yes_u = handler.market_frame(include_underlying=True)
    assert "underlying_price" in market_df_yes_u.columns
    chain_default = handler.chain_df()
    assert "underlying_price" not in chain_default.columns
    chain_with_u = handler.chain_df(include_underlying=True)
    assert "underlying_price" in chain_with_u.columns
    assert "tau_ms" not in market_df.columns
    assert "x" not in market_df.columns
    assert "snapshot_data_ts" not in market_df.columns
    assert "snapshot_market_ts" not in market_df.columns
    assert "slice_kind" not in market_df.columns
    assert "slice_key" not in market_df.columns
    assert "x_axis" not in market_df.columns
    assert "atm_ref" not in market_df.columns
    assert "underlying_ref" not in market_df.columns

    coords_df, meta = handler.coords_frame()
    assert int(meta["snapshot_data_ts"]) == data_ts
    assert int(meta["snapshot_market_ts"]) == market_ts
    assert meta.get("x_axis") == "log_moneyness"
    assert float(meta.get("atm_ref") or 0.0) == 30_000.0
    assert float(meta.get("underlying_ref") or 0.0) == 30_000.0
    assert "snapshot_data_ts" not in coords_df.columns
    assert "snapshot_market_ts" not in coords_df.columns
    assert "x_axis" not in coords_df.columns
    assert "atm_ref" not in coords_df.columns
    assert "underlying_ref" not in coords_df.columns
    assert "instrument_name" in coords_df.columns
    assert "expiry_ts" in coords_df.columns
    assert "strike" in coords_df.columns
    assert "cp" in coords_df.columns
    assert "tau_ms" in coords_df.columns
    assert "x" in coords_df.columns
    assert set(coords_df.columns) - {"tau_ms", "x"} <= MARKET_COLS

    tau_target = expiry_ts - market_ts
    tau_df, tau_meta = handler.select_tau(tau_ms=int(tau_target))
    assert int(tau_meta["snapshot_data_ts"]) == data_ts
    assert int(tau_meta["snapshot_market_ts"]) == market_ts
    assert not tau_df.empty

    report = handler.qc_report()
    assert int(report["snapshot_data_ts"]) == data_ts
    assert report["policy_id"] == "v1"
    assert report["summary"]["n_rows"] == len(coords_df)
    assert meta.get("policy_id") == "v1"
    assert report["summary"]["ok"] == bool(meta.get("tradable"))


def test_qc_report_cache_hit_skips_recompute(monkeypatch) -> None:
    data_ts = 1_700_000_000_000
    market_ts = data_ts - 5_000
    expiry_ts = data_ts + 10_000
    handler = OptionChainDataHandler(symbol="BTC", interval="1m", preset="option_chain")
    frame = _make_frame(data_ts, market_ts, expiry_ts)
    tick = IngestionTick(
        timestamp=data_ts,
        data_ts=data_ts,
        domain="option_chain",
        symbol="BTC",
        payload={"data_ts": data_ts, "frame": frame},
        source_id=getattr(handler, "source_id", None),
    )
    handler.on_new_tick(tick)

    calls = {"n": 0}
    original = handler._coords_frame_uncached

    def _wrapped(*args, **kwargs):
        calls["n"] += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(handler, "_coords_frame_uncached", _wrapped)
    handler.qc_report()
    handler.qc_report()
    assert calls["n"] == 1
    assert len(handler._qc_keys_by_ts[int(data_ts)]) == 1


def test_select_tau_method_validation_and_cache_keys() -> None:
    data_ts = 1_700_000_000_000
    market_ts = data_ts - 5_000
    expiry_ts = data_ts + 10_000
    handler = OptionChainDataHandler(symbol="BTC", interval="1m", preset="option_chain")
    frame = _make_frame(data_ts, market_ts, expiry_ts)
    tick = IngestionTick(
        timestamp=data_ts,
        data_ts=data_ts,
        domain="option_chain",
        symbol="BTC",
        payload={"data_ts": data_ts, "frame": frame},
        source_id=getattr(handler, "source_id", None),
    )
    handler.on_new_tick(tick)

    target_tau = expiry_ts - market_ts
    df_a, _ = handler.select_tau(tau_ms=int(target_tau), method="nearest_bucket")
    assert not df_a.empty
    df_b, _ = handler.select_tau(tau_ms=int(target_tau), method="nearest_bucket")
    assert not df_b.empty
    assert len(handler._select_tau_keys_by_ts[int(data_ts)]) == 1

    try:
        handler.select_tau(tau_ms=int(target_tau), method="bad_method")
        assert False, "expected ValueError for invalid method"
    except ValueError:
        pass


def test_select_tau_cache_key_includes_hops() -> None:  # +
    data_ts = 1_700_000_000_000  # +
    market_ts = data_ts - 5_000  # +
    expiry_ts = data_ts + 10_000  # +
    handler = OptionChainDataHandler(symbol="BTC", interval="1m", preset="option_chain")  # +
    frame = _make_frame(data_ts, market_ts, expiry_ts)  # +
    tick = IngestionTick(  # +
        timestamp=data_ts,  # +
        data_ts=data_ts,  # +
        domain="option_chain",  # +
        symbol="BTC",  # +
        payload={"data_ts": data_ts, "frame": frame},  # +
        source_id=getattr(handler, "source_id", None),  # +
    )  # +
    handler.on_new_tick(tick)  # +
    target_tau = expiry_ts - market_ts  # +
    handler.select_tau(  # +
        tau_ms=int(target_tau),  # +
        method="nearest_bucket",  # +
        quality_mode=handler.quality_mode,  # +
        max_bucket_hops=0,  # +
    )  # +
    handler.select_tau(  # +
        tau_ms=int(target_tau),  # +
        method="nearest_bucket",  # +
        quality_mode=handler.quality_mode,  # +
        max_bucket_hops=1,  # +
    )  # +
    tau_def_s = str(handler.coords_cfg.get("tau_def"))  # +
    prefix_h0 = (int(data_ts), int(target_tau), "nearest_bucket", int(handler.term_bucket_ms), int(0), str(handler.quality_mode), tau_def_s)  # +
    prefix_h1 = (int(data_ts), int(target_tau), "nearest_bucket", int(handler.term_bucket_ms), int(1), str(handler.quality_mode), tau_def_s)  # +
    keys = list(handler._select_tau_cache.keys())  # +
    key_h0 = next((k for k in keys if tuple(k[:7]) == prefix_h0), None)  # +
    key_h1 = next((k for k in keys if tuple(k[:7]) == prefix_h1), None)  # +
    assert key_h0 is not None  # +
    assert key_h1 is not None  # +
    assert key_h0 != key_h1  # +
    # cache key must include liquidity config + row_policy_hash suffix
    assert len(key_h0) >= 13  # +
    assert key_h0[7] == bool(handler.liquidity_gate_cfg.get("enabled"))  # +
    keys_at_ts = handler._select_tau_keys_by_ts[int(data_ts)]  # +
    assert key_h0 in keys_at_ts  # +
    assert key_h1 in keys_at_ts  # +


def test_option_chain_empty_payload_skips_without_throw() -> None:
    data_ts = 1_700_000_000_000
    handler = OptionChainDataHandler(symbol="BTC", interval="1m", preset="option_chain", quality_mode="TRADING")
    tick = IngestionTick(
        timestamp=data_ts,
        data_ts=data_ts,
        domain="option_chain",
        symbol="BTC",
        payload={"data_ts": data_ts, "frame": pd.DataFrame()},
        source_id=getattr(handler, "source_id", None),
    )
    handler.on_new_tick(tick)
    assert handler.last_timestamp() is None


def test_option_chain_selection_avoids_dataframe_truthiness() -> None:
    data_ts = 1_700_000_000_000
    handler = OptionChainDataHandler(symbol="BTC", interval="1m", preset="option_chain", quality_mode="STRICT")
    tick = IngestionTick(
        timestamp=data_ts,
        data_ts=data_ts,
        domain="option_chain",
        symbol="BTC",
        payload={"data_ts": data_ts, "frame": pd.DataFrame()},
        source_id=getattr(handler, "source_id", None),
    )
    handler.on_new_tick(tick)
    df, meta = handler.select_tau(tau_ms=10_000)
    assert df.empty
    assert meta["state"] in {"HARD_FAIL", "SOFT_DEGRADED", "OK"}


def test_coords_frame_not_cached_and_market_frame_cached(monkeypatch) -> None:
    data_ts = 1_700_000_000_000
    market_ts = data_ts - 5_000
    expiry_ts = data_ts + 10_000
    handler = OptionChainDataHandler(symbol="BTC", interval="1m", preset="option_chain")
    frame = _make_frame(data_ts, market_ts, expiry_ts)
    tick = IngestionTick(
        timestamp=data_ts,
        data_ts=data_ts,
        domain="option_chain",
        symbol="BTC",
        payload={"data_ts": data_ts, "frame": frame},
        source_id=getattr(handler, "source_id", None),
    )
    handler.on_new_tick(tick)

    calls = {"n": 0}
    original = handler._coords_frame_uncached

    def _wrapped(*args, **kwargs):
        calls["n"] += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(handler, "_coords_frame_uncached", _wrapped)
    handler.coords_frame()
    handler.coords_frame()
    assert calls["n"] == 2

    handler.market_frame()
    handler.market_frame()
    assert len(handler._market_cache) == 1


def test_option_chain_handler_evicts_cached_keys_with_main_cache() -> None:
    handler = OptionChainDataHandler(
        symbol="BTC",
        interval="1m",
        preset="option_chain",
        cache={"maxlen": 2},
    )

    def _push(ts: int) -> tuple[int, int]:
        market_ts = ts - 5_000
        expiry_ts = ts + 10_000
        tick = IngestionTick(
            timestamp=ts,
            data_ts=ts,
            domain="option_chain",
            symbol="BTC",
            payload={"data_ts": ts, "frame": _make_frame(ts, market_ts, expiry_ts)},
            source_id=getattr(handler, "source_id", None),
        )
        handler.on_new_tick(tick)
        return market_ts, expiry_ts

    ts1 = 1_700_000_000_000
    ts2 = ts1 + 60_000
    ts3 = ts2 + 60_000

    market_ts_1, expiry_ts_1 = _push(ts1)
    handler.coords_frame(ts=ts1)
    handler.select_tau(ts=ts1, tau_ms=int(expiry_ts_1 - market_ts_1))

    market_ts_2, expiry_ts_2 = _push(ts2)
    handler.coords_frame(ts=ts2)
    handler.select_tau(ts=ts2, tau_ms=int(expiry_ts_2 - market_ts_2))

    _push(ts3)

    assert all(int(k[0]) != ts1 for k in handler._market_cache)
    assert all(int(k[0]) != ts1 for k in handler._coords_aux_cache)
    assert all(int(k[0]) != ts1 for k in handler._select_tau_cache)
    assert any(int(k[0]) == ts2 for k in handler._market_cache)


def test_select_point_cp_policy_same_prefers_same_cp_linear_x() -> None:
    data_ts = 1_700_000_000_000
    market_ts = data_ts - 5_000
    expiry_ts = data_ts + 86_400_000
    handler = OptionChainDataHandler(symbol="BTC", interval="1m", preset="option_chain")
    frame = pd.DataFrame(
        [
            {
                "instrument_name": "BTC-EXP-105-C",
                "expiration_timestamp": expiry_ts,
                "strike": 105,
                "cp": "C",
                "mark_price": 10.0,
                "market_ts": market_ts,
                "underlying_price": 100.0,
            },
            {
                "instrument_name": "BTC-EXP-115-C",
                "expiration_timestamp": expiry_ts,
                "strike": 115,
                "cp": "C",
                "mark_price": 12.0,
                "market_ts": market_ts,
                "underlying_price": 100.0,
            },
            {
                "instrument_name": "BTC-EXP-85-P",
                "expiration_timestamp": expiry_ts,
                "strike": 85,
                "cp": "P",
                "mark_price": 1.0,
                "market_ts": market_ts,
                "underlying_price": 100.0,
            },
            {
                "instrument_name": "BTC-EXP-95-P",
                "expiration_timestamp": expiry_ts,
                "strike": 95,
                "cp": "P",
                "mark_price": 2.0,
                "market_ts": market_ts,
                "underlying_price": 100.0,
            },
        ]
    )
    tick = IngestionTick(
        timestamp=data_ts,
        data_ts=data_ts,
        domain="option_chain",
        symbol="BTC",
        payload={"data_ts": data_ts, "frame": frame},
        source_id=getattr(handler, "source_id", None),
    )
    handler.on_new_tick(tick)

    target_tau = expiry_ts - market_ts
    point, meta = handler.select_point(
        tau_ms=int(target_tau),
        x=0.02,
        x_axis="log_moneyness",
        interp="linear_x",
        cp_policy="same",
        price_field="mark_price",
    )
    assert point is not None
    assert int(point["ts"]) == data_ts
    value = point["value_fields"]["mark_price"]
    assert value > 5.0
    assert meta["state"] in {"OK", "SOFT_DEGRADED", "HARD_FAIL"}


def test_window_for_tau_selection_weight_propagates() -> None:
    data_ts = 1_700_000_000_000
    market_ts = data_ts - 5_000
    expiry_a = data_ts + 20_000
    expiry_b = data_ts + 40_000
    handler = OptionChainDataHandler(symbol="BTC", interval="1m", preset="option_chain")
    frame = pd.DataFrame(
        [
            {
                "instrument_name": "BTC-A",
                "expiration_timestamp": expiry_a,
                "strike": 10_000,
                "option_type": "call",
                "mark_price": 1.0,
                "market_ts": market_ts,
                "underlying_price": 30_000.0,
            },
            {
                "instrument_name": "BTC-B",
                "expiration_timestamp": expiry_b,
                "strike": 10_000,
                "option_type": "call",
                "mark_price": 2.0,
                "market_ts": market_ts,
                "underlying_price": 30_000.0,
            },
        ]
    )
    tick = IngestionTick(
        timestamp=data_ts,
        data_ts=data_ts,
        domain="option_chain",
        symbol="BTC",
        payload={"data_ts": data_ts, "frame": frame},
        source_id=getattr(handler, "source_id", None),
    )
    handler.on_new_tick(tick)

    target_tau = int((expiry_a - market_ts + expiry_b - market_ts) / 2)
    _, meta = handler.select_tau(tau_ms=target_tau, method="bracket")
    views = handler.window_for_tau(
        ts_start=data_ts,
        ts_end=data_ts,
        tau_ms=target_tau,
        step_ms=1,
        method="bracket",
    )
    assert len(views) == 1
    view = views[0]
    selection = view.selection or {} # type: ignore
    weights = meta["selection"]["weights"]
    selected = meta["selection"]["selected_expiries"]
    assert selection.get("weights") == weights
    assert selection.get("selected_expiries") == selected


def test_missing_market_ts_sets_reason_and_tau_anchor() -> None:
    data_ts = 1_700_000_000_000
    expiry_ts = data_ts + 10_000
    handler = OptionChainDataHandler(symbol="BTC", interval="1m", preset="option_chain")
    frame = pd.DataFrame(
        [
            {
                "instrument_name": "BTC-EXP-1",
                "expiration_timestamp": expiry_ts,
                "strike": 10_000,
                "option_type": "call",
                "mark_price": 1.0,
                "underlying_price": 30_000.0,
            }
        ]
    )
    tick = IngestionTick(
        timestamp=data_ts,
        data_ts=data_ts,
        domain="option_chain",
        symbol="BTC",
        payload={"data_ts": data_ts, "frame": frame},
        source_id=getattr(handler, "source_id", None),
    )
    handler.on_new_tick(tick)

    _, meta = handler.coords_frame(tau_def="market_ts")
    reason_codes = {r["reason_code"] for r in meta["reasons"]}
    assert "MISSING_MARKET_TS" in reason_codes
    assert meta["tau_anchor_ts"] == data_ts
    assert meta["market_ts_ref_method"] == "missing"


def test_quality_spread_max_no_longer_emits_chain_wide_spread_reason() -> None:
    original = GLOBAL_PRESETS["option_chain"]["quality"]["spread_max"]
    try:
        GLOBAL_PRESETS["option_chain"]["quality"]["spread_max"] = 0.01
        data_ts = 1_700_000_000_000
        market_ts = data_ts - 5_000
        expiry_ts = data_ts + 10_000
        handler = OptionChainDataHandler(symbol="BTC", interval="1m", preset="option_chain")
        frame = pd.DataFrame(
            [
                {
                    "instrument_name": "BTC-EXP-1",
                    "expiration_timestamp": expiry_ts,
                    "strike": 10_000,
                    "option_type": "call",
                    "bid_price": 1.0,
                    "ask_price": 1.2,
                    "market_ts": market_ts,
                    "underlying_price": 30_000.0,
                }
            ]
        )
        tick = IngestionTick(
            timestamp=data_ts,
            data_ts=data_ts,
            domain="option_chain",
            symbol="BTC",
            payload={"data_ts": data_ts, "frame": frame},
            source_id=getattr(handler, "source_id", None),
        )
        handler.on_new_tick(tick)
        _, meta = handler.coords_frame()
        reason_codes = {r["reason_code"] for r in meta["reasons"]}
        # Chain-level spread reason is removed; selection-level liquidity gate owns spread checks.
        assert "WIDE_SPREAD" not in reason_codes
        assert bool(meta.get("tradable")) is True

        GLOBAL_PRESETS["option_chain"]["quality"]["spread_max"] = 1.0
        handler_2 = OptionChainDataHandler(symbol="BTC", interval="1m", preset="option_chain")
        handler_2.on_new_tick(tick)
        _, meta_2 = handler_2.coords_frame()
        reason_codes_2 = {r["reason_code"] for r in meta_2["reasons"]}
        assert "WIDE_SPREAD" not in reason_codes_2
    finally:
        GLOBAL_PRESETS["option_chain"]["quality"]["spread_max"] = original


def test_term_bucket_ms_uses_global_preset_value() -> None:
    original_term = GLOBAL_PRESETS["option_chain"]["term_bucket_ms"]
    original_cache_term = GLOBAL_PRESETS["option_chain"]["cache"]["term_bucket_ms"]
    try:
        GLOBAL_PRESETS["option_chain"]["term_bucket_ms"] = 12_345
        GLOBAL_PRESETS["option_chain"]["cache"]["term_bucket_ms"] = 12_345
        handler = OptionChainDataHandler(symbol="BTC", interval="1m", preset="option_chain")
        assert handler.term_bucket_ms == 12_345
        _, meta = handler.select_tau(tau_ms=10_000)
        assert meta["selection_context"]["term_bucket_ms"] == 12_345
    finally:
        GLOBAL_PRESETS["option_chain"]["term_bucket_ms"] = original_term
        GLOBAL_PRESETS["option_chain"]["cache"]["term_bucket_ms"] = original_cache_term


# ---------------------------------------------------------------------------
# Phase 5: Row-mask QC patch tests
# ---------------------------------------------------------------------------


def _push_tick(handler, data_ts, frame):
    """Helper: push a single tick into handler."""
    tick = IngestionTick(
        timestamp=data_ts,
        data_ts=data_ts,
        domain="option_chain",
        symbol="BTC",
        payload={"data_ts": data_ts, "frame": frame},
        source_id=getattr(handler, "source_id", None),
    )
    handler.on_new_tick(tick)


def test_snapshot_row_annotation_lengths_match_chain_rows() -> None:
    data_ts = 1_700_000_000_000
    market_ts = data_ts - 5_000
    expiry_ts = data_ts + 10_000
    handler = OptionChainDataHandler(symbol="BTC", interval="1m", preset="option_chain")
    _push_tick(handler, data_ts, _make_frame(data_ts, market_ts, expiry_ts))

    snap = handler.get_snapshot(data_ts)
    assert snap is not None
    assert snap.row_flags is not None
    assert snap.row_mask is not None
    assert len(snap.row_mask) == len(snap.chain_frame)
    assert len(snap.row_flags) == len(snap.chain_frame)
    assert np.issubdtype(snap.row_flags.dtype, np.unsignedinteger)
    assert snap.row_mask.dtype == np.dtype("bool")
    assert isinstance(snap.row_policy_hash, str)
    assert len(snap.row_policy_hash) > 0


def test_row_annotation_is_deterministic_given_same_input() -> None:
    data_ts = 1_700_000_000_000
    market_ts = data_ts - 5_000
    expiry_ts = data_ts + 10_000
    frame = _make_frame(data_ts, market_ts, expiry_ts)

    handler_a = OptionChainDataHandler(symbol="BTC", interval="1m", preset="option_chain")
    _push_tick(handler_a, data_ts, frame.copy())
    snap_a = handler_a.get_snapshot(data_ts)

    handler_b = OptionChainDataHandler(symbol="BTC", interval="1m", preset="option_chain")
    _push_tick(handler_b, data_ts, frame.copy())
    snap_b = handler_b.get_snapshot(data_ts)
    assert snap_a is not None
    assert snap_b is not None
    assert snap_a.row_policy_hash == snap_b.row_policy_hash
    np.testing.assert_array_equal(snap_a.row_flags, snap_b.row_flags) # type: ignore
    np.testing.assert_array_equal(snap_a.row_mask, snap_b.row_mask) # type: ignore


def test_qc_report_includes_mask_coverage_fields() -> None:
    data_ts = 1_700_000_000_000
    market_ts = data_ts - 5_000
    expiry_ts = data_ts + 10_000
    handler = OptionChainDataHandler(symbol="BTC", interval="1m", preset="option_chain")
    _push_tick(handler, data_ts, _make_frame(data_ts, market_ts, expiry_ts))

    report = handler.qc_report()
    summary = report["summary"]
    assert summary["n_rows"] >= 1
    assert "n_eligible_rows" in summary
    assert summary["n_eligible_rows"] <= summary["n_rows"]
    assert summary["qc_scope"] == "masked_rows"
    assert isinstance(summary.get("mask_coverage"), float)
    assert 0.0 <= summary["mask_coverage"] <= 1.0
    assert isinstance(report.get("row_policy_hash"), str)


def test_qc_cache_key_changes_with_row_policy() -> None:
    data_ts = 1_700_000_000_000
    market_ts = data_ts - 5_000
    expiry_ts = data_ts + 10_000
    frame = _make_frame(data_ts, market_ts, expiry_ts)

    handler_a = OptionChainDataHandler(symbol="BTC", interval="1m", preset="option_chain")
    _push_tick(handler_a, data_ts, frame.copy())

    original_rp = GLOBAL_PRESETS["option_chain"]["quality"]["row_policy"]
    try:
        custom_rp = dict(original_rp)
        custom_rp["min_open_interest"] = 100.0
        GLOBAL_PRESETS["option_chain"]["quality"]["row_policy"] = custom_rp
        handler_b = OptionChainDataHandler(symbol="BTC", interval="1m", preset="option_chain")
        _push_tick(handler_b, data_ts, frame.copy())

        assert handler_a.row_policy_hash != handler_b.row_policy_hash
    finally:
        GLOBAL_PRESETS["option_chain"]["quality"]["row_policy"] = original_rp


def test_tail_rows_do_not_trigger_wide_spread_when_masked_out() -> None:
    data_ts = 1_700_000_000_000
    market_ts = data_ts - 5_000
    expiry_ts = data_ts + 86_400_000

    # 2 clean near-ATM rows + 2 dust tails (missing bid → masked out)
    frame = pd.DataFrame(
        [
            {
                "instrument_name": "BTC-EXP-C1",
                "expiration_timestamp": expiry_ts,
                "strike": 30_000,
                "option_type": "call",
                "bid_price": 1.0,
                "ask_price": 1.02,
                "mark_price": 1.01,
                "market_ts": market_ts,
                "underlying_price": 30_000.0,
                "open_interest": 10.0,
            },
            {
                "instrument_name": "BTC-EXP-P1",
                "expiration_timestamp": expiry_ts,
                "strike": 30_000,
                "option_type": "put",
                "bid_price": 1.0,
                "ask_price": 1.02,
                "mark_price": 1.01,
                "market_ts": market_ts,
                "underlying_price": 30_000.0,
                "open_interest": 10.0,
            },
            {
                "instrument_name": "BTC-EXP-C2",
                "expiration_timestamp": expiry_ts,
                "strike": 100_000,
                "option_type": "call",
                "bid_price": float("nan"),
                "ask_price": 0.0001,
                "mark_price": 0.0001,
                "market_ts": market_ts,
                "underlying_price": 30_000.0,
                "open_interest": 0.0,
            },
            {
                "instrument_name": "BTC-EXP-P2",
                "expiration_timestamp": expiry_ts,
                "strike": 1_000,
                "option_type": "put",
                "bid_price": 0.0,
                "ask_price": 0.0001,
                "mark_price": 0.0001,
                "market_ts": market_ts,
                "underlying_price": 30_000.0,
                "open_interest": 0.0,
            },
        ]
    )

    handler = OptionChainDataHandler(symbol="BTC", interval="1m", preset="option_chain")
    _push_tick(handler, data_ts, frame)
    _, meta = handler.coords_frame()
    reason_codes = {r["reason_code"] for r in meta.get("reasons", [])}
    # Dust tails masked out → spread computed only on clean rows → no WIDE_SPREAD
    assert "WIDE_SPREAD" not in reason_codes
    assert meta["coverage"]["qc_scope"] == "masked_rows"


def test_fallback_to_full_rows_when_annotation_missing() -> None:
    data_ts = 1_700_000_000_000
    market_ts = data_ts - 5_000
    expiry_ts = data_ts + 10_000
    handler = OptionChainDataHandler(symbol="BTC", interval="1m", preset="option_chain")
    _push_tick(handler, data_ts, _make_frame(data_ts, market_ts, expiry_ts))

    # Forcibly strip annotation from cached snapshot
    snap = handler.get_snapshot(data_ts)
    from dataclasses import replace as _dc_replace

    bare_snap = _dc_replace(
        snap, # type: ignore
        row_flags=None,
        row_mask=None,
        row_policy_hash=None,
    )
    object.__setattr__(bare_snap, "_frame_cache", None)
    object.__setattr__(bare_snap, "_market_ts_ref", None)
    # Overwrite in cache buffer (deque)
    buf = handler.cache.main.buffer # type: ignore
    for i, s in enumerate(buf):
        if int(s.data_ts) == data_ts:
            buf[i] = bare_snap
            break

    _, meta = handler.coords_frame()
    assert meta["coverage"].get("qc_scope") == "full_rows"
    assert meta["coverage"]["n_rows"] >= 1


def test_cache_eviction_still_removes_qc_entries_with_extended_keys() -> None:
    handler = OptionChainDataHandler(
        symbol="BTC",
        interval="1m",
        preset="option_chain",
        cache={"maxlen": 2},
    )

    def _push_ts(ts: int) -> tuple[int, int]:
        market_ts = ts - 5_000
        expiry_ts = ts + 10_000
        _push_tick(handler, ts, _make_frame(ts, market_ts, expiry_ts))
        return market_ts, expiry_ts

    ts1 = 1_700_000_000_000
    ts2 = ts1 + 60_000
    ts3 = ts2 + 60_000

    _push_ts(ts1)
    handler.coords_frame(ts=ts1)

    _push_ts(ts2)
    handler.coords_frame(ts=ts2)

    _push_ts(ts3)

    # ts1 should have been evicted
    assert all(int(k[0]) != ts1 for k in handler._qc_cache)
    assert ts1 not in handler._qc_keys_by_ts
    # ts2 should still exist
    assert any(int(k[0]) == ts2 for k in handler._qc_cache)


def test_row_flags_bit_registry_stability() -> None:
    data_ts = 1_700_000_000_000
    market_ts = data_ts - 5_000
    expiry_ts = data_ts + 86_400_000

    frame = pd.DataFrame(
        [
            {   # (a) missing bid → _ROW_FLAG_MISSING_BID_ASK
                "instrument_name": "BTC-A",
                "expiration_timestamp": expiry_ts,
                "strike": 50_000,
                "option_type": "call",
                "bid_price": float("nan"),
                "ask_price": 1.0,
                "mark_price": 1.0,
                "market_ts": market_ts,
                "underlying_price": 30_000.0,
                "open_interest": 10.0,
            },
            {   # (b) crossed quote → _ROW_FLAG_CROSSED_OR_LOCKED
                "instrument_name": "BTC-B",
                "expiration_timestamp": expiry_ts,
                "strike": 30_000,
                "option_type": "call",
                "bid_price": 2.0,
                "ask_price": 1.5,
                "mark_price": 1.75,
                "market_ts": market_ts,
                "underlying_price": 30_000.0,
                "open_interest": 10.0,
            },
            {   # (c) zero OI → _ROW_FLAG_OI_BELOW_FLOOR (only with min_open_interest > 0)
                "instrument_name": "BTC-C",
                "expiration_timestamp": expiry_ts,
                "strike": 30_000,
                "option_type": "put",
                "bid_price": 1.0,
                "ask_price": 1.1,
                "mark_price": 1.05,
                "market_ts": market_ts,
                "underlying_price": 30_000.0,
                "open_interest": 0.0,
            },
            {   # (d) clean row → flags == 0, mask == True
                "instrument_name": "BTC-D",
                "expiration_timestamp": expiry_ts,
                "strike": 31_000,
                "option_type": "call",
                "bid_price": 1.0,
                "ask_price": 1.1,
                "mark_price": 1.05,
                "market_ts": market_ts,
                "underlying_price": 30_000.0,
                "open_interest": 10.0,
            },
        ]
    )

    # Use min_open_interest > 0 to trigger OI flag
    original_rp = GLOBAL_PRESETS["option_chain"]["quality"]["row_policy"]
    try:
        custom_rp = dict(original_rp)
        custom_rp["min_open_interest"] = 1.0
        GLOBAL_PRESETS["option_chain"]["quality"]["row_policy"] = custom_rp
        handler = OptionChainDataHandler(symbol="BTC", interval="1m", preset="option_chain")
        _push_tick(handler, data_ts, frame)
        snap = handler.get_snapshot(data_ts)

        flags = snap.row_flags # type: ignore
        mask = snap.row_mask # type: ignore

        # Resolve positions by instrument_name (row order may change after split)
        merged = snap.frame.reset_index(drop=True) # type: ignore
        idx = {name: i for i, name in enumerate(merged["instrument_name"])}
        assert flags is not None
        assert mask is not None
        # (a) missing bid
        a = idx["BTC-A"]
        assert flags[a] & _ROW_FLAG_MISSING_BID_ASK != 0
        assert mask[a] == False  # noqa: E712

        # (b) crossed quote
        b = idx["BTC-B"]
        assert flags[b] & _ROW_FLAG_CROSSED_OR_LOCKED != 0
        assert mask[b] == False  # noqa: E712

        # (c) zero OI with min_open_interest=1.0
        c = idx["BTC-C"]
        assert flags[c] & _ROW_FLAG_OI_BELOW_FLOOR != 0
        assert mask[c] == False  # noqa: E712

        # (d) clean row
        d = idx["BTC-D"]
        assert flags[d] == 0
        assert mask[d] == True  # noqa: E712

        # No bit overlap: each condition sets its own unique bit
        assert _ROW_FLAG_MISSING_BID_ASK != _ROW_FLAG_CROSSED_OR_LOCKED
        assert _ROW_FLAG_CROSSED_OR_LOCKED != _ROW_FLAG_OI_BELOW_FLOOR
    finally:
        GLOBAL_PRESETS["option_chain"]["quality"]["row_policy"] = original_rp


def test_on_new_tick_does_not_access_snap_frame(monkeypatch) -> None:
    """Regression: on_new_tick must use positional sub-frame extraction, not snap.frame merge."""
    data_ts = 1_700_000_000_000
    market_ts = data_ts - 5_000
    expiry_ts = data_ts + 86_400_000
    handler = OptionChainDataHandler(symbol="BTC", interval="1m", preset="option_chain")
    frame = _make_frame(data_ts, market_ts, expiry_ts)

    frame_calls = {"n": 0}
    _orig_frame = OptionChainSnapshot.frame.fget

    def _counting_frame(self):
        frame_calls["n"] += 1
        return _orig_frame(self) # type: ignore

    monkeypatch.setattr(OptionChainSnapshot, "frame", property(_counting_frame))

    _push_tick(handler, data_ts, frame)

    assert frame_calls["n"] == 0, "snap.frame should not be accessed during on_new_tick"
    snap = handler.cache.last()
    assert snap is not None
    assert snap.row_mask is not None, "row annotation must still be applied"


def test_select_tau_liquidity_gate_passes_for_tight_near_atm_spreads() -> None:
    data_ts = 1_700_000_000_000
    market_ts = data_ts - 5_000
    expiry_ts = market_ts + (7 * 24 * 60 * 60 * 1000)
    rows = []
    for i, strike in enumerate([28_500, 29_000, 29_500, 30_000, 30_500, 31_000, 31_500, 32_000]):
        rows.append(
            {
                "instrument_name": f"BTC-LQ-{i}",
                "expiration_timestamp": expiry_ts,
                "strike": strike,
                "option_type": "call" if i % 2 == 0 else "put",
                "bid_price": 1.00,
                "ask_price": 1.02,
                "mid_price": 1.01,
                "mark_price": 1.01,
                "market_ts": market_ts,
                "underlying_price": 30_000.0,
                "open_interest": 20.0,
            }
        )
    handler = OptionChainDataHandler(symbol="BTC", interval="1m", preset="option_chain")
    _push_tick(handler, data_ts, pd.DataFrame(rows))
    _, meta = handler.select_tau(tau_ms=int(expiry_ts - market_ts), method="nearest_bucket")
    liq = meta.get("liquidity") or {}
    assert bool(liq.get("enabled")) is True
    assert bool(liq.get("ok")) is True
    assert liq.get("state") == "OK"
    assert int(liq.get("n_corridor") or 0) >= int(liq.get("min_n") or 0)
    reason_codes = {r["reason_code"] for r in meta.get("reasons", [])}
    assert "LIQUIDITY_COVERAGE_LOW" not in reason_codes
    assert "LIQUIDITY_SPREAD_WIDE" not in reason_codes


def test_select_tau_liquidity_gate_fails_for_wide_near_atm_spreads() -> None:
    data_ts = 1_700_000_060_000
    market_ts = data_ts - 5_000
    expiry_ts = market_ts + (7 * 24 * 60 * 60 * 1000)
    rows = []
    for i, strike in enumerate([28_500, 29_000, 29_500, 30_000, 30_500, 31_000, 31_500, 32_000]):
        rows.append(
            {
                "instrument_name": f"BTC-WIDE-{i}",
                "expiration_timestamp": expiry_ts,
                "strike": strike,
                "option_type": "call" if i % 2 == 0 else "put",
                "bid_price": 1.00,
                "ask_price": 2.00,
                "mid_price": 1.50,
                "mark_price": 1.50,
                "market_ts": market_ts,
                "underlying_price": 30_000.0,
                "open_interest": 20.0,
            }
        )
    handler = OptionChainDataHandler(symbol="BTC", interval="1m", preset="option_chain")
    _push_tick(handler, data_ts, pd.DataFrame(rows))
    _, meta = handler.select_tau(tau_ms=int(expiry_ts - market_ts), method="nearest_bucket")
    liq = meta.get("liquidity") or {}
    assert bool(liq.get("ok")) is False
    reason_codes = {r["reason_code"] for r in meta.get("reasons", [])}
    assert "LIQUIDITY_SPREAD_WIDE" in reason_codes


def test_select_tau_liquidity_gate_fails_for_sparse_corridor() -> None:
    data_ts = 1_700_000_120_000
    market_ts = data_ts - 5_000
    expiry_ts = market_ts + (30 * 24 * 60 * 60 * 1000)
    # Most rows are far from ATM; only a couple remain in |x| <= 0.10 corridor.
    rows = []
    for i, strike in enumerate([10_000, 12_000, 15_000, 18_000, 45_000, 50_000, 30_000, 31_000]):
        rows.append(
            {
                "instrument_name": f"BTC-SPARSE-{i}",
                "expiration_timestamp": expiry_ts,
                "strike": strike,
                "option_type": "call" if i % 2 == 0 else "put",
                "bid_price": 1.00,
                "ask_price": 1.02,
                "mid_price": 1.01,
                "mark_price": 1.01,
                "market_ts": market_ts,
                "underlying_price": 30_000.0,
                "open_interest": 20.0,
            }
        )
    handler = OptionChainDataHandler(symbol="BTC", interval="1m", preset="option_chain")
    _push_tick(handler, data_ts, pd.DataFrame(rows))
    _, meta = handler.select_tau(tau_ms=int(expiry_ts - market_ts), method="nearest_bucket")
    liq = meta.get("liquidity") or {}
    assert bool(liq.get("ok")) is False
    reason_codes = {r["reason_code"] for r in meta.get("reasons", [])}
    assert "LIQUIDITY_COVERAGE_LOW" in reason_codes


def test_select_tau_cache_key_changes_with_liquidity_config() -> None:
    data_ts = 1_700_000_180_000
    market_ts = data_ts - 5_000
    expiry_ts = data_ts + 10_000
    frame = _make_frame(data_ts, market_ts, expiry_ts)

    handler_a = OptionChainDataHandler(symbol="BTC", interval="1m", preset="option_chain")
    _push_tick(handler_a, data_ts, frame.copy())
    handler_a.select_tau(tau_ms=int(expiry_ts - market_ts), method="nearest_bucket")
    keys_a = list(handler_a._select_tau_cache.keys())

    original_lg = GLOBAL_PRESETS["option_chain"]["quality"]["liquidity_gate"]
    try:
        lg = dict(original_lg)
        lg["x_max"] = 0.05
        GLOBAL_PRESETS["option_chain"]["quality"]["liquidity_gate"] = lg
        handler_b = OptionChainDataHandler(symbol="BTC", interval="1m", preset="option_chain")
        _push_tick(handler_b, data_ts, frame.copy())
        handler_b.select_tau(tau_ms=int(expiry_ts - market_ts), method="nearest_bucket")
        keys_b = list(handler_b._select_tau_cache.keys())
    finally:
        GLOBAL_PRESETS["option_chain"]["quality"]["liquidity_gate"] = original_lg

    assert len(keys_a) == 1 and len(keys_b) == 1
    assert keys_a[0] != keys_b[0]
