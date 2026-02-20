from __future__ import annotations

import pandas as pd

from ingestion.contracts.tick import IngestionTick
from quant_engine.data.derivatives.option_chain.chain_handler import OptionChainDataHandler
from quant_engine.strategy.base import GLOBAL_PRESETS


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
    key_h0 = (int(data_ts), int(target_tau), "nearest_bucket", int(handler.term_bucket_ms), int(0), str(handler.quality_mode), tau_def_s)  # +
    key_h1 = (int(data_ts), int(target_tau), "nearest_bucket", int(handler.term_bucket_ms), int(1), str(handler.quality_mode), tau_def_s)  # +
    assert key_h0 in handler._select_tau_cache  # +
    assert key_h1 in handler._select_tau_cache  # +
    assert key_h0 != key_h1  # +
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
    selection = view.selection or {}
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


def test_quality_spread_max_from_global_preset_changes_reason() -> None:
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
        assert "WIDE_SPREAD" in reason_codes

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
