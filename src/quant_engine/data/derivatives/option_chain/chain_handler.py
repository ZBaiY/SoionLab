from __future__ import annotations

from collections import deque
import time
from typing import Any, Deque, Dict, List, Optional

import pandas as pd

from quant_engine.data.contracts.protocol_realtime import RealTimeDataHandler, to_interval_ms
from quant_engine.data.derivatives.option_chain.option_chain import OptionChain
from quant_engine.data.derivatives.option_chain.option_contract import OptionContract, OptionType
from quant_engine.data.derivatives.option_chain.snapshot import OptionChainSnapshot
from quant_engine.utils.logger import get_logger, log_debug, log_info


class OptionChainDataHandler(RealTimeDataHandler):
    """Runtime option chain handler (mode-agnostic).

    Config (Strategy.DATA.*.option_chain):
      - source: routing/metadata (default: "deribit")
      - interval: required cadence (e.g. "1m", "5m")
      - bootstrap.lookback: convenience horizon for Engine.bootstrap()
      - cache.max_bars: in-memory cache depth (OptionChainSnapshot)

    IO boundary:
      - IO-free by default.

    Anti-lookahead:
      - warmup_to(ts) sets anchor; get_snapshot/window clamp to it by default.

    Note:
      - Internal state keeps per-expiry OptionChain for incremental updates,
        but the engine-facing surface is timestamped OptionChainSnapshot.
    """

    # --- declared attributes (protocol/typing shadow) ---
    symbol: str
    interval: str
    bootstrap_cfg: dict[str, Any]
    cache_cfg: dict[str, Any]
    chains: Dict[str, OptionChain]
    interval_ms: int
    _snapshots: Deque[OptionChainSnapshot]
    _anchor_ts: int | None
    _logger: Any

    def __init__(self, symbol: str, **kwargs: Any):
        self._logger = get_logger(self.__class__.__name__)
        self.symbol = symbol

        ri = kwargs.get("interval")
        if not isinstance(ri, str) or not ri:
            raise ValueError("OptionChain handler requires non-empty 'interval' (e.g. '1m')")
        self.interval = ri
        ri_ms = to_interval_ms(self.interval)
        if ri_ms is None:
            raise ValueError(f"Invalid interval format: {self.interval}")
        self.interval_ms = int(ri_ms)
        
        bootstrap = kwargs.get("bootstrap") or {}
        if not isinstance(bootstrap, dict):
            raise TypeError("OptionChain 'bootstrap' must be a dict")
        self.bootstrap_cfg = dict(bootstrap)

        cache = kwargs.get("cache") or {}
        if not isinstance(cache, dict):
            raise TypeError("OptionChain 'cache' must be a dict")
        self.cache_cfg = dict(cache)

        max_bars = self.cache_cfg.get("max_bars")
        if max_bars is None:
            max_bars = kwargs.get("window", 1000)
        max_bars_i = int(max_bars)
        if max_bars_i <= 0:
            raise ValueError("OptionChain cache.max_bars must be > 0")

        self._snapshots = deque(maxlen=max_bars_i)
        self.chains = {}
        self._anchor_ts = None

        log_debug(
            self._logger,
            "OptionChainDataHandler initialized",
            symbol=self.symbol,
            interval=self.interval,
            max_bars=max_bars_i,
            bootstrap=self.bootstrap_cfg,
        )

    # ----------------------------------------------------------------------
    # Protocol lifecycle
    # ----------------------------------------------------------------------

    def bootstrap(self, *, anchor_ts: int | None = None, lookback: Any | None = None) -> None:
        if lookback is None:
            lookback = self.bootstrap_cfg.get("lookback")
        log_debug(
            self._logger,
            "OptionChainDataHandler.bootstrap (no-op)",
            symbol=self.symbol,
            anchor_ts=anchor_ts,
            lookback=lookback,
        )

    # align_to(ts) defines the maximum visible engine-time for all read APIs.
    def align_to(self, ts: int) -> None:
        self._anchor_ts = int(ts)
        log_debug(self._logger, "OptionChainDataHandler align_to", symbol=self.symbol, anchor_ts=self._anchor_ts)

    def last_timestamp(self) -> int | None:
        if not self._snapshots:
            return None
        last_ts = int(self._snapshots[-1].timestamp)
        if self._anchor_ts is not None:
            return min(last_ts, int(self._anchor_ts))
        return last_ts

    def get_snapshot(self, ts: int | None = None) -> OptionChainSnapshot | None:
        if ts is None:
            ts = self._anchor_ts if self._anchor_ts is not None else self.last_timestamp()
            if ts is None:
                return None

        t = min(int(ts), int(self._anchor_ts)) if self._anchor_ts is not None else int(ts)
        for snap in reversed(self._snapshots):
            if int(snap.timestamp) <= t:
                return snap
        return None

    def window(self, ts: int | None = None, n: int = 1) -> list[OptionChainSnapshot]:
        if ts is None:
            ts = self._anchor_ts if self._anchor_ts is not None else self.last_timestamp()
            if ts is None:
                return []

        t = min(int(ts), int(self._anchor_ts)) if self._anchor_ts is not None else int(ts)
        out: list[OptionChainSnapshot] = []
        for snap in reversed(self._snapshots):
            if int(snap.timestamp) <= t:
                out.append(snap)
                if len(out) >= int(n):
                    break
        out.reverse()
        return out

    def on_new_tick(self, bar: Any) -> None:
        """
        Ingest an option-chain payload (event-time fact).

        Payload contract:
          - Represents an already-occurred option-chain observation.
          - May be:
              * OptionChainSnapshot
              * pandas.DataFrame
              * dict with resolvable event-time and chain payload
          - Ingest is append-only and unconditional.
          - No visibility or engine-time decisions are made here.
        """
        snap = _coerce_snapshot(self.symbol, bar)
        if snap is None:
            return
        self._snapshots.append(snap)

    def reset(self) -> None:
        self._snapshots.clear()
        self.chains.clear()

    # ----------------------------------------------------------------------
    # Existing API (kept for compatibility)
    # ----------------------------------------------------------------------

    def load_initial(self, data: Any) -> None:
        """Load initial option chains.

        Accepts:
          - List[OptionChain]
          - {"chains": [...], "timestamp": ...}
        """
        if isinstance(data, dict) and "chains" in data:
            chains = data["chains"]
            ts = data.get("timestamp")
        else:
            chains = data
            ts = None

        for i, chain in enumerate(chains or []):
            if i == 0:
                self.symbol = chain.symbol
            self.chains[chain.expiry] = chain

        # push a snapshot if possible
        df = self.get_latest_snapshot()
        if not df.empty:
            chain_ts = int(ts) if ts is not None else int(df["timestamp"].iloc[0]) if "timestamp" in df.columns else int(time.time() * 1000)
            self._snapshots.append(OptionChainSnapshot.from_chain(chain_ts, df, self.symbol))


    # LEGACY API (pre-v4): prefer on_new_tick(payload)
    def on_new_snapshot(self, df: pd.DataFrame) -> None:
        """Receive a full option-chain snapshot from exchange (DataFrame)."""
        log_debug(self._logger, "Received new full option chain snapshot", rows=len(df))

        if df.empty:
            return

        # Update internal per-expiry chains (optional, for incremental contract access)
        if "expiry" in df.columns:
            for expiry in df["expiry"].unique():
                sub = df[df.expiry == expiry]
                self.chains[str(expiry)] = self._df_to_chain(sub)

        # Push v4 snapshot
        chain_ts = int(df["timestamp"].iloc[0]) if "timestamp" in df.columns else int(time.time() * 1000)
        self._snapshots.append(OptionChainSnapshot.from_chain(chain_ts, df, self.symbol))

    def update_contract(self, expiry: str, strike: float, option_type: OptionType, **fields: Any) -> None:
        chain = self.chains.get(expiry)
        if chain is None:
            chain = OptionChain(symbol=self.symbol, expiry=expiry)
            self.chains[expiry] = chain

        contract = chain.get_contract(strike, option_type)
        if contract:
            for k, v in fields.items():
                if hasattr(contract, k):
                    setattr(contract, k, v)
        else:
            new_c = OptionContract(
                symbol=chain.symbol,
                expiry=expiry,
                strike=strike,
                option_type=option_type,
                bid=fields.get("bid"),
                ask=fields.get("ask"),
                last=fields.get("last"),
                volume=fields.get("volume"),
                open_interest=fields.get("open_interest") or fields.get("oi"),
                implied_vol=fields.get("iv") or fields.get("implied_vol"),
                delta=fields.get("delta"),
                gamma=fields.get("gamma"),
                vega=fields.get("vega"),
                theta=fields.get("theta"),
            )
            chain.contracts.append(new_c)

    def get_chain(self, expiry: str) -> Optional[OptionChain]:
        return self.chains.get(expiry)

    def get_all_expiries(self) -> List[str]:
        return sorted(self.chains.keys())

    def get_latest_snapshot(self) -> pd.DataFrame:
        frames = [c.to_dataframe() for c in self.chains.values()]
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def snapshot_by_expiry(self, expiry: str) -> pd.DataFrame:
        chain = self.chains.get(expiry)
        return pd.DataFrame() if chain is None else chain.to_dataframe()

    def snapshot_all(self) -> Dict[str, pd.DataFrame]:
        return {expiry: chain.to_dataframe() for expiry, chain in self.chains.items()}

    def get_contract(self, expiry: str, strike: float, option_type: OptionType) -> OptionContract | None:
        chain = self.chains.get(expiry)
        return None if chain is None else chain.get_contract(strike, option_type)

    def cleanup_expired(self, current_timestamp: str) -> None:
        expired = [e for e in self.chains.keys() if e < current_timestamp]
        for e in expired:
            log_info(self._logger, "Removing expired option chain", expiry=e)
            del self.chains[e]

    def on_tick(self, tick: Dict[str, Any]) -> None:
        expiry_raw = tick.get("expiry")
        strike_raw = tick.get("strike")
        type_raw = tick.get("type")

        if not isinstance(expiry_raw, str):
            return
        try:
            opt_type = OptionType(type_raw)
        except Exception:
            return

        fields = {k: v for k, v in tick.items() if k not in ("expiry", "strike", "type")}
        if not isinstance(strike_raw, (int, float)):
            return

        self.update_contract(expiry=expiry_raw, strike=float(strike_raw), option_type=opt_type, **fields)

    # ----------------------------------------------------------------------
    # Internal
    # ----------------------------------------------------------------------

    def _df_to_chain(self, df: pd.DataFrame) -> OptionChain:
        symbol = str(df["symbol"].iloc[0]) if "symbol" in df.columns else self.symbol
        expiry = str(df["expiry"].iloc[0]) if "expiry" in df.columns else ""

        contracts: list[OptionContract] = []
        for _, row in df.iterrows():
            contracts.append(
                OptionContract(
                    symbol=symbol,
                    expiry=expiry,
                    strike=float(row["strike"]),
                    option_type=OptionType(row["type"]),
                    bid=row.get("bid"),
                    ask=row.get("ask"),
                    last=row.get("last"),
                    volume=row.get("volume"),
                    open_interest=row.get("oi") or row.get("open_interest"),
                    implied_vol=row.get("iv") or row.get("implied_vol"),
                    delta=row.get("delta"),
                    gamma=row.get("gamma"),
                    vega=row.get("vega"),
                    theta=row.get("theta"),
                )
            )

        chain = OptionChain(symbol=symbol, expiry=expiry, contracts=contracts)
        if "timestamp" in df.columns:
            try:
                chain.set_timestamp(int(df["timestamp"].iloc[0]))
            except Exception:
                pass
        return chain


def _coerce_snapshot(symbol: str, x: Any) -> OptionChainSnapshot | None:
    if x is None:
        return None

    if isinstance(x, OptionChainSnapshot):
        return x

    if isinstance(x, pd.DataFrame):
        if x.empty:
            return None
        chain_ts = int(x["timestamp"].iloc[0]) if "timestamp" in x.columns else int(time.time() * 1000)
        return OptionChainSnapshot.from_chain(chain_ts, x, symbol)

    if isinstance(x, dict):
        # accept {engine_ts, data_ts/chain_timestamp/timestamp, chain/contracts/payload}
        data_ts = x.get("data_ts", x.get("chain_timestamp", x.get("timestamp", x.get("ts"))))
        if data_ts is None:
            return None
        data_ts_i = int(data_ts)

        payload = x.get("chain")
        if payload is None:
            payload = x.get("contracts")
        if payload is None:
            payload = x.get("payload")
        if payload is None:
            return None

        engine_ts = x.get("engine_ts", x.get("ts_engine", data_ts_i))
        return OptionChainSnapshot.from_chain_aligned(
            timestamp=int(engine_ts),
            data_ts=data_ts_i,
            symbol=symbol,
            chain=payload,
        )

    # OptionChain object (ingestion object) -> freeze to snapshot
    if hasattr(x, "to_snapshot_dict"):
        try:
            payload = x.to_snapshot_dict()  # type: ignore[attr-defined]
            data_ts = getattr(x, "timestamp", None)
            data_ts_i = int(data_ts) if data_ts is not None else int(time.time() * 1000)
            return OptionChainSnapshot.from_chain_aligned(
                timestamp=data_ts_i,
                data_ts=data_ts_i,
                symbol=symbol,
                chain=payload,
            )
        except Exception:
            return None

    return None