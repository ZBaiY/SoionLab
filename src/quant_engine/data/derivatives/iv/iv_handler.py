from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from quant_engine.utils.logger import get_logger, log_debug
from quant_engine.data.derivatives.option_chain.chain_handler import OptionChainDataHandler
from quant_engine.data.derivatives.option_chain.snapshot import OptionChainSnapshot
from quant_engine.data.derivatives.iv.snapshot import IVSurfaceSnapshot


_logger = get_logger(__name__)


@dataclass
class IVSurfaceDataHandler:
    """v4 IV Surface Data Handler

    This handler derives IVSurfaceSnapshot objects from an underlying
    OptionChainDataHandler. It does **not** touch raw exchange APIs and
    does not own the option chain itself; it is purely a derived layer.

    Design goals:
        • Use OptionChainDataHandler.get_snapshot(ts) as the only input
        • Produce IVSurfaceSnapshot objects for:
            - get_snapshot(ts)
            - window(ts, n)
        • Enforce anti-lookahead via the chain handler's alignment

    Notes
    -----
    - For now, this handler derives atm_iv, skew, and smile curve directly
      from OptionChainSnapshot (which already computes basic statistics).
      The `surface` field in IVSurfaceSnapshot is left as a lightweight
      params dict and can later be populated by real SABR/SSVI calibration.
    - This keeps the architecture clean: real calibration can be added
      without changing the handler contract.
    """

    symbol: str
    chain_handler: OptionChainDataHandler
    expiry: Optional[str] = None
    model_name: str = "CHAIN_DERIVED"  # e.g. "SSVI", "SABR", or placeholder

    _snapshots: List[IVSurfaceSnapshot] = field(default_factory=list, init=False)

    # ------------------------------------------------------------------
    # Core update entrypoint (called by engine on each tick)
    # ------------------------------------------------------------------
    def on_tick(self, ts: float) -> Optional[IVSurfaceSnapshot]:
        """Update IV surface state at engine timestamp ts.

        Pulls an OptionChainSnapshot from the underlying chain handler
        and, if available, converts it into an IVSurfaceSnapshot.
        """
        log_debug(_logger, "IVSurfaceDataHandler.on_tick called", symbol=self.symbol, ts=ts)

        chain_snap: OptionChainSnapshot | None = self.chain_handler.get_snapshot(ts)
        if chain_snap is None:
            log_debug(_logger, "No OptionChainSnapshot available for IV surface", symbol=self.symbol, ts=ts)
            return None

        iv_snap = self._build_from_chain_snapshot(ts, chain_snap)
        self._snapshots.append(iv_snap)

        log_debug(
            _logger,
            "IVSurfaceSnapshot updated",
            symbol=self.symbol,
            ts=ts,
            surface_ts=iv_snap.timestamp,
            atm_iv=iv_snap.atm_iv,
            skew=iv_snap.skew,
        )
        return iv_snap

    # ------------------------------------------------------------------
    # Internal construction from chain snapshot
    # ------------------------------------------------------------------
    def _build_from_chain_snapshot(self, ts: float, chain_snap: OptionChainSnapshot) -> IVSurfaceSnapshot:
        """Construct an IVSurfaceSnapshot from a given OptionChainSnapshot.

        For now, we:
            • reuse chain_snap.atm_iv and chain_snap.skew
            • interpret chain_snap.smile as the curve (strike → iv)
            • store a lightweight params dict in `surface`
        """
        surface_ts = float(getattr(chain_snap, "timestamp", ts))

        atm_iv = float(getattr(chain_snap, "atm_iv", 0.0))
        skew = float(getattr(chain_snap, "skew", 0.0))
        curve = dict(getattr(chain_snap, "smile", {}))

        params = {"source": "chain_snapshot", "model": self.model_name}

        return IVSurfaceSnapshot.from_surface(
            ts=ts,
            surface_ts=surface_ts,
            atm_iv=atm_iv,
            skew=skew,
            curve=curve,
            surface_params=params,
            symbol=self.symbol,
            expiry=self.expiry,
            model=self.model_name,
        )

    # ------------------------------------------------------------------
    # v4 timestamp-aligned API
    # ------------------------------------------------------------------
    def get_snapshot(self, ts: float) -> Optional[IVSurfaceSnapshot]:
        """Return the latest IVSurfaceSnapshot with timestamp ≤ ts.

        This does **not** perform calibration on-demand; it only returns
        already-computed snapshots. The engine is responsible for calling
        on_tick(ts) in lockstep with other handlers.
        """
        if not self._snapshots:
            return None

        candidates = [s for s in self._snapshots if float(s.timestamp) <= ts]
        if not candidates:
            return None

        return max(candidates, key=lambda s: float(s.timestamp))

    def window(self, ts: float, n: int) -> List[IVSurfaceSnapshot]:
        """Return the most recent n IV surface snapshots with timestamp ≤ ts."""
        if not self._snapshots:
            return []

        valid = [s for s in self._snapshots if float(s.timestamp) <= ts]
        if not valid:
            return []

        valid_sorted = sorted(valid, key=lambda s: float(s.timestamp))
        return valid_sorted[-n:]

    def last_timestamp(self) -> Optional[float]:
        """Return the timestamp of the most recent IV surface snapshot, if any."""
        if not self._snapshots:
            return None
        return float(self._snapshots[-1].timestamp)
