# contracts/portfolio.py
from decimal import Decimal, ROUND_FLOOR
from typing import Any, Mapping, Protocol, Dict
from dataclasses import dataclass

from quant_engine.utils.logger import log_debug, log_warn

"""
┌──────────────────────────┐
│   MatchingEngine / Live  │
└──────────────┬───────────┘
               │  apply_fill()
               ▼
┌──────────────────────────┐
│   PortfolioManagerProto  │  <─── Contract
└──────────────┬───────────┘
    updates    │ returns
positions, PnL │ PortfolioState
               ▼
┌──────────────────────────┐
│      PortfolioState      │  <─── Read-only snapshot
└──────────────────────────┘
                ▲
    used by Strategy / Risk / Reporter
"""

@dataclass
class PositionRecord:
    symbol: str
    lots: int
    entry_price: float
    unrealized_pnl: float = 0.0


@dataclass
class PortfolioState:
    snapshot_dict: Dict

    def to_dict(self) -> Dict:
        return dict(self.snapshot_dict)


class PortfolioManagerProto(Protocol):
    """
    Core accounting interface.
    Receives fills from MatchingEngine.
    Updates positions, PnL, metrics.
    """
    symbol: str

    def apply_fill(self, fill: Dict) -> Dict | None:
        """Update portfolio based on fill dict."""
        ...

    def state(self) -> PortfolioState:
        """Return current state."""
        ...
    def market_status(self, market_data: Dict | None) -> str | None:
        ...
    def update_marks(self, market_snapshots: Dict | None) -> None:
        ...

class PortfolioBase(PortfolioManagerProto):
    """
    V4 unified portfolio base:
        • symbol-aware (primary trading symbol)
        • child classes store positions, cash, pnl
        • must implement apply_fill() and state()
    """
    SCHEMA_VERSION = 2
    EPS_QTY = 1e-9

    def __init__(self, symbol: str, **kwargs):
        self.symbol = symbol
        self._last_mark: dict[str, float] = {}
        self._strict_leverage = bool(kwargs.get("strict_leverage", False))
        self.step_size = self._d(kwargs.get("step_size", "1"))
        if self.step_size <= 0:
            raise ValueError(f"step_size must be positive, got {self.step_size!r}")
        # self.cash = float(initial_capital)
        self.positions: dict[str, PositionRecord] = {}  # key: symbol


    def apply_fill(self, fill: Dict) -> Dict | None:
        raise NotImplementedError("Portfolio must implement apply_fill()")

    def state(self) -> PortfolioState:
        raise NotImplementedError("Portfolio must implement state()")

    def market_status(self, market_data: Dict | None) -> str | None:
        if market_data is None:
            return None
        if isinstance(market_data, dict):
            snap = market_data.get("orderbook") or market_data.get("ohlcv")
        else:
            snap = market_data
        if snap is None:
            return None
        market = snap.get_attr("market")
        status = getattr(market, "status", None)
        if status is not None:
            return str(status)
        return None

    def market_is_active(self, market_data: Dict | None) -> bool:
        status = self.market_status(market_data)
        if status is None:
            return True
        return str(status).lower() == "open"

    def _d(self, x: Any) -> Decimal:
        if isinstance(x, Decimal):
            return x
        return Decimal(str(x))

    def qty_from_lots(self, lots: int) -> Decimal:
        return self.step_size * Decimal(int(lots))

    def lots_from_qty(self, qty: float | Decimal, *, side: str) -> int:
        qty_d = self._d(qty)
        if qty_d < 0:
            qty_d = -qty_d
        if qty_d <= 0:
            return 0
        lots = (qty_d / self.step_size).to_integral_value(rounding=ROUND_FLOOR)
        return int(lots)

    
    def fmt_qty(self, lots: int) -> str:
        self._assert_finite_positive()
        qty = self.qty_from_lots(lots)  # Decimal
        scale = self._step_scale()
        if scale == 0:
            return f"{qty:.0f}"
        q = qty.quantize(Decimal(1).scaleb(-scale), rounding=ROUND_FLOOR)
        return f"{q:.{scale}f}"
    
    def qty_float(self, lots: int) -> float:
        return float(self.qty_from_lots(lots))

    def update_marks(self, market_snapshots: dict | None) -> None:
        if not isinstance(market_snapshots, dict):
            return
        ohlcv = market_snapshots.get("ohlcv")
        if not isinstance(ohlcv, dict):
            return
        for sym, snap in ohlcv.items():
            price = self._extract_mark_price(snap)
            if price is None:
                continue
            self._last_mark[str(sym)] = price

    # -------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------
    def _extract_mark_price(self, snap: Any) -> float | None:
        if snap is None:
            return None
        if hasattr(snap, "close"):
            try:
                return float(getattr(snap, "close"))
            except (TypeError, ValueError):
                return None
        if isinstance(snap, Mapping):
            numeric = snap.get("numeric")
            if isinstance(numeric, Mapping) and "close" in numeric:
                try:
                    return float(numeric["close"])
                except (TypeError, ValueError):
                    return None
            if "close" in snap:
                try:
                    return float(snap["close"])
                except (TypeError, ValueError):
                    return None
        return None

    def _get_mark_price(self, symbol: str) -> float | None:
        return self._last_mark.get(symbol)

    def _canonicalize_position(self, pos: PositionRecord) -> None:
        if pos.lots == 0:
            pos.entry_price = 0.0
            pos.unrealized_pnl = 0.0

    def _compute_position_value(self):
        """Mark positions using cached mark prices when available."""
        position_value = 0.0
        unrealized_total = 0.0
        for sym, pos in self.positions.items():
            self._canonicalize_position(pos)
            if pos.lots == 0:
                continue
            qty_decimal = self.qty_from_lots(pos.lots)
            qty_float = float(qty_decimal)
            mark = self._get_mark_price(sym)
            if mark is None:
                mark = pos.entry_price
                logger = getattr(self, "_logger", None)
                if logger is not None and pos.lots > 0:
                    log_warn(
                        logger,
                        "portfolio.mark.missing",
                        symbol=sym,
                        entry_price=pos.entry_price,
                        reason="Missing mark price; falling back to entry price",
                    )
            logger = getattr(self, "_logger", None)
            if logger is not None:
                log_debug(
                    logger,
                    "portfolio.mark.snapshot",
                    symbol=sym,
                    mark_price=mark,
                    qty_str=self.fmt_qty(pos.lots),
                    lots=pos.lots,
                )
            unrealized = (mark - pos.entry_price) * qty_float
            pos.unrealized_pnl = unrealized
            position_value += qty_float * mark
            unrealized_total += unrealized
        self.unrealized_pnl = unrealized_total
        return position_value

    def _compute_exposure(self):
        exposure = 0.0
        for sym, pos in self.positions.items():
            self._canonicalize_position(pos)
            if pos.lots == 0:
                continue
            qty_decimal = self.qty_from_lots(pos.lots)
            qty_float = float(qty_decimal)
            mark = self._get_mark_price(sym)
            if mark is None:
                mark = pos.entry_price
            exposure += abs(qty_float * mark)
        return exposure
    def _assert_finite_positive(self) -> None:
        if not self.step_size.is_finite():
            raise ValueError(f"step_size must be finite, got {self.step_size!r}")
        if self.step_size <= 0:
            raise ValueError(f"step_size must be positive, got {self.step_size!r}")

    def _step_scale(self) -> int:
        # step_size is finite positive by invariant
        s = self.step_size.normalize()
        exp = s.as_tuple().exponent
        # exp is int for finite decimals; narrow for pylance:
        if not isinstance(exp, int):
            raise ValueError(f"step_size exponent must be int, got {exp!r}")
        return max(-exp, 0)
