# portfolio/fractional.py
"""
Fractional Portfolio Manager - Supports fractional share trading.

Enforced Invariants (checked after every fill application):
- cash >= 0 always (no implicit borrowing/leverage)
- position_qty[symbol] >= 0 always (no shorting)
- Fractional quantities allowed (unlike STANDARD which requires integers)

Entry Price / Cost Basis Rules (long-only average cost):
- On BUY: new_avg = (old_avg * old_qty + fill_price * buy_qty) / new_qty
- On SELL: avg_entry_price remains unchanged (realized PnL computed using old_avg)
- When position goes flat (qty=0): avg_entry_price resets to 0

Post-Slippage Handling (final guard):
- For BUY: if total_cost > cash after slippage, clip qty to affordable amount
- Apply minimum trade filter after clip; drop if below thresholds
- For SELL: clip to available position

Clip Policy (not floor+drop):
- BUY: clip by cash affordability (fractional allowed)
- SELL: clip to available position (fractional allowed)
- Drop if below min_qty or min_notional thresholds

Use case:
- Crypto trading with fractional BTC/ETH
- Stock fractional share trading (Robinhood, etc.)
"""

from quant_engine.contracts.portfolio import PortfolioBase, PortfolioState, PositionRecord
from .registry import register_portfolio
from quant_engine.utils.logger import get_logger, log_debug, log_info, log_warn

# Float tolerance for rounding errors
EPS = 1e-9

# Default minimum trade thresholds for fractional (lower than STANDARD)
DEFAULT_MIN_QTY = 0.0001
DEFAULT_MIN_NOTIONAL = 1.0


@register_portfolio("FRACTIONAL")
class FractionalPortfolioManager(PortfolioBase):
    """
    Fractional-quantity portfolio manager.

    Invariants:
    - cash >= 0 (no borrowing)
    - position_qty >= 0 (no shorting)
    - fractional quantities allowed (float)

    Post-slippage handling (clip policy):
    - BUY: clip qty if total_cost > cash, drop if below min thresholds
    - SELL: clip to position, drop if below min thresholds

    If a fill would violate invariants after clip, it is dropped and logged.
    """
    _logger = get_logger(__name__)

    def __init__(self, symbol: str, initial_capital: float = 10000.0, **kwargs):
        super().__init__(symbol=symbol)
        self.cash = float(initial_capital)
        self.positions: dict[str, PositionRecord] = {}  # key: symbol
        self.fees = 0.0
        self.realized_pnl = 0.0
        self.unrealized_pnl = 0.0

        # Minimum trade filter thresholds (lower defaults for fractional)
        self.min_qty = float(kwargs.get("min_qty", DEFAULT_MIN_QTY))
        self.min_notional = float(kwargs.get("min_notional", DEFAULT_MIN_NOTIONAL))

    # -------------------------------------------------------
    # Core accounting: apply fills with post-slippage clip
    # -------------------------------------------------------
    def apply_fill(self, fill: dict):
        """
        Apply a fill to the portfolio with invariant guards and post-slippage clip.

        Fill format:
        {
            "symbol": "BTCUSDT",
            "timestamp": 1730000000000,  # epoch ms int (optional)
            "fill_price": 43000.5,       # ACTUAL price after slippage
            "filled_qty": 0.01,          # positive for BUY, negative for SELL
            "fee": 0.08,
            "side": "BUY" | "SELL"       # optional, inferred from qty sign
        }

        Post-slippage handling (clip policy):
        - BUY: if total_cost > cash, clip qty to what's affordable
        - SELL: clip to available position
        - Apply minimum trade filter after clip
        - Drop fill if resulting qty < min thresholds
        """
        log_debug(self._logger, "FractionalPortfolioManager received fill", fill=fill)

        price = float(fill["fill_price"])
        qty = float(fill["filled_qty"])
        fee = float(fill.get("fee", 0.0))
        fill_symbol = fill.get("symbol", self.symbol)
        fill_ts = fill.get("timestamp")

        # Determine side from qty sign or explicit side field
        side = fill.get("side")
        if side is None:
            side = "BUY" if qty > 0 else "SELL"
        side_s = str(side).upper()
        if side_s == "SELL" and qty > 0:
            qty = -abs(qty)
        elif side_s == "BUY" and qty < 0:
            qty = abs(qty)

        log_debug(
            self._logger,
            "portfolio.price_trace",
            symbol=fill_symbol,
            accounting_price=price,
            fill_ts=fill_ts,
        )

        # Initialize position if needed
        if fill_symbol not in self.positions:
            self.positions[fill_symbol] = PositionRecord(
                symbol=fill_symbol, qty=0.0, entry_price=0.0
            )

        pos = self.positions[fill_symbol]
        prev_qty = pos.qty
        prev_avg = pos.entry_price

        # -------------------------------------------------------
        # BUY validation with post-slippage clip
        # -------------------------------------------------------
        if qty > 0:  # BUY
            required_cash = price * qty + fee

            # Post-slippage over-budget handling: clip qty
            if required_cash > self.cash + EPS:
                # Calculate max affordable qty at actual fill price
                available_for_qty = self.cash - fee
                if available_for_qty <= 0:
                    log_info(
                        self._logger,
                        "portfolio.fill.drop_insufficient_cash",
                        symbol=fill_symbol,
                        side="BUY",
                        qty=qty,
                        price=price,
                        fee=fee,
                        required_cash=required_cash,
                        cash_available=self.cash,
                        reason="Insufficient cash after slippage, dropping fill"
                    )
                    return

                clipped_qty = available_for_qty / price

                if clipped_qty < EPS:
                    log_info(
                        self._logger,
                        "portfolio.fill.drop_insufficient_cash",
                        symbol=fill_symbol,
                        side="BUY",
                        original_qty=qty,
                        clipped_qty=0,
                        reason="Clipped qty to 0 after slippage, dropping fill"
                    )
                    return

                log_info(
                    self._logger,
                    "portfolio.fill.reclip_slippage_budget",
                    symbol=fill_symbol,
                    side="BUY",
                    original_qty=qty,
                    clipped_qty=clipped_qty,
                    price=price,
                    fee=fee,
                    cash_available=self.cash,
                    reason="Clipped qty due to post-slippage over-budget"
                )
                qty = clipped_qty
                required_cash = price * qty + fee

            # Apply minimum trade filter
            notional = qty * price
            if qty < self.min_qty or notional < self.min_notional:
                log_info(
                    self._logger,
                    "portfolio.fill.drop_min_trade",
                    symbol=fill_symbol,
                    side="BUY",
                    qty=qty,
                    notional=notional,
                    min_qty=self.min_qty,
                    min_notional=self.min_notional,
                    reason="BUY fill below minimum thresholds after clip"
                )
                return

            # Apply BUY
            new_qty = prev_qty + qty

            # Update average entry price (weighted average)
            if new_qty > EPS:
                new_avg = (prev_avg * prev_qty + price * qty) / new_qty
            else:
                new_avg = price

            # Update state
            pos.qty = new_qty
            pos.entry_price = new_avg
            self.cash -= required_cash
            self.fees += fee

            # Clamp small negative cash to 0 (rounding tolerance)
            if -EPS < self.cash < 0:
                self.cash = 0.0

            log_info(
                self._logger,
                "portfolio.fill.applied_buy",
                symbol=fill_symbol,
                qty=qty,
                price=price,
                accounting_price=price,
                fee=fee,
                new_position=pos.qty,
                new_avg_entry=pos.entry_price,
                cash=self.cash,
                fill_ts=fill_ts,
            )

        # -------------------------------------------------------
        # SELL validation with clip to position
        # -------------------------------------------------------
        elif qty < 0:  # SELL
            sell_qty = abs(qty)

            # Clip to available position
            if sell_qty > prev_qty + EPS:
                clipped_qty = prev_qty
                log_info(
                    self._logger,
                    "portfolio.fill.reclip_slippage_budget",
                    symbol=fill_symbol,
                    side="SELL",
                    original_qty=sell_qty,
                    clipped_qty=clipped_qty,
                    position_available=prev_qty,
                    reason="Clipped SELL qty to available position"
                )
                sell_qty = clipped_qty

            if sell_qty < EPS:
                log_debug(self._logger, "portfolio.fill.skipped_zero_sell", symbol=fill_symbol)
                return

            # Apply minimum trade filter
            notional = sell_qty * price
            if sell_qty < self.min_qty or notional < self.min_notional:
                log_info(
                    self._logger,
                    "portfolio.fill.drop_min_trade",
                    symbol=fill_symbol,
                    side="SELL",
                    qty=sell_qty,
                    notional=notional,
                    min_qty=self.min_qty,
                    min_notional=self.min_notional,
                    reason="SELL fill below minimum thresholds after clip"
                )
                return

            # Compute realized PnL before updating position
            realized_from_sell = (price - prev_avg) * sell_qty
            self.realized_pnl += realized_from_sell

            # Update position
            new_qty = prev_qty - sell_qty

            # Entry price remains unchanged on SELL (unless going flat)
            if new_qty < EPS:
                new_qty = 0.0
                new_avg = 0.0  # Reset when flat
            else:
                new_avg = prev_avg  # Preserve avg entry on partial sell

            # Cash increases from sell proceeds minus fee
            cash_received = price * sell_qty - fee

            pos.qty = new_qty
            pos.entry_price = new_avg
            self.cash += cash_received
            self.fees += fee

            # Clamp small negative values
            if -EPS < pos.qty < EPS:
                pos.qty = 0.0
            if -EPS < self.cash < 0:
                self.cash = 0.0

            log_info(
                self._logger,
                "portfolio.fill.applied_sell",
                symbol=fill_symbol,
                qty=sell_qty,
                price=price,
                accounting_price=price,
                fee=fee,
                realized_pnl=realized_from_sell,
                new_position=pos.qty,
                avg_entry=pos.entry_price,
                cash=self.cash,
                fill_ts=fill_ts,
            )

        else:  # qty == 0
            log_debug(self._logger, "portfolio.fill.skipped_zero_qty", symbol=fill_symbol)
            return

        # -------------------------------------------------------
        # Final invariant check (should never fail after above guards)
        # -------------------------------------------------------
        self._assert_invariants(fill)

    def _assert_invariants(self, fill: dict):
        """Final guard: verify invariants hold. Log violation if detected."""
        violation = False

        if self.cash < -EPS:
            log_warn(
                self._logger,
                "portfolio.invariant.violation_detected",
                invariant="cash >= 0",
                cash=self.cash,
                fill=fill,
                reason="Cash went negative after fill (should not happen)"
            )
            violation = True

        for sym, pos in self.positions.items():
            if pos.qty < -EPS:
                log_warn(
                    self._logger,
                    "portfolio.invariant.violation_detected",
                    invariant="position_qty >= 0",
                    symbol=sym,
                    qty=pos.qty,
                    fill=fill,
                    reason="Position went negative after fill (should not happen)"
                )
                violation = True

        if violation:
            # This should never happen if guards are correct
            # Log but don't raise - the system can continue
            log_warn(
                self._logger,
                "portfolio.invariant.violation_summary",
                cash=self.cash,
                positions={k: v.qty for k, v in self.positions.items()},
                fill=fill
            )

    # -------------------------------------------------------
    # Portfolio state snapshot
    # -------------------------------------------------------
    def state(self) -> PortfolioState:
        """
        Returns a static dict snapshot that Risk / Strategy / Reporter use.

        Note: portfolio timestamps are attached at the engine snapshot layer (epoch ms int).
        """
        log_debug(self._logger, "FractionalPortfolioManager computing state snapshot")
        total_value = self.cash + self._compute_position_value()

        # Compute current position for primary symbol (for execution policy compatibility)
        primary_position = 0.0
        if self.symbol in self.positions:
            primary_position = self.positions[self.symbol].qty
        if abs(primary_position) < EPS:
            primary_position = 0.0

        snapshot = {
            "cash": self.cash,
            "position": primary_position,  # for execution policy compatibility
            "position_qty": primary_position,
            "positions": {
                k: {
                    "qty": 0.0 if abs(v.qty) < EPS else v.qty,
                    "entry": 0.0 if abs(v.qty) < EPS else v.entry_price,
                    "unrealized": v.unrealized_pnl,
                }
                for k, v in self.positions.items()
            },
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "total_equity": total_value,
            "exposure": self._compute_exposure(),
            "position_frac": (self._compute_exposure() / total_value) if total_value > 0 else 0.0,
            "leverage": self._compute_leverage(total_value),
        }
        log_debug(
            self._logger,
            "FractionalPortfolioManager produced state",
            total_equity=total_value,
            realized_pnl=self.realized_pnl,
            unrealized_pnl=self.unrealized_pnl,
            position=primary_position
        )

        return PortfolioState(snapshot)

    # -------------------------------------------------------
    # Query methods for risk/execution layers
    # -------------------------------------------------------
    def get_position(self, symbol: str | None = None) -> float:
        """Get current position quantity for a symbol."""
        sym = symbol or self.symbol
        if sym in self.positions:
            return self.positions[sym].qty
        return 0.0

    def get_cash(self) -> float:
        """Get current available cash."""
        return self.cash

    def get_avg_entry_price(self, symbol: str | None = None) -> float:
        """Get average entry price for a symbol."""
        sym = symbol or self.symbol
        if sym in self.positions:
            return self.positions[sym].entry_price
        return 0.0

    # -------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------
    def _compute_position_value(self):
        """Mark positions at entry price (spot-only, no external mark)."""
        position_value = sum(p.qty * p.entry_price for p in self.positions.values())
        self.unrealized_pnl = 0.0
        return position_value

    def _compute_exposure(self):
        return sum(abs(p.qty * p.entry_price) for p in self.positions.values())

    def _compute_leverage(self, total_equity):
        exposure = self._compute_exposure()
        leverage = exposure / total_equity if total_equity > 0 else 0.0
        if leverage > 1.0 + EPS:
            log_warn(
                self._logger,
                "portfolio.leverage_violation",
                leverage=leverage,
                exposure=exposure,
                equity=total_equity,
                reason="Spot-only constraint breached",
            )
        return leverage
