# portfolio/manager.py
"""Standard integer-quantity portfolio accounting."""

from quant_engine.contracts.portfolio import PortfolioBase, PortfolioState, PositionRecord
from .registry import register_portfolio
from quant_engine.utils.logger import get_logger, log_debug, log_info, log_warn

# Float tolerance for rounding errors
EPS = 1e-9

# Default minimum trade thresholds (can be overridden via params)
DEFAULT_MIN_QTY = 1
DEFAULT_MIN_NOTIONAL = 10.0


@register_portfolio("STANDARD")
class PortfolioManager(PortfolioBase):
    """
    Standard integer-quantity portfolio manager.

    Invariants:
    - cash >= 0 (no borrowing)
    - position_qty >= 0 (no shorting)
    - quantities must be integers

    Post-slippage handling:
    - BUY: re-floor qty if total_cost > cash, drop if qty <= 0 or below min thresholds
    - SELL: clip to position, drop if qty == 0

    If a fill would violate invariants after re-clip, it is dropped and logged.
    """
    _logger = get_logger(__name__)

    def __init__(self, symbol: str, initial_capital: float = 10000.0, **kwargs):
        super().__init__(symbol=symbol, **kwargs)
        self.cash = float(initial_capital)
        self.fees = 0.0
        self.realized_pnl = 0.0
        self.unrealized_pnl = 0.0
        # Minimum trade filter thresholds
        self.min_qty = float(kwargs.get("min_qty", DEFAULT_MIN_QTY))
        self.min_notional = float(kwargs.get("min_notional", DEFAULT_MIN_NOTIONAL))

    def apply_fill(self, fill: dict) -> dict | None:
        """Apply a fill with post-slippage guards and minimum trade filters."""
        log_debug(self._logger, "PortfolioManager received fill", fill=fill)

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

        projected_target = qty
        outcome_decision = "ACCEPTED"
        outcome_realizable = qty

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
                symbol=fill_symbol, lots=0, entry_price=0.0
            )

        pos = self.positions[fill_symbol]
        prev_lots = pos.lots
        prev_avg = pos.entry_price

        if qty > 0:  # BUY
            fill_lots = self.lots_from_qty(qty, side="BUY")
            if fill_lots <= 0:
                log_info(
                    self._logger,
                    "portfolio.fill.drop_zero_lots",
                    symbol=fill_symbol,
                    side="BUY",
                    original_qty=qty,
                    step_size=str(self.step_size),
                    execution_decision="REJECTED",
                    reject_reason="integer_rounding",
                    projected_target=qty,
                    realizable_target=0.0,
                    reason="BUY qty below step size, dropping fill",
                )
                return {
                    "execution_decision": "REJECTED",
                    "reject_reason": "integer_rounding",
                    "projected_target": qty,
                    "realizable_target": 0.0,
                    "symbol": fill_symbol,
                    "side": "BUY",
                    "fill_ts": fill_ts,
                }

            required_cash = price * self.qty_float(fill_lots) + fee

            # Post-slippage over-budget handling: re-floor qty
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
                        execution_decision="REJECTED",
                        reject_reason="insufficient_cash",
                        projected_target=qty,
                        realizable_target=0.0,
                        reason="Insufficient cash after slippage, dropping fill"
                    )
                    return {
                        "execution_decision": "REJECTED",
                        "reject_reason": "insufficient_cash",
                        "projected_target": qty,
                        "realizable_target": 0.0,
                        "symbol": fill_symbol,
                        "side": "BUY",
                        "fill_ts": fill_ts,
                    }

                max_affordable_qty = available_for_qty / price
                reclipped_lots = self.lots_from_qty(max_affordable_qty, side="BUY")

                if reclipped_lots <= 0:
                    log_info(
                        self._logger,
                        "portfolio.fill.drop_insufficient_cash",
                        symbol=fill_symbol,
                        side="BUY",
                        original_qty=qty,
                        max_affordable=max_affordable_qty,
                        reclipped_lots=0,
                        execution_decision="REJECTED",
                        reject_reason="insufficient_cash",
                        projected_target=qty,
                        realizable_target=0.0,
                        reason="Re-floored qty to 0 after slippage, dropping fill"
                    )
                    return {
                        "execution_decision": "REJECTED",
                        "reject_reason": "insufficient_cash",
                        "projected_target": qty,
                        "realizable_target": 0.0,
                        "symbol": fill_symbol,
                        "side": "BUY",
                        "fill_ts": fill_ts,
                    }

                log_info(
                    self._logger,
                    "portfolio.fill.reclip_slippage_budget",
                    symbol=fill_symbol,
                    side="BUY",
                    original_qty=qty,
                    reclipped_lots=reclipped_lots,
                    price=price,
                    fee=fee,
                    cash_available=self.cash,
                    execution_decision="CLAMPED",
                    projected_target=qty,
                    realizable_target=self.qty_float(reclipped_lots),
                    reason="Re-floored qty due to post-slippage over-budget"
                )
                outcome_decision = "CLAMPED"
                outcome_realizable = self.qty_float(reclipped_lots)
                fill_lots = reclipped_lots
                required_cash = price * self.qty_float(fill_lots) + fee

            # Apply minimum trade filter
            qty_decimal = self.qty_from_lots(fill_lots)
            qty_float = float(qty_decimal)
            notional = qty_float * price
            if qty_float < self.min_qty or notional < self.min_notional:
                reject_reason = "min_lot" if qty_float < self.min_qty else "min_notional"
                log_info(
                    self._logger,
                    "portfolio.fill.drop_min_trade",
                    symbol=fill_symbol,
                    side="BUY",
                    lots=fill_lots,
                    qty_str=self.fmt_qty(fill_lots),
                    step_size=str(self.step_size),
                    notional=notional,
                    min_qty=self.min_qty,
                    min_notional=self.min_notional,
                    execution_decision="REJECTED",
                    reject_reason=reject_reason,
                    projected_target=qty,
                    realizable_target=0.0,
                    reason="BUY fill below minimum thresholds after re-clip"
                )
                return {
                    "execution_decision": "REJECTED",
                    "reject_reason": reject_reason,
                    "projected_target": qty,
                    "realizable_target": 0.0,
                    "symbol": fill_symbol,
                    "side": "BUY",
                    "fill_ts": fill_ts,
                }

            # Apply BUY
            new_lots = prev_lots + fill_lots

            # Update average entry price (weighted average)
            if new_lots > 0:
                new_avg = (prev_avg * prev_lots + price * fill_lots) / new_lots
            else:
                new_avg = price

            # Update state
            pos.lots = new_lots
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
                lots=fill_lots,
                qty_str=self.fmt_qty(fill_lots),
                step_size=str(self.step_size),
                price=price,
                accounting_price=price,
                fee=fee,
                new_position_lots=pos.lots,
                new_position_qty=self.qty_float(pos.lots),
                new_avg_entry=pos.entry_price,
                cash=self.cash,
                fill_ts=fill_ts,
                execution_decision=outcome_decision,
                projected_target=projected_target,
                realizable_target=outcome_realizable,
            )

        elif qty < 0:  # SELL
            requested_lots = self.lots_from_qty(abs(qty), side="SELL")
            if requested_lots <= 0:
                log_info(
                    self._logger,
                    "portfolio.fill.drop_zero_lots",
                    symbol=fill_symbol,
                    side="SELL",
                    original_qty=qty,
                    step_size=str(self.step_size),
                    execution_decision="REJECTED",
                    reject_reason="integer_rounding",
                    projected_target=qty,
                    realizable_target=0.0,
                    reason="SELL qty below step size, dropping fill",
                )
                return {
                    "execution_decision": "REJECTED",
                    "reject_reason": "integer_rounding",
                    "projected_target": qty,
                    "realizable_target": 0.0,
                    "symbol": fill_symbol,
                    "side": "SELL",
                    "fill_ts": fill_ts,
                }

            # Clip to available position
            if requested_lots > prev_lots:
                clipped_lots = prev_lots
                log_info(
                    self._logger,
                    "portfolio.fill.reclip_slippage_budget",
                    symbol=fill_symbol,
                    side="SELL",
                    original_lots=requested_lots,
                    clipped_lots=clipped_lots,
                    position_available_lots=prev_lots,
                    execution_decision="CLAMPED",
                    projected_target=qty,
                    realizable_target=-self.qty_float(clipped_lots),
                    reason="Clipped SELL qty to available position"
                )
                sell_lots = clipped_lots
                outcome_decision = "CLAMPED"
                outcome_realizable = -self.qty_float(clipped_lots)
            else:
                sell_lots = requested_lots

            if sell_lots <= 0:
                log_info(
                    self._logger,
                    "portfolio.fill.drop_zero_lots",
                    symbol=fill_symbol,
                    side="SELL",
                    original_qty=qty,
                    execution_decision="REJECTED",
                    reject_reason="position_limit",
                    projected_target=qty,
                    realizable_target=0.0,
                    reason="Clipped SELL lots to 0, dropping fill",
                )
                return {
                    "execution_decision": "REJECTED",
                    "reject_reason": "position_limit",
                    "projected_target": qty,
                    "realizable_target": 0.0,
                    "symbol": fill_symbol,
                    "side": "SELL",
                    "fill_ts": fill_ts,
                }

            # Apply minimum trade filter
            sell_qty_float = self.qty_float(sell_lots)
            notional = sell_qty_float * price
            if sell_qty_float < self.min_qty or notional < self.min_notional:
                reject_reason = "min_lot" if sell_qty_float < self.min_qty else "min_notional"
                log_info(
                    self._logger,
                    "portfolio.fill.drop_min_trade",
                    symbol=fill_symbol,
                    side="SELL",
                    lots=sell_lots,
                    qty_str=self.fmt_qty(sell_lots),
                    step_size=str(self.step_size),
                    notional=notional,
                    min_qty=self.min_qty,
                    min_notional=self.min_notional,
                    execution_decision="REJECTED",
                    reject_reason=reject_reason,
                    projected_target=qty,
                    realizable_target=0.0,
                    reason="SELL fill below minimum thresholds after clip"
                )
                return {
                    "execution_decision": "REJECTED",
                    "reject_reason": reject_reason,
                    "projected_target": qty,
                    "realizable_target": 0.0,
                    "symbol": fill_symbol,
                    "side": "SELL",
                    "fill_ts": fill_ts,
                }

            # Compute realized PnL before updating position
            realized_from_sell = (price - prev_avg) * sell_qty_float
            self.realized_pnl += realized_from_sell

            # Update position
            new_lots = prev_lots - sell_lots

            # Entry price remains unchanged on SELL (unless going flat)
            if new_lots <= 0:
                new_lots = 0
                new_avg = 0.0  # Reset when flat
            else:
                new_avg = prev_avg  # Preserve avg entry on partial sell

            # Cash increases from sell proceeds minus fee
            cash_received = price * sell_qty_float - fee

            pos.lots = new_lots
            pos.entry_price = new_avg
            self.cash += cash_received
            self.fees += fee

            # Clamp small negative values
            if -EPS < self.cash < 0:
                self.cash = 0.0

            log_info(
                self._logger,
                "portfolio.fill.applied_sell",
                symbol=fill_symbol,
                lots=sell_lots,
                qty_str=self.fmt_qty(sell_lots),
                step_size=str(self.step_size),
                price=price,
                accounting_price=price,
                fee=fee,
                realized_pnl=realized_from_sell,
                new_position_lots=pos.lots,
                new_position_qty=self.qty_float(pos.lots),
                avg_entry=pos.entry_price,
                cash=self.cash,
                fill_ts=fill_ts,
                execution_decision=outcome_decision,
                projected_target=projected_target,
                realizable_target=outcome_realizable,
            )

        else:  # qty == 0
            log_debug(self._logger, "portfolio.fill.skipped_zero_qty", symbol=fill_symbol)
            return {
                "execution_decision": "REJECTED",
                "reject_reason": "zero_qty",
                "projected_target": 0.0,
                "realizable_target": 0.0,
                "symbol": fill_symbol,
                "side": side_s if isinstance(side_s, str) else None,
                "fill_ts": fill_ts,
            }

        self._canonicalize_position(pos)

        self._assert_invariants(fill)
        return {
            "execution_decision": outcome_decision,
            "projected_target": projected_target,
            "realizable_target": outcome_realizable,
            "symbol": fill_symbol,
            "side": side_s,
            "fill_ts": fill_ts,
        }

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
            if pos.lots < 0:
                log_warn(
                    self._logger,
                    "portfolio.invariant.violation_detected",
                    invariant="position_qty >= 0",
                    symbol=sym,
                    lots=pos.lots,
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
                positions={k: v.lots for k, v in self.positions.items()},
                fill=fill
            )

    def state(self) -> PortfolioState:
        """Return a snapshot dict for risk/strategy/reporting."""
        log_debug(self._logger, "PortfolioManager computing state snapshot")
        total_value = self.cash + self._compute_position_value()
        exposure = self._compute_exposure()

        # Compute current position for primary symbol (for execution policy compatibility)
        primary_lots = 0
        if self.symbol in self.positions:
            primary_lots = self.positions[self.symbol].lots
        primary_position = self.qty_float(primary_lots)

        snapshot = {
            "cash": self.cash,
            "position": primary_position,  # for execution policy compatibility
            "position_qty": primary_position,
            "position_qty_str": self.fmt_qty(primary_lots),
            "position_lots": primary_lots,
            "step_size": float(self.step_size),
            "qty_step": str(self.step_size),
            "qty_mode": "LOTS",
            "min_qty": self.min_qty,
            "min_notional": self.min_notional,
            "positions": {
                k: {
                    "lots": v.lots,
                    "qty": self.qty_float(v.lots),
                    "qty_str": self.fmt_qty(v.lots),
                    "entry": 0.0 if v.lots == 0 else v.entry_price,
                    "unrealized": v.unrealized_pnl,
                }
                for k, v in self.positions.items()
            },
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "total_equity": total_value,
            "exposure": exposure,
            "position_frac": (exposure / total_value) if total_value > 0 else 0.0,
            "leverage": self._compute_leverage(total_value),
        }
        log_debug(
            self._logger,
            "PortfolioManager produced state",
            total_equity=total_value,
            realized_pnl=self.realized_pnl,
            unrealized_pnl=self.unrealized_pnl,
            position=primary_position
        )

        return PortfolioState(snapshot)

    def get_position(self, symbol: str | None = None) -> float:
        """Get current position quantity for a symbol."""
        sym = symbol or self.symbol
        if sym in self.positions:
            return self.qty_float(self.positions[sym].lots)
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
            if getattr(self, "_strict_leverage", False):
                raise ValueError(f"Spot-only leverage violation: {leverage}")
        return leverage
