# portfolio/manager.py

from quant_engine.contracts.portfolio import PortfolioManagerProto, PortfolioState, PositionRecord
from .registry import register_portfolio


@register_portfolio("STANDARD")
class PortfolioManager(PortfolioManagerProto):
    def __init__(self, initial_capital: float = 10000.0):
        self.cash = initial_capital
        self.positions: dict[str, PositionRecord] = {}  # key: symbol
        self.fees = 0.0
        self.realized_pnl = 0.0
        self.unrealized_pnl = 0.0

    # -------------------------------------------------------
    # Core accounting: apply fills
    # -------------------------------------------------------
    def apply_fill(self, fill: dict):
        """
        fill example:
        {
            "symbol": "BTCUSDT",
            "fill_price": 43000.5,
            "filled_qty": 0.01,
            "fee": 0.08
        }
        """
        symbol = fill.get("symbol", "DEFAULT")
        price = fill["fill_price"]
        qty = fill["filled_qty"]
        fee = fill["fee"]

        self.fees += fee

        if symbol not in self.positions:
            self.positions[symbol] = PositionRecord(qty=0.0, entry_price=price)

        pos = self.positions[symbol]

        # --- Update position ---
        prev_qty = pos.qty
        new_qty = prev_qty + qty

        # Realized PnL when reducing/inverting a position
        if prev_qty != 0 and (prev_qty * new_qty < 0 or abs(new_qty) < abs(prev_qty)):
            self.realized_pnl += (price - pos.entry_price) * (qty * -1 if prev_qty > 0 else qty)

        # Update entry price (VWAP)
        if new_qty != 0:
            pos.entry_price = (prev_qty * pos.entry_price + qty * price) / new_qty

        pos.qty = new_qty

        # --- Cash update ---
        self.cash -= price * qty + fee

    # -------------------------------------------------------
    # Portfolio state snapshot
    # -------------------------------------------------------
    def state(self) -> PortfolioState:
        """
        Returns a static dict snapshot that Risk / Strategy / Reporter use.
        """
        total_value = self.cash + self._compute_unrealized_total()

        snapshot = {
            "cash": self.cash,
            "positions": {
                k: {"qty": v.qty, "entry": v.entry_price, "unrealized": v.unrealized_pnl}
                for k, v in self.positions.items()
            },
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "total_equity": total_value,
            "exposure": self._compute_exposure(),
            "leverage": self._compute_leverage(total_value),
        }

        return snapshot

    # -------------------------------------------------------
    # Internal helpers (simplified)
    # -------------------------------------------------------
    def _compute_unrealized_total(self):
        # placeholder: assume last price available globally
        self.unrealized_pnl = sum(p.unrealized_pnl for p in self.positions.values())
        return self.unrealized_pnl

    def _compute_exposure(self):
        return sum(abs(p.qty * p.entry_price) for p in self.positions.values())

    def _compute_leverage(self, total_equity):
        exposure = self._compute_exposure()
        return exposure / total_equity if total_equity > 0 else 0.0