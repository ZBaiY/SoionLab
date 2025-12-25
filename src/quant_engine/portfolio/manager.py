# portfolio/manager.py

from quant_engine.contracts.portfolio import PortfolioBase, PortfolioState, PositionRecord
from .registry import register_portfolio
from quant_engine.utils.logger import get_logger, log_debug, log_info


@register_portfolio("STANDARD")
class PortfolioManager(PortfolioBase):
    _logger = get_logger(__name__)
    def __init__(self, symbol: str, initial_capital: float = 10000.0):
        super().__init__(symbol=symbol)
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
            "timestamp": 1730000000000,  # epoch ms int (optional)
            "fill_price": 43000.5,
            "filled_qty": 0.01,
            "fee": 0.08
        }
        """
        log_debug(self._logger, "PortfolioManager received fill", fill=fill)
        
        price = fill["fill_price"]
        qty = fill["filled_qty"]
        fee = fill["fee"]

        self.fees += fee

        if self.symbol not in self.positions:
            self.positions[self.symbol] = PositionRecord(symbol=self.symbol, qty=0.0, entry_price=price)

        pos = self.positions[self.symbol]

        # --- Update position ---
        prev_qty = pos.qty
        new_qty = prev_qty + qty
        log_debug(self._logger, "PortfolioManager updated position quantities", prev_qty=prev_qty, new_qty=new_qty)

        # Realized PnL when reducing/inverting a position
        if prev_qty != 0 and (prev_qty * new_qty < 0 or abs(new_qty) < abs(prev_qty)):
            self.realized_pnl += (price - pos.entry_price) * (qty * -1 if prev_qty > 0 else qty)

        # Update entry price (VWAP)
        if new_qty != 0:
            pos.entry_price = (prev_qty * pos.entry_price + qty * price) / new_qty

        pos.qty = new_qty

        # --- Cash update ---
        self.cash -= price * qty + fee
        log_info(self._logger, "PortfolioManager applied fill", symbol=self.symbol, price=price, qty=qty, fee=fee, cash=self.cash)

    # -------------------------------------------------------
    # Portfolio state snapshot
    # -------------------------------------------------------
    def state(self) -> PortfolioState:
        """
        Returns a static dict snapshot that Risk / Strategy / Reporter use.

        Note: portfolio timestamps are attached at the engine snapshot layer (epoch ms int).
        """
        log_debug(self._logger, "PortfolioManager computing state snapshot")
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
        log_debug(self._logger, "PortfolioManager produced state", total_equity=total_value, realized_pnl=self.realized_pnl, unrealized_pnl=self.unrealized_pnl)

        return PortfolioState(snapshot)

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