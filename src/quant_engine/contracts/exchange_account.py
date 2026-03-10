from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class AssetBalance:
    free: float
    locked: float


@dataclass(frozen=True)
class AccountState:
    balances: dict[str, AssetBalance]
    positions: dict[str, float]
    timestamp: int


@dataclass(frozen=True)
class SymbolConstraints:
    step_size: float
    min_qty: float
    min_notional: float
    base_asset: str | None = None
    quote_asset: str | None = None


class ExchangeAccountAdapter(Protocol):
    def get_account_state(self) -> AccountState:
        ...

    def get_symbol_constraints(self, symbol: str) -> SymbolConstraints:
        ...
