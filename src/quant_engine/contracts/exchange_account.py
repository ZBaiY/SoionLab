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


@dataclass(frozen=True)
class StartupReadiness:
    timestamp: int
    symbol: str
    open_order_count: int
    quote_asset: str
    quote_free: float
    quote_locked: float
    base_asset: str
    base_position_qty: float


class ExchangeAccountAdapter(Protocol):
    def get_account_state(self) -> AccountState:
        ...

    def get_symbol_constraints(self, symbol: str) -> SymbolConstraints:
        ...

    def get_startup_readiness(self, symbol: str) -> StartupReadiness:
        ...
