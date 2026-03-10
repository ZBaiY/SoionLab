from __future__ import annotations

import time

from quant_engine.contracts.exchange_account import (
    AccountState,
    AssetBalance,
    StartupReadiness,
    SymbolConstraints,
)

from .binance_client import BinanceClientError, BinanceSpotClient


class BinanceAccountAdapter:
    def __init__(self, client: BinanceSpotClient) -> None:
        self.client = client
        self._symbol_assets: dict[str, tuple[str, str]] = {}

    def get_symbol_constraints(self, symbol: str) -> SymbolConstraints:
        key = str(symbol).strip().upper()
        info = self.client.get_exchange_info(key)
        filters = self.client.get_symbol_filters(key)
        lot = filters.get("LOT_SIZE", {})
        notional_filter = filters.get("MIN_NOTIONAL", filters.get("NOTIONAL", {}))
        base_asset = str(info.get("baseAsset") or "").strip().upper() or None
        quote_asset = str(info.get("quoteAsset") or "").strip().upper() or None
        if base_asset is not None and quote_asset is not None:
            self._symbol_assets[key] = (base_asset, quote_asset)
        return SymbolConstraints(
            step_size=float(lot.get("stepSize", 0.0) or 0.0),
            min_qty=float(lot.get("minQty", 0.0) or 0.0),
            min_notional=float(notional_filter.get("minNotional", 0.0) or 0.0),
            base_asset=base_asset,
            quote_asset=quote_asset,
        )

    def get_account_state(self) -> AccountState:
        payload = self.client.api("GET", "/api/v3/account", signed=True, params={})
        balances_raw = payload.get("balances") if isinstance(payload, dict) else None
        if not isinstance(balances_raw, list):
            raise BinanceClientError("Binance account payload missing balances[]")

        balances: dict[str, AssetBalance] = {}
        totals: dict[str, float] = {}
        for item in balances_raw:
            if not isinstance(item, dict):
                continue
            asset = str(item.get("asset") or "").strip().upper()
            if not asset:
                continue
            try:
                free = float(item.get("free", 0.0) or 0.0)
                locked = float(item.get("locked", 0.0) or 0.0)
            except Exception as exc:
                raise BinanceClientError(f"invalid Binance balance payload for asset={asset}") from exc
            balances[asset] = AssetBalance(free=free, locked=locked)
            totals[asset] = free + locked

        positions: dict[str, float] = {}
        for symbol, assets in self._symbol_assets.items():
            base_asset, _quote_asset = assets
            positions[symbol] = float(totals.get(base_asset, 0.0) or 0.0)

        return AccountState(
            balances=balances,
            positions=positions,
            timestamp=int(time.time() * 1000),
        )

    def get_startup_readiness(self, symbol: str) -> StartupReadiness:
        key = str(symbol).strip().upper()
        assets = self._symbol_assets.get(key)
        if assets is None:
            raise BinanceClientError(
                f"get_startup_readiness: symbol {key} not resolved; "
                "call get_symbol_constraints first"
            )
        base_asset, quote_asset = assets

        account_state = self.get_account_state()

        open_orders_raw = self.client.api(
            "GET", "/api/v3/openOrders", signed=True, params={"symbol": key},
        )
        open_order_count = len(open_orders_raw) if isinstance(open_orders_raw, list) else 0

        quote_bal = account_state.balances.get(quote_asset)
        base_position_qty = float(account_state.positions.get(key, 0.0) or 0.0)

        return StartupReadiness(
            timestamp=account_state.timestamp,
            symbol=key,
            open_order_count=open_order_count,
            quote_asset=quote_asset,
            quote_free=float(quote_bal.free) if quote_bal else 0.0,
            quote_locked=float(quote_bal.locked) if quote_bal else 0.0,
            base_asset=base_asset,
            base_position_qty=base_position_qty,
        )
