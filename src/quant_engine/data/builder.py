from __future__ import annotations
from typing import Dict, Any

from quant_engine.data.ohlcv.realtime import OHLCVDataHandler
from quant_engine.data.orderbook.realtime import RealTimeOrderbookHandler
from quant_engine.data.derivatives.option_chain.chain_handler import OptionChainDataHandler
from quant_engine.data.sentiment.sentiment_handler import SentimentDataHandler
from quant_engine.data.trades.realtime import TradesDataHandler
from quant_engine.data.derivatives.option_trades.realtime import OptionTradesDataHandler
from quant_engine.data.derivatives.iv.iv_handler import IVSurfaceDataHandler
from quant_engine.data.contracts.protocol_realtime import DataHandlerProto

from quant_engine.utils.logger import get_logger, log_debug

_logger = get_logger(__name__)


def build_multi_symbol_handlers(
    *,
    data_spec: Dict[str, Any],
    **kwargs: Any,
) -> Dict[str, Dict[str, DataHandlerProto]]:
    """
    Construct multi-symbol data handlers from a Strategy DATA specification.

    DATA is the single source of truth for:
    - data domains (ohlcv / orderbook / option_chain / iv / sentiment)
    - primary vs secondary symbols
    - handler-level configuration (passed opaquely)
    """

    log_debug(_logger, "Building multi-symbol handlers", data_spec=data_spec)

    primary = data_spec.get("primary", {})
    secondary = data_spec.get("secondary", {})
    mode = kwargs.get("mode")

    ohlcv_handlers: Dict[str, OHLCVDataHandler] = {}

    if "ohlcv" in primary:
        cfg = primary["ohlcv"]
        symbol = kwargs.get("primary_symbol")
        if symbol is None:
            raise ValueError("primary_symbol must be provided for primary OHLCV")
        ohlcv_handlers[symbol] = OHLCVDataHandler(
            symbol=symbol,
            mode=mode,
            **cfg,
        )

    for sec_symbol, sec_cfg in secondary.items():
        if "ohlcv" not in sec_cfg:
            continue
        ohlcv_handlers[sec_symbol] = OHLCVDataHandler(
            symbol=sec_symbol,
            mode=mode,
            **sec_cfg["ohlcv"],
        )

    orderbook_handlers: Dict[str, RealTimeOrderbookHandler] = {}

    if "orderbook" in primary:
        cfg = primary["orderbook"]
        symbol = kwargs.get("primary_symbol")
        if symbol is None:
            raise ValueError("primary_symbol must be provided for primary orderbook")
        orderbook_handlers[symbol] = RealTimeOrderbookHandler(
            symbol=symbol,
            mode=mode,
            **cfg,
        )

    for sec_symbol, sec_cfg in secondary.items():
        if "orderbook" not in sec_cfg:
            continue
        orderbook_handlers[sec_symbol] = RealTimeOrderbookHandler(
            symbol=sec_symbol,
            mode=mode,
            **sec_cfg["orderbook"],
        )

    option_chain_handlers: Dict[str, OptionChainDataHandler] = {}

    if "option_chain" in primary:
        cfg = primary["option_chain"]
        symbol = kwargs.get("primary_symbol")
        if symbol is None:
            raise ValueError("primary_symbol must be provided for primary option_chain")
        option_chain_handlers[symbol] = OptionChainDataHandler(
            symbol=symbol,
            mode=mode,
            **cfg,
        )

    for sec_symbol, sec_cfg in secondary.items():
        if "option_chain" not in sec_cfg:
            continue
        option_chain_handlers[sec_symbol] = OptionChainDataHandler(
            symbol=sec_symbol,
            mode=mode,
            **sec_cfg["option_chain"],
        )

    iv_surface_handlers: Dict[str, IVSurfaceDataHandler] = {}

    if "iv_surface" in primary:
        cfg = primary["iv_surface"]
        symbol = kwargs.get("primary_symbol")
        if symbol is None:
            raise ValueError("primary_symbol must be provided for primary iv_surface")
        if symbol not in option_chain_handlers:
            raise ValueError(f"IV surface for {symbol} requires option_chain handler")
        iv_surface_handlers[symbol] = IVSurfaceDataHandler(
            symbol=symbol,
            chain_handler=option_chain_handlers[symbol],
            mode=mode,
            **cfg,
        )

    for sec_symbol, sec_cfg in secondary.items():
        if "iv_surface" not in sec_cfg:
            continue
        cfg2 = sec_cfg["iv_surface"]
        if sec_symbol not in option_chain_handlers:
            raise ValueError(
                f"IV surface for {sec_symbol} requires option_chain handler"
            )
        iv_surface_handlers[sec_symbol] = IVSurfaceDataHandler(
            symbol=sec_symbol,
            chain_handler=option_chain_handlers[sec_symbol],
            mode=mode,
            **cfg2,
        )

    sentiment_handlers: Dict[str, SentimentDataHandler] = {}

    if "sentiment" in primary:
        cfg = primary["sentiment"]
        symbol = kwargs.get("primary_symbol")
        if symbol is None:
            raise ValueError("primary_symbol must be provided for primary sentiment")

        # v4: SentimentHandler is runtime IO-free; adapters for historical live elsewhere.
        sentiment_handlers[symbol] = SentimentDataHandler(
            symbol=symbol,
            mode=mode,
            **cfg,
        )

    for sec_symbol, sec_cfg in secondary.items():
        if "sentiment" not in sec_cfg:
            continue
        sentiment_handlers[sec_symbol] = SentimentDataHandler(
            symbol=sec_symbol,
            mode=mode,
            **sec_cfg["sentiment"],
        )

    trades_handlers: Dict[str, TradesDataHandler] = {}

    if "trades" in primary:
        cfg = primary["trades"]
        symbol = kwargs.get("primary_symbol")
        if symbol is None:
            raise ValueError("primary_symbol must be provided for primary trades")
        trades_handlers[symbol] = TradesDataHandler(
            symbol=symbol,
            mode=mode,
            **cfg,
        )

    for sec_symbol, sec_cfg in secondary.items():
        if "trades" not in sec_cfg:
            continue
        trades_handlers[sec_symbol] = TradesDataHandler(
            symbol=sec_symbol,
            mode=mode,
            **sec_cfg["trades"],
        )

    option_trades_handlers: Dict[str, OptionTradesDataHandler] = {}

    if "option_trades" in primary:
        cfg = primary["option_trades"]
        symbol = kwargs.get("primary_symbol")
        if symbol is None:
            raise ValueError("primary_symbol must be provided for primary option_trades")
        option_trades_handlers[symbol] = OptionTradesDataHandler(
            symbol=symbol,
            mode=mode,
            **cfg,
        )

    for sec_symbol, sec_cfg in secondary.items():
        if "option_trades" not in sec_cfg:
            continue
        option_trades_handlers[sec_symbol] = OptionTradesDataHandler(
            symbol=sec_symbol,
            mode=mode,
            **sec_cfg["option_trades"],
        )

    handler_dict = {}
    if ohlcv_handlers:
        handler_dict["ohlcv"] = ohlcv_handlers
    if orderbook_handlers:
        handler_dict["orderbook"] = orderbook_handlers
    if option_chain_handlers:
        handler_dict["option_chain"] = option_chain_handlers
    if iv_surface_handlers:
        handler_dict["iv_surface"] = iv_surface_handlers
    if sentiment_handlers:
        handler_dict["sentiment"] = sentiment_handlers
    if trades_handlers:
        handler_dict["trades"] = trades_handlers
    if option_trades_handlers:
        handler_dict["option_trades"] = option_trades_handlers

    log_debug(
        _logger,
        "Built handler groups",
        domains=list(handler_dict.keys()),
    )

    return handler_dict
