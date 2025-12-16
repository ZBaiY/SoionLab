from __future__ import annotations
from typing import Dict, Set, Any

from quant_engine.data.ohlcv.realtime import RealTimeDataHandler
from quant_engine.data.orderbook.realtime import RealTimeOrderbookHandler
from quant_engine.data.derivatives.option_chain.chain_handler import OptionChainDataHandler
from quant_engine.data.sentiment.loader import SentimentLoader
from quant_engine.data.derivatives.iv.iv_handler import IVSurfaceDataHandler

from quant_engine.utils.logger import get_logger, log_debug

_logger = get_logger(__name__)


def build_multi_symbol_handlers(
    *,
    data_spec: Dict[str, Any],
    backtest: bool = False,
    **kwargs: Any,
) -> Dict[str, Dict[str, object]]:
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

    symbols: Set[str] = set()

    # primary symbol is implicit in DATA['primary']
    # secondary symbols are explicit
    symbols.update(data_spec.get("secondary", {}).keys())

    ohlcv_handlers: Dict[str, RealTimeDataHandler] = {}

    if "ohlcv" in primary:
        cfg = primary["ohlcv"]
        symbol = kwargs.get("primary_symbol")
        if symbol is None:
            raise ValueError("primary_symbol must be provided for primary OHLCV")

        if backtest:
            from quant_engine.data.ohlcv.historical import HistoricalOHLCVHandler
            hist = HistoricalOHLCVHandler(symbol=symbol, **cfg)
            rt = RealTimeDataHandler.from_historical(
                hist,
                start_ts=kwargs.get("start_ts"),
            )
            ohlcv_handlers[symbol] = rt
        else:
            ohlcv_handlers[symbol] = RealTimeDataHandler(
                symbol=symbol,
                **cfg,
            )

        for sec_symbol, sec_cfg in secondary.items():
            if "ohlcv" not in sec_cfg:
                continue
            ohlcv_handlers[sec_symbol] = RealTimeDataHandler(
                symbol=sec_symbol,
                **sec_cfg["ohlcv"],
            )

    orderbook_handlers: Dict[str, RealTimeOrderbookHandler] = {}

    if "orderbook" in primary:
        cfg = primary["orderbook"]
        symbol = kwargs.get("primary_symbol")
        if symbol is None:
            raise ValueError("primary_symbol must be provided for primary orderbook")

        if backtest and hasattr(RealTimeOrderbookHandler, "from_historical"):
            # TODO: implement/standardize HistoricalOrderbookHandler and the adapter.
            try:
                from quant_engine.data.orderbook.historical import HistoricalOrderbookHandler  # type: ignore

                hist = HistoricalOrderbookHandler(symbol=symbol, **cfg)
                orderbook_handlers[symbol] = RealTimeOrderbookHandler.from_historical(
                    hist,
                    start_ts=kwargs.get("start_ts"),
                    end_ts=kwargs.get("end_ts"),
                )
            except Exception as e:
                log_debug(
                    _logger,
                    "Orderbook backtest adapter not available; falling back to realtime handler shell",
                    symbol=symbol,
                    error=str(e),
                )
                orderbook_handlers[symbol] = RealTimeOrderbookHandler(
                    symbol=symbol,
                    **cfg,
                )
        else:
            if backtest:
                log_debug(
                    _logger,
                    "Orderbook backtest requested but from_historical not implemented; using realtime handler shell",
                    symbol=symbol,
                )
            orderbook_handlers[symbol] = RealTimeOrderbookHandler(
                symbol=symbol,
                **cfg,
            )

        for sec_symbol, sec_cfg in secondary.items():
            if "orderbook" not in sec_cfg:
                continue
            if backtest and hasattr(RealTimeOrderbookHandler, "from_historical"):
                try:
                    from quant_engine.data.orderbook.historical import HistoricalOrderbookHandler  # type: ignore

                    hist = HistoricalOrderbookHandler(symbol=sec_symbol, **sec_cfg["orderbook"])
                    orderbook_handlers[sec_symbol] = RealTimeOrderbookHandler.from_historical(
                        hist,
                        start_ts=kwargs.get("start_ts"),
                        end_ts=kwargs.get("end_ts"),
                    )
                except Exception as e:
                    log_debug(
                        _logger,
                        "Orderbook backtest adapter not available for secondary; falling back to realtime handler shell",
                        symbol=sec_symbol,
                        error=str(e),
                    )
                    orderbook_handlers[sec_symbol] = RealTimeOrderbookHandler(
                        symbol=sec_symbol,
                        **sec_cfg["orderbook"],
                    )
            else:
                if backtest:
                    log_debug(
                        _logger,
                        "Orderbook backtest requested for secondary but from_historical not implemented; using realtime handler shell",
                        symbol=sec_symbol,
                    )
                orderbook_handlers[sec_symbol] = RealTimeOrderbookHandler(
                    symbol=sec_symbol,
                    **sec_cfg["orderbook"],
                )

    option_chain_handlers: Dict[str, OptionChainDataHandler] = {}

    if "option_chain" in primary:
        cfg = primary["option_chain"]
        symbol = kwargs.get("primary_symbol")
        if symbol is None:
            raise ValueError("primary_symbol must be provided for primary option_chain")

        if backtest and hasattr(OptionChainDataHandler, "from_historical"):
            # TODO: implement/standardize HistoricalOptionChainHandler and the adapter.
            try:
                from quant_engine.data.derivatives.option_chain.historical import HistoricalOptionChainHandler  # type: ignore

                hist = HistoricalOptionChainHandler(symbol=symbol, **cfg)
                option_chain_handlers[symbol] = OptionChainDataHandler.from_historical(
                    hist,
                    start_ts=kwargs.get("start_ts"),
                    end_ts=kwargs.get("end_ts"),
                )
            except Exception as e:
                log_debug(
                    _logger,
                    "OptionChain backtest adapter not available; falling back to realtime handler shell",
                    symbol=symbol,
                    error=str(e),
                )
                option_chain_handlers[symbol] = OptionChainDataHandler(
                    symbol=symbol,
                    **cfg,
                )
        else:
            if backtest:
                log_debug(
                    _logger,
                    "OptionChain backtest requested but from_historical not implemented; using realtime handler shell",
                    symbol=symbol,
                )
            option_chain_handlers[symbol] = OptionChainDataHandler(
                symbol=symbol,
                **cfg,
            )

        for sec_symbol, sec_cfg in secondary.items():
            if "option_chain" not in sec_cfg:
                continue
            if backtest and hasattr(OptionChainDataHandler, "from_historical"):
                try:
                    from quant_engine.data.derivatives.option_chain.historical import HistoricalOptionChainHandler  # type: ignore

                    hist = HistoricalOptionChainHandler(symbol=sec_symbol, **sec_cfg["option_chain"])
                    option_chain_handlers[sec_symbol] = OptionChainDataHandler.from_historical(
                        hist,
                        start_ts=kwargs.get("start_ts"),
                        end_ts=kwargs.get("end_ts"),
                    )
                except Exception as e:
                    log_debug(
                        _logger,
                        "OptionChain backtest adapter not available; falling back to realtime handler shell",
                        symbol=sec_symbol,
                        error=str(e),
                    )
                    option_chain_handlers[sec_symbol] = OptionChainDataHandler(
                        symbol=sec_symbol,
                        **sec_cfg["option_chain"],
                    )
            else:
                if backtest:
                    log_debug(
                        _logger,
                        "OptionChain backtest requested but from_historical not implemented; using realtime handler shell",
                        symbol=sec_symbol,
                    )
                option_chain_handlers[sec_symbol] = OptionChainDataHandler(
                    symbol=sec_symbol,
                    **sec_cfg["option_chain"],
                )

    iv_surface_handlers: Dict[str, IVSurfaceDataHandler] = {}

    if "iv_surface" in primary:
        symbol = kwargs.get("primary_symbol")
        if symbol not in option_chain_handlers:
            raise ValueError(f"IV surface for {symbol} requires option_chain handler")

        if backtest and hasattr(IVSurfaceDataHandler, "from_historical"):
            # TODO: implement/standardize HistoricalIVSurfaceHandler and the adapter.
            try:
                from quant_engine.data.derivatives.iv.historical import HistoricalIVSurfaceHandler  # type: ignore

                hist = HistoricalIVSurfaceHandler(
                    symbol=symbol,
                    chain_handler=option_chain_handlers[symbol],
                )
                iv_surface_handlers[symbol] = IVSurfaceDataHandler.from_historical(
                    hist,
                    start_ts=kwargs.get("start_ts"),
                    end_ts=kwargs.get("end_ts"),
                )
            except Exception as e:
                log_debug(
                    _logger,
                    "IVSurface backtest adapter not available; falling back to realtime handler shell",
                    symbol=symbol,
                    error=str(e),
                )
                iv_surface_handlers[symbol] = IVSurfaceDataHandler(
                    symbol=symbol,
                    chain_handler=option_chain_handlers[symbol],
                )
        else:
            if backtest:
                log_debug(
                    _logger,
                    "IVSurface backtest requested but from_historical not implemented; using realtime handler shell",
                    symbol=symbol,
                )
            iv_surface_handlers[symbol] = IVSurfaceDataHandler(
                symbol=symbol,
                chain_handler=option_chain_handlers[symbol],
            )

    for sec_symbol, sec_cfg in secondary.items():
        if "iv_surface" not in sec_cfg:
            continue
        if sec_symbol not in option_chain_handlers:
            raise ValueError(
                f"IV surface for {sec_symbol} requires option_chain handler"
            )
        if backtest and hasattr(IVSurfaceDataHandler, "from_historical"):
            try:
                from quant_engine.data.derivatives.iv.historical import HistoricalIVSurfaceHandler  # type: ignore

                hist = HistoricalIVSurfaceHandler(
                    symbol=sec_symbol,
                    chain_handler=option_chain_handlers[sec_symbol],
                )
                iv_surface_handlers[sec_symbol] = IVSurfaceDataHandler.from_historical(
                    hist,
                    start_ts=kwargs.get("start_ts"),
                    end_ts=kwargs.get("end_ts"),
                )
            except Exception as e:
                log_debug(
                    _logger,
                    "IVSurface backtest adapter not available; falling back to realtime handler shell",
                    symbol=sec_symbol,
                    error=str(e),
                )
                iv_surface_handlers[sec_symbol] = IVSurfaceDataHandler(
                    symbol=sec_symbol,
                    chain_handler=option_chain_handlers[sec_symbol],
                )
        else:
            if backtest:
                log_debug(
                    _logger,
                    "IVSurface backtest requested but from_historical not implemented; using realtime handler shell",
                    symbol=sec_symbol,
                )
            iv_surface_handlers[sec_symbol] = IVSurfaceDataHandler(
                symbol=sec_symbol,
                chain_handler=option_chain_handlers[sec_symbol],
            )

    sentiment_handlers: Dict[str, SentimentLoader] = {}

    if "sentiment" in primary:
        cfg = primary["sentiment"]
        symbol = kwargs.get("primary_symbol")
        if backtest and hasattr(SentimentLoader, "from_historical"):
            try:
                from quant_engine.data.sentiment.historical import HistoricalSentimentHandler  # type: ignore

                hist = HistoricalSentimentHandler(symbol=symbol, **cfg)
                sentiment_handlers[symbol] = SentimentLoader.from_historical(
                    hist,
                    start_ts=kwargs.get("start_ts"),
                    end_ts=kwargs.get("end_ts"),
                )
            except Exception as e:
                log_debug(
                    _logger,
                    "Sentiment backtest adapter not available; falling back to realtime loader shell",
                    symbol=symbol,
                    error=str(e),
                )
                sentiment_handlers[symbol] = SentimentLoader(
                    symbol=symbol,
                    **cfg,
                )
        else:
            if backtest:
                log_debug(
                    _logger,
                    "Sentiment backtest requested but from_historical not implemented; using realtime loader shell",
                    symbol=symbol,
                )
            sentiment_handlers[symbol] = SentimentLoader(
                symbol=symbol,
                **cfg,
            )

        for sec_symbol, sec_cfg in secondary.items():
            if "sentiment" not in sec_cfg:
                continue
            if backtest and hasattr(SentimentLoader, "from_historical"):
                try:
                    from quant_engine.data.sentiment.historical import HistoricalSentimentHandler  # type: ignore

                    hist = HistoricalSentimentHandler(symbol=sec_symbol, **sec_cfg["sentiment"])
                    sentiment_handlers[sec_symbol] = SentimentLoader.from_historical(
                        hist,
                        start_ts=kwargs.get("start_ts"),
                        end_ts=kwargs.get("end_ts"),
                    )
                except Exception as e:
                    log_debug(
                        _logger,
                        "Sentiment backtest adapter not available for secondary; falling back to realtime loader shell",
                        symbol=sec_symbol,
                        error=str(e),
                    )
                    sentiment_handlers[sec_symbol] = SentimentLoader(
                        symbol=sec_symbol,
                        **sec_cfg["sentiment"],
                    ) 
            else:
                if backtest:
                    log_debug(
                        _logger,
                        "Sentiment backtest requested for secondary but from_historical not implemented; using realtime loader shell",
                        symbol=sec_symbol,
                    )
                sentiment_handlers[sec_symbol] = SentimentLoader(
                    symbol=sec_symbol,
                    **sec_cfg["sentiment"],
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

    log_debug(
        _logger,
        "Built handler groups",
        domains=list(handler_dict.keys()),
    )

    return handler_dict
