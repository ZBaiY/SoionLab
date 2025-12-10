from __future__ import annotations
from typing import Dict, Set

from quant_engine.data.ohlcv.realtime import RealTimeDataHandler
from quant_engine.data.orderbook.realtime import RealTimeOrderbookHandler
from quant_engine.data.derivatives.option_chain.chain_handler import OptionChainDataHandler
from quant_engine.data.sentiment.loader import SentimentLoader
from quant_engine.data.derivatives.iv.iv_handler import IVSurfaceDataHandler

from quant_engine.utils.logger import get_logger, log_debug

_logger = get_logger(__name__)


def build_multi_symbol_handlers(symbols: Set[str]) -> Dict[str, Dict[str, object]]:
    """
    Construct multi-symbol handler groups for:
        - ohlcv
        - orderbook
        - option_chain
        - sentiment

    Each returns: dict[str → dict[symbol → handler]].

    This is the Version 3 data ingestion builder.
    """

    log_debug(_logger, "Building multi-symbol handlers", symbols=list(symbols))

    ohlcv_handlers: Dict[str, RealTimeDataHandler] = {
        s: RealTimeDataHandler(symbol=s) for s in symbols
    }

    orderbook_handlers: Dict[str, RealTimeOrderbookHandler] = {
        s: RealTimeOrderbookHandler(symbol=s) for s in symbols
    }

    option_chain_handlers: Dict[str, OptionChainDataHandler] = {
        s: OptionChainDataHandler(symbol=s) for s in symbols
    }

    iv_surface_handlers: Dict[str, IVSurfaceDataHandler] = {
        s: IVSurfaceDataHandler(symbol=s, chain_handler=option_chain_handlers[s])
        for s in symbols
    }

    sentiment_handlers: Dict[str, SentimentLoader] = {
        s: SentimentLoader(symbol=s) for s in symbols
    }

    handler_dict = {
        "ohlcv": ohlcv_handlers,
        "orderbook": orderbook_handlers,
        "option_chain": option_chain_handlers,
        "iv_surface": iv_surface_handlers,
        "sentiment": sentiment_handlers,
    }

    log_debug(_logger, "Built handler groups",
              ohlcv=len(ohlcv_handlers),
              orderbook=len(orderbook_handlers),
              option_chain=len(option_chain_handlers),
              iv_surface=len(iv_surface_handlers),
              sentiment=len(sentiment_handlers))

    return handler_dict
