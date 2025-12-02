import json
import importlib
import logging
from quant_engine.utils.logger import get_logger
from quant_engine.portfolio.state import PortfolioState
import numpy as np
import random

def set_seed(seed):
    np.random.seed(seed)
    random.seed(seed)

class StrategyEngine:
    """
    Build strategies from config and orchestrate the full pipeline.
    """

    def __init__(self, config_path: str, debug=False):
        self.debug = debug
        self.log = get_logger(__name__)
        if self.debug:
            self.log.setLevel(logging.DEBUG)
        self.config_path = config_path
        
        self.config = self._load_config()

        # strategy components
        self.features = []
        self.model = None
        self.decision = None
        self.risk = None
        self.policy = None
        self.router = None
        self.slippage = None
        self.matching = None
        self.portfolio = PortfolioState()

        self._assemble()

    # --------------------------------------------------------
    def _load_config(self):
        with open(self.config_path, "r") as f:
            return json.load(f)

    # --------------------------------------------------------
    def _resolve(self, class_name: str):
        """
        Dynamically import a class by searching known namespaces.
        """
        namespaces = [
            "quant_engine.features",
            "quant_engine.models",
            "quant_engine.decision",
            "quant_engine.risk",
            "quant_engine.execution",
        ]

        for ns in namespaces:
            try:
                module = importlib.import_module(ns + "." + class_name.lower().replace("model", ""))
            except:
                continue

            if hasattr(module, class_name):
                return getattr(module, class_name)

        raise ImportError(f"Class {class_name} not found in known namespaces.")
    def debug_trace(self, stage, data):
        if self.debug:
            self.log.debug(f"[TRACE] {stage}: {data}")
    # --------------------------------------------------------
    def _make(self, cfg: dict):
        cls = self._resolve(cfg["class"])
        params = cfg.get("params", {})
        return cls(**params)

    # --------------------------------------------------------
    def _assemble(self):
        # Features
        for fcfg in self.config.get("features", []):
            self.features.append(self._make(fcfg))

        # Model
        self.model = self._make(self.config["model"])

        # Decision
        self.decision = self._make(self.config["decision"])

        # Risk
        self.risk = self._make(self.config["risk"])

        # Execution stack
        exec_cfg = self.config["execution"]
        self.policy = self._make(exec_cfg["policy"])
        self.router = self._make(exec_cfg["router"])
        self.slippage = self._make(exec_cfg["slippage"])
        self.matching = self._make(exec_cfg["matching"])

    # --------------------------------------------------------
    def on_bar(self, bar, window):
        # merge all features
        #with timer("features", self.log):
        feature_values = {}
        for f in self.features:
            feature_values.update(f.compute(window))

        score = self.model.predict(feature_values)
        intent = self.decision.decide(score)
        size = self.risk.size(intent)

        orders = self.policy.generate_orders(size, self.portfolio.position)
        routed = self.router.route(orders)

        for o in routed:
            price = self.slippage.apply(bar["close"], o.qty)
            fill = self.matching.fill(price, o.qty)
            self.portfolio.position += o.qty if o.side == "BUY" else -o.qty

    # --------------------------------------------------------
    def backtest(self, data_handler):
        # set_seed(self.seed)
        for bar, window in data_handler.stream():
            self.on_bar(bar, window)
        


    # --------------------------------------------------------
    def report(self):
        print("Final Position:", self.portfolio.position)