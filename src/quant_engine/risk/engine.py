# risk/engine.py
from collections.abc import Iterable
from typing import Callable

from quant_engine.utils.logger import get_logger, log_debug, log_error

from quant_engine.contracts.risk import RiskBase


class RiskEngine:

    def __init__(
        self,
        rules: list[RiskBase],
        symbol: str = "",
        risk_config: dict | None = None,
    ):
        self._logger = get_logger(__name__)
        self._risk_config = dict(risk_config) if isinstance(risk_config, dict) else {}
        self.shortable = bool(self._risk_config.get("shortable", False))
        self.mapping_name = self._resolve_mapping_name(self._risk_config.get("mapping"))
        self._mapping_fn = self._resolve_mapping_fn(self.mapping_name)
        self.rules = self._order_rules(list(rules))
        self.symbol = symbol
        self._validate_rule_spaces(self.rules)

    def adjust(self, size: float, context: dict) -> float:
        score = float(size)
        context.setdefault("decision_score", score)
        risk_state = context.setdefault("risk_state", {})
        risk_state["shortable"] = self.shortable

        self._assert_in_range(
            input_value=score,
            output_value=score,
            rule_name="decision_score",
            expected_range="[-1,1]",
            context=context,
        )

        target_position = float(self._mapping_fn(context, score))
        self._assert_in_range(
            input_value=score,
            output_value=target_position,
            rule_name=f"mapping:{self.mapping_name}",
            expected_range="[-1,1]",
            context=context,
        )

        log_debug(
            self._logger,
            "risk.score_trace",
            symbol=self.symbol,
            score=score,
            target_position=target_position,
            engine_ts=context.get("timestamp"),
            mapping=self.mapping_name,
        )

        for rule in self.rules:
            prev_value = target_position
            target_position = float(rule.adjust(target_position, context))
            self._assert_in_range(
                input_value=prev_value,
                output_value=target_position,
                rule_name=getattr(rule, "_risk_name", type(rule).__name__),
                expected_range="[-1,1]",
                context=context,
            )

        if self.shortable:
            target_position = max(-1.0, min(1.0, target_position))
        else:
            target_position = max(0.0, min(1.0, target_position))

        return target_position

    def _order_rules(self, rules: list[RiskBase]) -> list[RiskBase]:
        debug_full = bool(self._risk_config.get("debug_full_position", False))
        full_rules = [r for r in rules if type(r).__name__ == "FullAllocation"]
        if full_rules and not debug_full:
            log_debug(
                self._logger,
                "risk.rules.full_allocation_skipped",
                reason="debug_full_position disabled",
                full_allocation_count=len(full_rules),
            )
        other_rules = [r for r in rules if type(r).__name__ != "FullAllocation"]
        if debug_full and full_rules:
            log_debug(
                self._logger,
                "risk.rules.reordered",
                reason="FullAllocation forced last",
                full_allocation_count=len(full_rules),
            )
        return other_rules + (full_rules if debug_full else [])

    def _validate_rule_spaces(self, rules: list[RiskBase]) -> None:
        for idx, rule in enumerate(rules):
            space = getattr(rule, "RULE_SPACE", None)
            if space is None:
                continue
            if str(space).lower() != "target":
                raise ValueError(
                    "Score-space logic belongs in Decision, not Risk. "
                    f"Offending rule index={idx}, name={type(rule).__name__}, space={space}."
                )

    def _resolve_mapping_name(self, mapping_cfg: object | None) -> str:
        if isinstance(mapping_cfg, dict):
            name = mapping_cfg.get("name")
            if name:
                return str(name)
        return "score_to_target"

    def _resolve_mapping_fn(self, name: str) -> Callable[[dict, float], float]:
        mapping_table = {
            "identity": self._map_identity,
            "score_to_target": self._map_score_to_target,
        }
        if name not in mapping_table:
            raise ValueError(f"Unknown risk mapping: {name}")
        return mapping_table[name]

    def _map_identity(self, context: dict, score: float) -> float:
        return float(score)

    def _map_score_to_target(self, context: dict, score: float) -> float:
        risk_state = context.setdefault("risk_state", {})
        price_info = self._get_price_ref(context)
        if price_info is not None:
            price_ref, price_source, price_data_ts = price_info
            risk_state.setdefault("price_ref", price_ref)
            risk_state.setdefault("price_source", price_source)
            risk_state.setdefault("price_data_ts", price_data_ts)

        portfolio = context.get("portfolio", {})
        if isinstance(portfolio, dict) and "portfolio" in portfolio:
            portfolio = portfolio.get("portfolio", {})
        cash = float(portfolio.get("cash", 0.0))
        current_qty = float(portfolio.get("position_qty", portfolio.get("position", 0.0)))
        price_ref = float(risk_state.get("price_ref", 0.0))
        equity = cash + current_qty * price_ref if price_ref > 0 else cash
        current_position_frac = (current_qty * price_ref / equity) if equity > 0 else 0.0

        risk_state["current_qty"] = current_qty
        risk_state["equity"] = equity
        risk_state["current_position_frac"] = current_position_frac

        if score < 0.0:
            return -1.0
        if score > 0.0:
            return 1.0
        return current_position_frac

    def _get_price_ref(self, context: dict) -> tuple[float, str, int | None] | None:
        primary_snapshots = context.get("primary_snapshots", {})
        orderbook = primary_snapshots.get("orderbook")
        if orderbook is not None:
            bid = orderbook.get_attr("best_bid") if hasattr(orderbook, "get_attr") else None
            ask = orderbook.get_attr("best_ask") if hasattr(orderbook, "get_attr") else None
            if bid is not None and ask is not None:
                try:
                    price = (float(bid) + float(ask)) / 2.0
                    data_ts = getattr(orderbook, "data_ts", None)
                    return price, "orderbook.mid", data_ts
                except (TypeError, ValueError):
                    pass
            mid = orderbook.get_attr("mid") if hasattr(orderbook, "get_attr") else None
            if mid is not None:
                try:
                    price = float(mid)
                    data_ts = getattr(orderbook, "data_ts", None)
                    return price, "orderbook.mid", data_ts
                except (TypeError, ValueError):
                    pass

        ohlcv = primary_snapshots.get("ohlcv")
        if ohlcv is not None:
            close = ohlcv.get_attr("close") if hasattr(ohlcv, "get_attr") else None
            if close is not None:
                try:
                    price = float(close)
                    data_ts = getattr(ohlcv, "data_ts", None)
                    return price, "ohlcv.close", data_ts
                except (TypeError, ValueError):
                    pass

        return None

    def _assert_in_range(
        self,
        *,
        input_value: float,
        output_value: float,
        rule_name: str,
        expected_range: str,
        context: dict,
    ) -> None:
        if output_value < -1.0 - 1e-9 or output_value > 1.0 + 1e-9:
            log_error(
                self._logger,
                "risk_contract_violation",
                rule_name=rule_name,
                input_value=input_value,
                output_value=output_value,
                expected_range=expected_range,
                shortable=self.shortable,
                strategy_name=context.get("strategy_name"),
                ts=context.get("timestamp"),
            )
            raise ValueError(
                f"Risk contract violation: {rule_name} output {output_value} outside {expected_range}"
            )

    def set_required_features(self, feature_names: Iterable[str]) -> None:
        for rule in self.rules:
            if hasattr(rule, "set_required_features"):
                rule.set_required_features(feature_names)

    def validate_features(self, available_features: set[str]) -> None:
        for rule in self.rules:
            if hasattr(rule, "validate_features"):
                rule.validate_features(available_features)

    def validate_feature_types(self, available_feature_types: set[str]) -> None:
        for rule in self.rules:
            if hasattr(rule, "validate_feature_types"):
                rule.validate_feature_types(available_feature_types)
