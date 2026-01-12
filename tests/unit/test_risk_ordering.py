from quant_engine.risk.engine import RiskEngine
from quant_engine.risk.rule_full import FullAllocation
from quant_engine.risk.rules_constraints import CashPositionConstraintRule


def test_cash_constraint_runs_last():
    cash_rule = CashPositionConstraintRule(symbol="BTCUSDT")
    full_rule = FullAllocation(symbol="BTCUSDT")
    engine = RiskEngine(
        [cash_rule, full_rule],
        symbol="BTCUSDT",
        risk_config={"debug_full_position": True},
    )

    ordered_names = [type(r).__name__ for r in engine.rules]
    assert ordered_names == ["CashPositionConstraintRule", "FullAllocation"]
