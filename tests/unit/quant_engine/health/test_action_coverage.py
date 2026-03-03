import pytest

from quant_engine.exceptions.core import FatalError
from quant_engine.health.events import Action, ActionKind
from quant_engine.strategy.engine import StepFallback, StrategyEngine


def test_all_action_kinds_handled_by_step_dispatch() -> None:
    engine = StrategyEngine.__new__(StrategyEngine)
    for kind in ActionKind:
        action = Action(kind=kind)
        if kind == ActionKind.HALT:
            with pytest.raises(FatalError):
                engine._apply_step_action(action, "test")
            continue
        fallback = engine._apply_step_action(action, "test")
        assert fallback in {
            StepFallback.CONTINUE,
            StepFallback.SKIP,
            StepFallback.HOLD,
            StepFallback.FLATTEN,
        }


def test_all_action_kinds_documented_for_async_dispatch() -> None:
    handled = {
        ActionKind.HALT,
        ActionKind.RESTART_SOURCE,
        ActionKind.CONTINUE,
        ActionKind.SKIP_STEP,
        ActionKind.SKIP_DOMAIN,
        ActionKind.FORCE_HOLD,
        ActionKind.FORCE_FLATTEN,
        ActionKind.DISABLE_DOMAIN,
        ActionKind.LOG_ONLY,
    }
    assert handled == set(ActionKind)
