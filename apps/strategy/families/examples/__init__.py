"""Public example and smoke-test strategy family."""

# Import family modules so strategy/feature/decision registration does not
# depend on package discovery order alone.
from . import minimal_flip as _minimal_flip  # noqa: F401
from . import strategies as _strategies  # noqa: F401
