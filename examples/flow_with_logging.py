"""Compatibility wrapper.

Re-exports `examples.basic_flow.flow_with_logging`.
"""

# ruff: noqa: F403
from examples.basic_flow.flow_with_logging import *
from examples.basic_flow.flow_with_logging import main

if __name__ == "__main__":
    main()
