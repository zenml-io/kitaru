"""Compatibility wrapper.

Re-exports `examples.execution_management.client_execution_management`.
"""

# ruff: noqa: F403
from examples.execution_management.client_execution_management import *
from examples.execution_management.client_execution_management import main

if __name__ == "__main__":
    main()
