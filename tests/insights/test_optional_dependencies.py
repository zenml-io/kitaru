#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Tests for the optional insight-provider dependency boundary."""

import subprocess
import sys


def test_base_insight_import_does_not_load_provider_dependencies() -> None:
    script = """
import sys
import kitaru.insights
import kitaru.insights.generation
from kitaru.insights import (
    GenerationObserver,
    InsightModelGenerator,
    ModelGenerationConfig,
)
assert GenerationObserver is not None
assert InsightModelGenerator is not None
assert ModelGenerationConfig is not None
assert "openai" not in sys.modules
assert "langfuse" not in sys.modules
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
