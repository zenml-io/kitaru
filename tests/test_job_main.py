#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Tests for the job process entry."""

import subprocess
import sys
from pathlib import Path


def run_module(*args: str, tmp_path: Path) -> subprocess.CompletedProcess[str]:
    """Run the job process entry as a subprocess with a clean environment."""
    return subprocess.run(
        [sys.executable, "-m", "kitaru.job", *args],
        capture_output=True,
        text=True,
        cwd=tmp_path,
        env={},
        check=False,
    )


def test_main_rejects_a_missing_kind(tmp_path: Path) -> None:
    """Exit non-zero and print usage without a kind argument."""
    result = run_module(tmp_path=tmp_path)
    assert result.returncode == 1
    assert "Usage: python -m kitaru.job" in result.stderr


def test_main_rejects_an_unknown_kind(tmp_path: Path) -> None:
    """Exit non-zero and print usage for a kind that is not score or import."""
    result = run_module("replay", tmp_path=tmp_path)
    assert result.returncode == 1
    assert "Usage: python -m kitaru.job" in result.stderr


def test_main_reports_a_missing_environment(tmp_path: Path) -> None:
    """Exit non-zero naming the missing variable when the environment is empty."""
    result = run_module("score", tmp_path=tmp_path)
    assert result.returncode == 1
    assert "KITARU_JOB_ID is not set" in result.stderr
