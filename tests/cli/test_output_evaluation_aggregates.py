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
"""Human text rendering of experiment-run evaluation aggregates."""

import pytest
from rich.text import Text
from tests.cli.output_rendering import render_text

from kitaru.cli.output import (
    CommandResult,
)


@pytest.mark.parametrize("columns", [80, 160])
def test_human_experiment_run_shows_evaluation_aggregate_summary(columns: int) -> None:
    """Aggregate output shows statistics without expanding replay values."""
    stdout, stderr = render_text(
        "experiment.run.get",
        CommandResult(
            item={
                "status": "completed",
                "evaluation_aggregates": [
                    {
                        "name": "correctness",
                        "data_type": "bool",
                        "evaluator_name": "quality-check",
                        "evaluator_version": 2,
                        "evaluator_version_id": "019f0000-1111-7222-8333-444444444444",
                        "baseline": {"count": 4, "mean": 0.25, "pass_rate": 0.75},
                        "result": {"count": 4, "mean": 0.75, "pass_rate": 0.25},
                        "replays": [{"replay_id": "hidden-replay-details"}],
                    },
                    {
                        "name": "resolution",
                        "data_type": "categorical",
                        "evaluator_name": "category-check",
                        "evaluator_version": 3,
                        "evaluator_version_id": "019f0000-1111-7222-8333-555555555555",
                        "baseline": {
                            "count": 4,
                            "mean": None,
                            "pass_rate": None,
                            "value_counts": {"blocked": 1, "resolved": 3},
                        },
                        "result": {
                            "count": 0,
                            "mean": None,
                            "pass_rate": None,
                            "value_counts": {},
                        },
                        "replays": [],
                    },
                ],
            }
        ),
        rich=True,
        columns=columns,
    )

    rendered = " ".join(stdout.split())
    assert "Evaluation aggregates" in rendered
    assert "correctness (bool)" in rendered
    assert "quality-check@2" in rendered
    assert "019f0000-1111-7222-8333-444444444444" in rendered
    assert "Baseline: count=4; mean score=0.25; pass rate=75%" in rendered
    assert "Result: count=4; mean score=0.75; pass rate=25%" in rendered
    assert "resolution (categorical)" in rendered
    assert "category-check@3" in rendered
    assert "Baseline: count=4; values: blocked=1, resolved=3" in rendered
    assert "Result: count=0" in rendered
    assert "hidden-replay-details" not in rendered
    assert "null" not in rendered
    assert stderr == ""


@pytest.mark.parametrize("columns", [80, 160])
def test_human_experiment_run_distinguishes_empty_and_unrequested_aggregates(
    columns: int,
) -> None:
    """Only an explicitly requested empty aggregate list shows an empty state."""
    default, _ = render_text(
        "experiment.run.get",
        CommandResult(item={"status": "completed"}),
        rich=True,
        columns=columns,
    )
    empty, _ = render_text(
        "experiment.run.get",
        CommandResult(item={"status": "completed", "evaluation_aggregates": []}),
        rich=True,
        columns=columns,
    )

    assert "Evaluation aggregates" not in default
    assert "Evaluation aggregates" in empty
    assert "No evaluation aggregates found." in empty
    assert empty.startswith(default)


def test_human_evaluation_aggregate_summary_escapes_terminal_controls() -> None:
    """Formatted names retain literal markup and cannot control the terminal."""
    stdout, _ = render_text(
        "experiment.run.get",
        CommandResult(
            item={
                "evaluation_aggregates": [
                    {
                        "name": "quality[red]\x1b[999D",
                        "data_type": "bool",
                        "evaluator_name": "quality-check",
                        "evaluator_version": 1,
                        "baseline": {"count": 0},
                        "result": {"count": 0},
                    }
                ]
            }
        ),
        rich=True,
        strip_ansi=False,
    )

    assert "\x1b[999D" not in stdout
    assert "quality[red]\\u001b[999D" in Text.from_ansi(stdout).plain
