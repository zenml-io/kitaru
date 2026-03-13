"""Integration test for the replay example workflow."""

from __future__ import annotations

from examples.replay.replay_with_overrides import run_workflow


def test_phase16_replay_example_runs_end_to_end() -> None:
    """Verify replay-from-checkpoint with checkpoint override semantics."""
    source_exec_id, replay_exec_id, original_result, replay_output = run_workflow(
        "kitaru"
    )

    assert source_exec_id
    assert replay_exec_id
    assert replay_exec_id != source_exec_id
    assert original_result == "PUBLISHED: Draft from notes about kitaru"
    assert replay_output == "PUBLISHED: Draft from edited notes for kitaru"
