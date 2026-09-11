import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from kitaru_atif_importer.harbor import collect_trajectories, main


def _write_trial(root: Path, name: str = "trial") -> Path:
    trial = root / name
    agent = trial / "agent"
    agent.mkdir(parents=True)
    (agent / "trajectory.json").write_text('{"schema_version":"ATIF-v1.7"}')
    (trial / "result.json").write_text(
        json.dumps(
            {
                "id": name,
                "task_name": "example",
                "config": {"env": {"API_KEY": "must-not-be-bundled"}},
                "verifier_result": {"rewards": {"reward": 0}},
            }
        )
    )
    return trial


def test_collects_sorted_trials_without_configuration(tmp_path: Path) -> None:
    _write_trial(tmp_path, "b")
    _write_trial(tmp_path, "a")
    records = collect_trajectories(tmp_path)["trajectories"]
    assert [r["source_id"] for r in records] == [
        "a/agent/trajectory.json",
        "b/agent/trajectory.json",
    ]
    assert records[0]["harbor_result"]["verifier_result"]["rewards"] == {"reward": 0}
    assert "must-not-be-bundled" not in json.dumps(records)


def test_collects_multistep_trial_with_step_outcome(tmp_path: Path) -> None:
    trial = _write_trial(tmp_path)
    (trial / "agent" / "trajectory.json").unlink()
    agent = trial / "steps" / "solve" / "agent"
    agent.mkdir(parents=True)
    (agent / "trajectory.json").write_text("{}")
    (trial / "result.json").write_text(
        json.dumps(
            {
                "id": "trial-id",
                "step_results": [
                    {
                        "step_name": "solve",
                        "exception_info": {"exception_message": "failed"},
                    }
                ],
            }
        )
    )
    record = collect_trajectories(tmp_path)["trajectories"][0]
    assert record["source_id"] == "trial-id/steps/solve/agent/trajectory.json"
    assert record["harbor_result"]["exception_info"]["exception_message"] == "failed"


def test_rejects_symlinked_trajectory(tmp_path: Path) -> None:
    trial = _write_trial(tmp_path)
    source = trial / "agent" / "trajectory.json"
    source.unlink()
    source.symlink_to(trial / "result.json")
    with pytest.raises(ValueError, match="symlink"):
        collect_trajectories(tmp_path)


def test_rejects_invalid_json_and_oversize_input(tmp_path: Path) -> None:
    trial = _write_trial(tmp_path)
    with pytest.raises(ValueError, match="limit"):
        collect_trajectories(tmp_path, max_bytes=1)
    (trial / "agent" / "trajectory.json").write_text("invalid")
    with pytest.raises(ValueError, match="JSON"):
        collect_trajectories(tmp_path)


def test_cli_never_overwrites_output(tmp_path: Path) -> None:
    _write_trial(tmp_path)
    output = tmp_path / "bundle.json"
    assert main([str(tmp_path), "--output", str(output)]) == 0
    first = output.read_bytes()
    assert main([str(tmp_path), "--output", str(output)]) == 1
    assert output.read_bytes() == first


def test_cli_removes_partial_output_before_retry(tmp_path: Path) -> None:
    _write_trial(tmp_path)
    output = tmp_path / "bundle.json"
    real_fdopen = os.fdopen

    def fail_write(descriptor: int, mode: str) -> None:
        stream = real_fdopen(descriptor, mode)
        stream.write(b'{"partial":')
        stream.close()
        raise OSError("disk full")

    with patch("kitaru_atif_importer.harbor.os.fdopen", side_effect=fail_write):
        assert main([str(tmp_path), "--output", str(output)]) == 1
    assert not output.exists()
    assert main([str(tmp_path), "--output", str(output)]) == 0
    assert len(json.loads(output.read_bytes())["trajectories"]) == 1
