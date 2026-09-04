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
"""Schema test for foreign key deletion rules."""

import kitaru.server.adapters.db.orm  # noqa: F401
from kitaru.server.adapters.db.orm.base import Base

# Expected `ondelete` for every foreign key in the schema, keyed by
# (table, column). `None` means Postgres NO ACTION, the ondelete argument
# left unset.
EXPECTED_ONDELETE: dict[tuple[str, str], str | None] = {
    ("api_key", "owner_id"): None,
    ("secret", "owner_id"): None,
    ("device", "account_id"): None,
    ("idempotency_key", "account_id"): None,
    ("agent", "owner_id"): None,
    ("blob", "owner_id"): None,
    ("job", "owner_id"): None,
    ("plugin", "owner_id"): None,
    ("plugin", "agent_id"): "SET NULL",
    ("replay_config", "owner_id"): None,
    ("tag", "owner_id"): None,
    ("worker", "owner_id"): None,
    ("agent_version", "agent_id"): "CASCADE",
    ("agent_version", "owner_id"): None,
    ("cohort", "agent_id"): "CASCADE",
    ("cohort", "owner_id"): None,
    ("cohort_version", "cohort_id"): "CASCADE",
    ("cohort_version", "owner_id"): None,
    ("experiment", "agent_id"): "CASCADE",
    ("experiment", "owner_id"): None,
    ("experiment", "replay_config_id"): None,
    ("plugin_version", "blob_id"): None,
    ("plugin_version", "plugin_id"): "CASCADE",
    ("tag_link", "tag_id"): "CASCADE",
    ("tag_link", "session_id"): "CASCADE",
    ("tag_link", "cohort_id"): "CASCADE",
    ("tag_link", "cohort_version_id"): "CASCADE",
    ("tag_link", "agent_version_id"): "CASCADE",
    ("tag_link", "experiment_id"): "CASCADE",
    ("tag_link", "experiment_run_id"): "CASCADE",
    ("agent_version_secret", "agent_version_id"): "CASCADE",
    ("agent_version_secret", "secret_id"): None,
    ("experiment_run", "agent_version_id"): None,
    ("experiment_run", "cohort_version_id"): None,
    ("experiment_run", "experiment_id"): "CASCADE",
    ("experiment_run", "owner_id"): None,
    ("session", "agent_id"): "CASCADE",
    ("session", "agent_version_id"): "SET NULL",
    ("session", "owner_id"): None,
    ("session", "task_id"): "SET NULL",
    ("session", "import_id"): "SET NULL",
    ("session", "inputs_blob_id"): None,
    ("session", "outputs_blob_id"): None,
    ("cohort_version_session", "cohort_version_id"): "CASCADE",
    ("cohort_version_session", "session_id"): None,
    ("replay", "baseline_session_id"): None,
    ("replay", "experiment_run_id"): "CASCADE",
    ("replay", "job_id"): "SET NULL",
    ("replay", "owner_id"): None,
    ("replay", "replay_config_id"): None,
    ("replay", "result_session_id"): None,
    ("session_node", "session_id"): "CASCADE",
    ("session_node", "inputs_blob_id"): None,
    ("session_node", "outputs_blob_id"): None,
    ("session_node", "attributes_blob_id"): None,
    ("session_node", "reasoning_blob_id"): None,
    # A task names its inputs by id and carries no constraint to them, so
    # agent_version_id, import_id, input_session_id, and plugin_version_id are
    # absent here. Only the job a task belongs to and the worker holding it
    # stay constrained.
    ("task", "job_id"): "CASCADE",
    ("task", "worker_id"): "SET NULL",
    # evaluator_version_id carries no constraint, an evaluator-born row keeps
    # this id forever, even after the plugin version it references is deleted.
    ("evaluation", "owner_id"): None,
    ("evaluation", "session_id"): "CASCADE",
    ("evaluation", "task_id"): "SET NULL",
    ("replay_evaluation", "replay_id"): "CASCADE",
    ("replay_evaluation", "evaluation_id"): "CASCADE",
    ("investigation", "agent_id"): "CASCADE",
    ("investigation", "owner_id"): None,
    ("investigation_session", "investigation_id"): "CASCADE",
    ("investigation_session", "session_id"): None,
    ("annotation", "investigation_session_id"): "CASCADE",
    ("annotation", "owner_id"): None,
    ("annotation", "session_id"): "CASCADE",
    ("insight", "agent_id"): "CASCADE",
    ("insight", "owner_id"): None,
    ("import", "agent_id"): "CASCADE",
    ("import", "agent_version_id"): "SET NULL",
    ("import", "importer_version_id"): None,
    ("import", "job_id"): "SET NULL",
    ("import", "owner_id"): None,
    ("import", "payload_blob_id"): None,
}


def test_foreign_key_ondelete_matches_expected() -> None:
    """Every foreign key's ondelete rule matches the expected mapping."""
    actual: dict[tuple[str, str], str | None] = {}
    for table in Base.metadata.tables.values():
        for fk_constraint in table.foreign_key_constraints:
            for column_name in fk_constraint.column_keys:
                actual[(table.name, column_name)] = fk_constraint.ondelete

    unknown = set(actual) - set(EXPECTED_ONDELETE)
    assert not unknown, f"Foreign keys missing from the expected mapping: {unknown}"

    missing = set(EXPECTED_ONDELETE) - set(actual)
    assert not missing, f"Expected foreign keys no longer in the schema: {missing}"

    mismatched = {
        key: (actual[key], expected)
        for key, expected in EXPECTED_ONDELETE.items()
        if actual[key] != expected
    }
    assert not mismatched, (
        f"Foreign keys with an unexpected ondelete, as (actual, expected): {mismatched}"
    )
