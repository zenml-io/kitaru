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
"""Import job process entrypoint."""

import sys
import uuid
from collections.abc import Iterator
from pathlib import Path

from kitaru.api_models.v1.jobs import (
    MAX_IMPORT_FAILURES,
    ImportFailure,
    ImportStats,
    JobSpecImporter,
    JobUpdateRequest,
)
from kitaru.api_models.v1.session_nodes import SessionNodeBatchRequest
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, ConflictError
from kitaru.importing import (
    ParsedItem,
    ParsedNode,
    ParseFailure,
    Parser,
    SessionImportError,
    call_parser,
    flatten_nodes,
    session_request,
)
from kitaru.plugin_loader import (
    PluginLoadError,
    load_plugin_module,
    module_attribute,
    required_env,
    run_harness,
)

PLUGIN_MODULE_NAME = "kitaru_importer_plugin"
IMPORTER_LABEL = "Importer"
NODE_BATCH_SIZE = 20


def load_plugin_parser(path: Path, entrypoint: str) -> Parser:
    """Import a parser from a materialized code file.

    Args:
        path: Path of the code file.
        entrypoint: Attribute implementing the importer.

    Raises:
        SessionImportError: The file does not import, or the attribute is
            missing or not callable.

    Returns:
        Parser function.
    """
    try:
        module = load_plugin_module(PLUGIN_MODULE_NAME, path)
    except PluginLoadError as exc:
        raise SessionImportError(
            f"Failed to import importer code from {path}: {exc}"
        ) from exc
    try:
        return module_attribute(module, entrypoint, IMPORTER_LABEL)
    except PluginLoadError as exc:
        raise SessionImportError(str(exc)) from exc


def read_payload(path: Path) -> bytes:
    """Read the payload the worker materialized.

    Args:
        path: Path of the payload file.

    Raises:
        SessionImportError: The file does not read.

    Returns:
        Payload content.
    """
    try:
        return path.read_bytes()
    except OSError as exc:
        raise SessionImportError(f"Failed to read the payload from {path}") from exc


async def ingest_nodes(
    client: KitaruAPIClient,
    session_id: uuid.UUID,
    nodes: list[ParsedNode],
    batch_size: int = NODE_BATCH_SIZE,
) -> None:
    """Send the node tree of an imported session in batches.

    Args:
        client: API client.
        session_id: Id of the session.
        nodes: Root nodes of the tree.
        batch_size: Maximum nodes per request.

    Raises:
        APIError: An ingest request failed.
    """
    requests = flatten_nodes(nodes)
    for start in range(0, len(requests), batch_size):
        await client.session_nodes.upsert(
            session_id,
            SessionNodeBatchRequest(nodes=requests[start : start + batch_size]),
        )


class _StatsBuilder:
    """Running import stats."""

    def __init__(self) -> None:
        """Initialize the builder."""
        self.created = 0
        self.skipped = 0
        self.failed = 0
        self._failures: list[ImportFailure] = []

    def fail(self, line: int, external_id: str | None, error: str) -> None:
        """Count a failure and keep it while the sample has room.

        Args:
            line: Line the failure occurred on.
            external_id: External id of the failed session.
            error: Error message.
        """
        self.failed += 1
        if len(self._failures) < MAX_IMPORT_FAILURES:
            self._failures.append(
                ImportFailure(line=line, external_id=external_id, error=error)
            )

    def build(self) -> ImportStats:
        """Return the recorded stats.

        Returns:
            Import stats.
        """
        return ImportStats(
            created=self.created,
            skipped=self.skipped,
            failed=self.failed,
            failures=list(self._failures),
        )


async def import_sessions(
    client: KitaruAPIClient,
    importer: JobSpecImporter,
    parsed: Iterator[ParsedItem],
) -> ImportStats:
    """Ingest a stream of parsed items and report what landed.

    Items are consumed one at a time. A session the server already holds
    counts as skipped, a session whose creation or node ingest failed
    counts as failed alongside the parse failures the importer reported.

    Args:
        client: API client.
        importer: Importer of the job spec.
        parsed: Stream of parsed sessions and parse failures.

    Returns:
        Import stats.
    """
    stats = _StatsBuilder()
    for position, item in enumerate(parsed, start=1):
        if isinstance(item, ParseFailure):
            stats.fail(item.line, item.external_id, item.error)
            continue
        try:
            session = await client.sessions.create(session_request(importer, item))
        except ConflictError:
            stats.skipped += 1
            continue
        except APIError as exc:
            stats.fail(position, item.external_id, str(exc))
            continue
        try:
            await ingest_nodes(client, session.id, item.nodes)
        except APIError as exc:
            stats.fail(position, item.external_id, str(exc))
            continue
        stats.created += 1
    return stats.build()


async def import_job(client: KitaruAPIClient, job_id: uuid.UUID) -> ImportStats:
    """Import the payload of an import job and record the stats.

    Args:
        client: API client.
        job_id: Id of the job.

    Raises:
        SessionImportError: The job is not an import job, its importer
            does not load, or its payload does not read.
        APIError: The spec read or the stats update failed.

    Returns:
        Recorded stats.
    """
    spec = await client.jobs.get_spec(job_id)
    if spec.importer is None:
        raise SessionImportError(f"Job {job_id} is not an import job")
    parser = load_plugin_parser(
        Path(required_env("KITARU_JOB_PLUGIN_PATH", SessionImportError)),
        spec.importer.plugin.entrypoint,
    )
    payload = read_payload(
        Path(required_env("KITARU_JOB_PAYLOAD_PATH", SessionImportError))
    )
    stats = await import_sessions(
        client, spec.importer, call_parser(parser, payload, spec.importer.params)
    )
    await client.jobs.update(job_id, JobUpdateRequest(stats=stats))
    return stats


async def run() -> None:
    """Run the import job named by the process environment.

    Raises:
        SessionImportError: The environment is incomplete or the import
            failed.
        APIError: The spec read or the stats update failed.
    """
    job_id = uuid.UUID(required_env("KITARU_JOB_ID", SessionImportError))
    async with KitaruAPIClient(
        base_url=required_env("KITARU_API_URL", SessionImportError),
        api_key=required_env("KITARU_API_KEY", SessionImportError),
    ) as client:
        await import_job(client, job_id)


def main() -> int:
    """Run the import job process.

    Returns:
        Exit code.
    """
    return run_harness(run)


if __name__ == "__main__":
    sys.exit(main())
