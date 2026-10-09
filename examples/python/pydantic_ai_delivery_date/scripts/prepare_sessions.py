"""Prepare three delivery conversations and write a receipt for the MCP demo."""

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path
from uuid import UUID

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.imports import BlobImportSource, ImportCreateRequest
from kitaru.api_models.v1.session import SessionListParams
from kitaru.client import KitaruAPIClient

from delivery_date.models import get_scenario
from delivery_date.persistence import get_agent_id, record_result
from delivery_date.runner import run_scenario
from delivery_date.tracing import require_langfuse_environment, trace_scenario

CASES = (("missing-date", "control"), ("missing-date", "fix"), ("known-date", "fix"))


async def import_trace(
    payload: bytes, *, server_url: str, agent_name: str, timeout: float = 180
) -> tuple[UUID, UUID]:
    """Import a native export through the SDK's normal importer job path."""
    async with KitaruAPIClient(base_url=server_url) as client:
        agent_id = await get_agent_id(client, agent_name)
        blob = await client.blobs.upload(
            payload, media_type="application/json", filename="delivery-langfuse.json"
        )
        imported = await client.imports.create(
            ImportCreateRequest(
                importer="kitaru/langfuse",
                agent_id=agent_id,
                source=BlobImportSource(blob_id=blob.id),
                max_sessions=1,
            )
        )
        deadline = time.monotonic() + timeout
        while True:
            current = await client.imports.get(imported.id)
            if current.error or (current.stats and current.stats.failed):
                raise RuntimeError(f"Import {imported.id} failed; inspect it in Kitaru")
            if current.stats is not None:
                params = SessionListParams(
                    filter=FilterCondition(
                        field="import_id", op=FilterOp.EQ, value=str(imported.id)
                    )
                )
                sessions = await client.sessions.list(params)
                if len(sessions.items) != 1:
                    raise RuntimeError(
                        f"Import {imported.id} did not create one session"
                    )
                session = sessions.items[0]
                if session.agent_id != agent_id:
                    raise RuntimeError("Imported session belongs to the wrong agent")
                return session.id, imported.id
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Import {imported.id} pending; run the Kitaru importer worker and retry"
                )
            await asyncio.sleep(2)


async def prepare(args: argparse.Namespace) -> dict:
    """Run the requested source without falling back to another recording path."""
    if args.source == "langfuse":
        require_langfuse_environment()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    receipt = {"source": args.source, "backend": args.backend, "runs": []}
    for scenario_name, variant in CASES:
        scenario = get_scenario(scenario_name)
        source_url = import_id = None
        if args.source == "langfuse":
            result, payload, source_url = await trace_scenario(
                scenario, variant=variant, backend=args.backend, model=args.model
            )
            export_path = args.output.parent / f"{result.run_id}.langfuse.json"
            export_path.write_bytes(payload)
            session_id, import_id = await import_trace(
                payload, server_url=args.server_url, agent_name=args.agent
            )
        else:
            result = await run_scenario(
                scenario, variant=variant, backend=args.backend, model=args.model
            )
            session_id = await record_result(
                result, server_url=args.server_url, agent_name=args.agent
            )
        receipt["runs"].append(
            {
                "scenario": scenario_name,
                "variant": variant,
                "session_id": str(session_id),
                "import_id": str(import_id) if import_id else None,
                "source_url": source_url,
                "status": result.status,
                "model_name": result.model_name,
                "fixture_only": args.backend == "scripted",
            }
        )
        args.output.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    return receipt


def get_args() -> argparse.Namespace:
    """Parse explicit source and backend selection."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=("langfuse", "kitaru"), default="langfuse")
    parser.add_argument("--backend", choices=("openai", "scripted"), default="openai")
    parser.add_argument("--model", default="gpt-6-luna")
    parser.add_argument("--server-url", required=True)
    parser.add_argument("--agent", default="delivery-date-demo")
    parser.add_argument("--output", type=Path, default=Path("delivery-sessions.json"))
    return parser.parse_args()


if __name__ == "__main__":
    print(json.dumps(asyncio.run(prepare(get_args())), indent=2))
