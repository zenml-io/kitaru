"""Seed a kitaru server with demo data for the dev dashboard.

Read-only dashboards need something to show; this script creates a small,
plausible object graph through the public REST API: agents with versions,
sessions with node trees and evaluations, evaluators, a cohort, an
experiment with a run (whose replays stay pending without a worker), a
standalone replay, and a live worker.

Usage:
    uv run python dashboard/scripts/seed.py [--url http://localhost:8000] [--api-key KITKEY_...]
"""

import argparse
import random
import sys
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

random.seed(7)


class Seeder:
    def __init__(self, base_url: str, api_key: str | None) -> None:
        headers = {}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        self.client = httpx.Client(base_url=base_url, headers=headers, timeout=30)

    def _request(self, method: str, path: str, payload: dict[str, Any] | None) -> dict[str, Any]:
        response = self.client.request(method, path, json=payload)
        if response.status_code >= 400:
            raise RuntimeError(f"{method} {path} -> {response.status_code}: {response.text}")
        return response.json()

    def post(self, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        return self._request("POST", path, payload)

    def patch(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("PATCH", path, payload)

    def seed(self) -> None:
        now = datetime.now(UTC)

        docs_agent = self.post(
            "/v1/agents",
            {"name": "docs-agent", "description": "Answers policy questions from the docs corpus."},
        )
        support_bot = self.post(
            "/v1/agents",
            {"name": "support-bot", "description": "Front-line support triage bot."},
        )
        print(f"agents: {docs_agent['id']} (docs-agent), {support_bot['id']} (support-bot)")

        docs_v1 = self.post(
            f"/v1/agents/{docs_agent['id']}/versions",
            {
                "description": "Initial wrapped entrypoint.",
                "capabilities": {
                    "tools": ["lookup_document", "search_index"],
                    "mcp_servers": [],
                    "skills": [],
                },
                "run_spec": {"command": "python -m app.kitaru_entry", "env": {}},
            },
        )
        docs_v2 = self.post(
            f"/v1/agents/{docs_agent['id']}/versions",
            {
                "display_version": "pr-311",
                "description": "Rejects superseded documents before citing.",
                "capabilities": {
                    "tools": ["lookup_document", "search_index"],
                    "mcp_servers": [],
                    "skills": [],
                },
                # Replays require the agent version to carry a run spec.
                "run_spec": {"command": "python -m app.kitaru_entry", "env": {}},
            },
        )
        support_v1 = self.post(f"/v1/agents/{support_bot['id']}/versions", {})
        print(f"agent versions: docs v1 {docs_v1['id']}, docs v2 {docs_v2['id']}, support v1 {support_v1['id']}")

        evaluator_names: list[str] = []
        for name, description, entrypoint in [
            ("cited-superseded-doc", "Fails when a superseded document is cited.", "kitaru_evals:cited_superseded_doc"),
            ("latency", "Wall-clock seconds from start to end.", "kitaru_evals:latency"),
            ("policy-freshness", "Age bucket of the cited policy document.", "kitaru_evals:policy_freshness"),
        ]:
            evaluator = self.post("/v1/evaluators", {"name": name, "description": description})
            self.post(
                f"/v1/evaluators/{evaluator['id']}/versions",
                {
                    "source": {
                        "type": "package",
                        "requirement": "kitaru-evals==0.1.0",
                        "entrypoint": entrypoint,
                    }
                },
            )
            evaluator_names.append(name)
        print(f"evaluators: {', '.join(evaluator_names)}")

        session_ids: list[str] = []
        superseded_ids: list[str] = []
        for index in range(15):
            failed = index % 5 == 4
            in_progress = index == 14
            cites_superseded = index % 3 == 0 and not in_progress
            version = docs_v1 if index < 10 else docs_v2
            started = now - timedelta(hours=30 - index * 2)
            duration = timedelta(seconds=random.randint(8, 240))
            if in_progress:
                status = "in_progress"
            elif failed:
                status = "failed"
            else:
                status = "completed"
            question = random.choice(
                [
                    "What is the current travel reimbursement limit?",
                    "Which security policy covers laptop encryption?",
                    "How many vacation days carry over?",
                    "What is the incident escalation path?",
                ]
            )
            imported = index < 6
            outputs = (
                None
                if in_progress
                else {"answer": "Cited policy DOC-3311." if cites_superseded else "Cited policy DOC-4180."}
            )
            error = "ToolTimeout: search_index took longer than 30s" if failed else None
            ended_at = None if in_progress else (started + duration).isoformat()
            # Recorded sessions only accept node ingestion while in progress, so
            # they are created open and finished after their nodes are posted.
            # Imported sessions accept nodes in any state.
            session = self.post(
                "/v1/sessions",
                {
                    "agent_id": docs_agent["id"],
                    "agent_version_id": version["id"],
                    "origin": "imported" if imported else "recorded",
                    "status": status if imported else "in_progress",
                    "name": f"docs-run-{index + 1:02d}",
                    "inputs": {"question": question},
                    "outputs": outputs if imported else None,
                    "expected": None,
                    "error": error if imported else None,
                    "started_at": started.isoformat(),
                    "ended_at": ended_at if imported else None,
                    "provider": "openai" if index % 2 == 0 else "anthropic",
                    "framework": "pydantic-ai",
                    "metadata": {"import_batch": "langfuse-2026-07"} if imported else {},
                },
            )
            session_ids.append(session["id"])
            if cites_superseded:
                superseded_ids.append(session["id"])

            doc_id = "DOC-3311" if cites_superseded else "DOC-4180"
            nodes = [
                {
                    "index": 0,
                    "node_type": "span",
                    "name": "run",
                    "status": "completed",
                    "inputs": {"question": question},
                    "outputs": None,
                    "attributes": {},
                    "started_at": started.isoformat(),
                    "ended_at": (started + duration).isoformat(),
                },
                {
                    "index": 1,
                    "parent_index": 0,
                    "node_type": "llm_call",
                    "name": "plan",
                    "status": "completed",
                    "inputs": {"messages": [{"role": "user", "content": question}]},
                    "outputs": {"tool_calls": [{"name": "search_index"}]},
                    "attributes": {},
                    "model": "gpt-5",
                    "provider": "openai",
                    "tokens": {"input_tokens": 812, "output_tokens": 64},
                    "cost": "0.0043",
                    "started_at": started.isoformat(),
                    "ended_at": (started + timedelta(seconds=2)).isoformat(),
                },
                {
                    "index": 2,
                    "parent_index": 0,
                    "node_type": "tool_call",
                    "name": "search_index",
                    "tool_name": "search_index",
                    "status": "failed" if failed else "completed",
                    "inputs": {"query": question},
                    "outputs": None if failed else {"hits": [doc_id, "DOC-2044"]},
                    "error": error,
                    "attributes": {},
                    "started_at": (started + timedelta(seconds=2)).isoformat(),
                    "ended_at": (started + timedelta(seconds=6)).isoformat(),
                },
            ]
            if not failed:
                nodes.extend(
                    [
                        {
                            "index": 3,
                            "parent_index": 0,
                            "node_type": "tool_call",
                            "name": "lookup_document",
                            "tool_name": "lookup_document",
                            "status": "completed",
                            "inputs": {"doc_id": doc_id},
                            "outputs": {
                                "doc_id": doc_id,
                                "status": "superseded" if cites_superseded else "current",
                                "superseded_by": "DOC-4180" if cites_superseded else None,
                            },
                            "attributes": {},
                            "started_at": (started + timedelta(seconds=6)).isoformat(),
                            "ended_at": (started + timedelta(seconds=7)).isoformat(),
                        },
                        {
                            "index": 4,
                            "parent_index": 0,
                            "node_type": "llm_call",
                            "name": "answer",
                            "status": "in_progress" if in_progress else "completed",
                            "inputs": {"context": [doc_id]},
                            "outputs": None if in_progress else {"answer": f"Cited policy {doc_id}."},
                            "attributes": {},
                            "model": "claude-sonnet-5",
                            "provider": "anthropic",
                            "tokens": {"input_tokens": 2140, "output_tokens": 188, "reasoning_tokens": 96},
                            "cost": "0.0121",
                            "started_at": (started + timedelta(seconds=7)).isoformat(),
                            "ended_at": None if in_progress else (started + duration).isoformat(),
                        },
                    ]
                )
            self.post(f"/v1/sessions/{session['id']}/nodes", {"nodes": nodes})

            if not imported and not in_progress:
                self.patch(
                    f"/v1/sessions/{session['id']}",
                    {
                        "status": status,
                        "outputs": outputs,
                        "error": error,
                        "ended_at": ended_at,
                    },
                )

            if status == "completed":
                self.post(
                    f"/v1/sessions/{session['id']}/evaluations",
                    {
                        "evaluations": [
                            {
                                "name": "cited-superseded-doc",
                                "score": cites_superseded,
                                "explanation": f"cited {doc_id}, superseded by DOC-4180"
                                if cites_superseded
                                else None,
                            },
                            {"name": "latency", "score": duration.total_seconds()},
                            {
                                "name": "policy-freshness",
                                "score": 0.0 if cites_superseded else 1.0,
                                "value": "stale" if cites_superseded else "fresh",
                            },
                        ]
                    },
                )
        print(f"sessions: {len(session_ids)} for docs-agent ({len(superseded_ids)} cite superseded docs)")

        support_session = self.post(
            "/v1/sessions",
            {
                "agent_id": support_bot["id"],
                "agent_version_id": support_v1["id"],
                "origin": "recorded",
                "status": "completed",
                "name": "support-run-01",
                "inputs": {"ticket": "Cannot log in after password reset"},
                "outputs": {"resolution": "escalated"},
                "expected": None,
                "started_at": (now - timedelta(hours=2)).isoformat(),
                "ended_at": (now - timedelta(hours=2) + timedelta(seconds=41)).isoformat(),
                "provider": "anthropic",
            },
        )
        print(f"support session: {support_session['id']}")

        cohort = self.post(
            "/v1/cohorts",
            {
                "name": "cites-superseded-policy",
                "description": "Sessions where the agent cited a document that has been superseded.",
                "agent_id": docs_agent["id"],
                "session_ids": superseded_ids,
            },
        )
        print(f"cohort: {cohort['id']} ({cohort['session_count']} sessions)")

        experiment = self.post(
            "/v1/experiments",
            {
                "name": "fix-validation",
                "description": "Does the superseded-document rejection in pr-311 hold on the failing cohort?",
                "evaluators": [
                    {"evaluator": "cited-superseded-doc"},
                    {"evaluator": "latency"},
                ],
                "tool_policy": {
                    "default": {"type": "history", "scope": "baseline", "on_miss": "error_result"},
                },
            },
        )
        run = self.post(
            f"/v1/experiments/{experiment['id']}/runs",
            {"cohort_id": cohort["id"], "agent_version_id": docs_v2["id"], "evaluate_baselines": True},
        )
        print(f"experiment: {experiment['id']}, run #{run['number']}: {run['id']}")

        standalone = self.post(
            "/v1/replays",
            {
                "baseline_session_id": superseded_ids[0],
                "agent_version_id": docs_v2["id"],
                "evaluators": [{"evaluator": "cited-superseded-doc"}],
                "tool_policy": {
                    "default": {"type": "history", "scope": "baseline", "on_miss": "passthrough"},
                },
            },
        )
        print(f"standalone replay: {standalone['id']}")

        worker = self.post(
            "/v1/workers",
            {
                "name": "dev-laptop",
                "scope": {"kinds": ["agent", "evaluator"]},
                "runtime": {
                    "platform": "local",
                    "hostname": "dev-laptop.local",
                    "os": "linux",
                    "arch": "x86_64",
                    "python_version": "3.13.1",
                    "kitaru_version": "0.21.0",
                },
            },
        )
        self.post(f"/v1/workers/{worker['id']}/heartbeat")
        print(f"worker: {worker['id']} (heartbeat sent)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--api-key", default=None)
    args = parser.parse_args()

    try:
        Seeder(args.url, args.api_key).seed()
    except (httpx.HTTPError, RuntimeError) as error:
        print(f"Seeding failed: {error}", file=sys.stderr)
        sys.exit(1)
    print("Done.")


if __name__ == "__main__":
    main()
