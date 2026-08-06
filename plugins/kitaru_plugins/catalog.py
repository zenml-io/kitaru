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
"""Default plugin catalog exposed to Kitaru servers."""

from typing import Literal, TypedDict


class PluginDefinition(TypedDict):
    """Plugin fields consumed by the Kitaru catalog loader."""

    kind: Literal["importer", "evaluator"]
    name: str
    description: str
    provider: str | None
    entrypoint: str


def get_definitions() -> list[PluginDefinition]:
    """Return the importers and evaluators provided by this distribution."""
    return [
        {
            "kind": "importer",
            "name": "kitaru/kitaru-jsonl",
            "description": "Import sessions matching the Kitaru JSONL contract.",
            "provider": "kitaru-jsonl",
            "entrypoint": "kitaru_plugins.importers.kitaru:parse",
        },
        {
            "kind": "importer",
            "name": "kitaru/langfuse",
            "description": "Import Langfuse JSON and JSONL trace exports.",
            "provider": "langfuse",
            "entrypoint": "kitaru_plugins.importers.langfuse:parse",
        },
        {
            "kind": "importer",
            "name": "kitaru/langsmith",
            "description": "Import LangSmith run-query and bulk-export records.",
            "provider": "langsmith",
            "entrypoint": "kitaru_plugins.importers.langsmith:parse",
        },
        {
            "kind": "importer",
            "name": "kitaru/braintrust",
            "description": "Import Braintrust project-log and UI exports.",
            "provider": "braintrust",
            "entrypoint": "kitaru_plugins.importers.braintrust:parse",
        },
        {
            "kind": "importer",
            "name": "kitaru/opentelemetry",
            "description": "Import OpenTelemetry, Arize, and Logfire JSON exports.",
            "provider": "opentelemetry",
            "entrypoint": "kitaru_plugins.importers.otlp:parse",
        },
        {
            "kind": "evaluator",
            "name": "kitaru/cost",
            "description": "Report the total recorded session cost.",
            "provider": None,
            "entrypoint": "kitaru_plugins.evaluators.basic:cost",
        },
        {
            "kind": "evaluator",
            "name": "kitaru/latency",
            "description": "Measure session wall-clock duration.",
            "provider": None,
            "entrypoint": "kitaru_plugins.evaluators.basic:latency",
        },
        {
            "kind": "evaluator",
            "name": "kitaru/tool-call-patterns",
            "description": "Count repeated calls to the same tool.",
            "provider": None,
            "entrypoint": "kitaru_plugins.evaluators.basic:tool_call_patterns",
        },
    ]
