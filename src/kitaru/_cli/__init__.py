"""Internal CLI app wiring for Kitaru."""

from __future__ import annotations

from typing import Annotated

import cyclopts
from cyclopts import Parameter

_UNKNOWN_VERSION = "unknown"

app = cyclopts.App(
    name="kitaru",
    help=(
        "Record, replay, and improve AI agents in production. Create "
        "deployments with `kitaru deploy`; inspect existing deployments "
        "with `kitaru flow`."
    ),
    version=_UNKNOWN_VERSION,
    version_flags=["-V"],
)

log_store_app = cyclopts.App(
    name="log-store",
    help="Manage global runtime log-store settings.",
)
stack_app = cyclopts.App(
    name="stack",
    help="Inspect, create, delete, and switch stacks.",
)
project_app = cyclopts.App(
    name="project",
    help="Inspect, create, delete, and switch Kitaru projects.",
)
secrets_app = cyclopts.App(
    name="secrets",
    help="Manage centralized runtime secrets.",
)
model_app = cyclopts.App(
    name="model",
    help="Manage local model aliases for kitaru.llm().",
)
executions_app = cyclopts.App(
    name="executions",
    help="Inspect and manage flow executions.",
)
clean_app = cyclopts.App(
    name="clean",
    help="Reset Kitaru state.",
)
analytics_app = cyclopts.App(
    name="analytics",
    help="Manage anonymous usage analytics preferences.",
)
auth_app = cyclopts.App(
    name="auth",
    help="Manage active Kitaru server authentication helpers.",
)
auth_service_accounts_app = cyclopts.App(
    name="service-accounts",
    help="Manage Kitaru service accounts.",
)
auth_api_keys_app = cyclopts.App(
    name="api-keys",
    help="Manage service-account API keys.",
)
flow_app = cyclopts.App(
    name="flow",
    help=(
        "Inspect existing deployments and manage deployment routing. Create "
        "new deployments with `kitaru deploy`."
    ),
    version_flags=[],
)
flow_deployments_app = cyclopts.App(
    name="deployments",
    help="Inspect and manage existing deployment versions for a flow.",
    version_flags=[],
)

app.command(log_store_app)
app.command(stack_app)
app.command(project_app)
app.command(secrets_app)
app.command(model_app)
app.command(executions_app)
app.command(clean_app)
app.command(analytics_app)
app.command(auth_app)
auth_app.command(auth_service_accounts_app)
auth_app.command(auth_api_keys_app)
app.command(flow_app)
flow_app.command(flow_deployments_app)


@app.default
def main(
    version: Annotated[
        bool,
        Parameter(alias="--version", help="Show the Kitaru version and exit."),
    ] = False,
) -> None:
    """Show help when invoked without arguments."""
    if version:
        print(app.version)
        raise SystemExit(0)
    app.help_print()


from . import (  # noqa: F401,E402
    _analytics,
    _auth,
    _clean,
    _executions,
    _flows,
    _init,
    _models,
    _projects,
    _secrets,
    _stacks,
    _status,
)

__all__ = [
    "_UNKNOWN_VERSION",
    "analytics_app",
    "app",
    "auth_api_keys_app",
    "auth_app",
    "auth_service_accounts_app",
    "clean_app",
    "executions_app",
    "flow_app",
    "flow_deployments_app",
    "log_store_app",
    "main",
    "model_app",
    "project_app",
    "secrets_app",
    "stack_app",
]
