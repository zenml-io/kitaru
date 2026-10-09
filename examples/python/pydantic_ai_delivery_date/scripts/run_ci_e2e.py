"""Run the provider-free demo against an isolated Kitaru database and server."""

import asyncio
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[4]
EXAMPLE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from devtools import stack  # noqa: E402


async def main() -> int:
    """Start only a private server/database and remove both after validation."""
    database = f"kitaru_delivery_demo_{uuid4().hex[:10]}"
    port = stack.get_free_port()
    await stack.ensure_postgres(start_missing=False)
    await stack.create_database(database)
    server = None
    try:
        with tempfile.TemporaryDirectory(prefix="kitaru-delivery-test-") as directory:
            log = Path(directory) / "server.log"
            server = stack.start_server(
                database,
                port,
                log,
                overrides={"KITARU_SERVER_ANALYTICS_OPT_IN": "false"},
            )
            url = f"http://127.0.0.1:{port}"
            await stack.wait_for_health(url, server, log)
            print(f"Testing isolated server at {url}", flush=True)
            process = await asyncio.create_subprocess_exec(
                str(EXAMPLE / ".venv" / "bin" / "python"),
                "-m",
                "pytest",
                "-q",
                cwd=EXAMPLE,
                env=os.environ
                | {"KITARU_DELIVERY_E2E_URL": url, "PYDANTIC_AI_NO_BANNER": "1"},
            )
            return await process.wait()
    finally:
        if server is not None:
            server.terminate()
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=10)
        await stack.drop_database(database)


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
