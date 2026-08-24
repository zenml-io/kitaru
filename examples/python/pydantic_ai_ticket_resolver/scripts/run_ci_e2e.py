"""Run the quickstart example against an isolated Kitaru server."""

import asyncio
import os
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

import asyncpg

ROOT = Path(__file__).parents[1]


def _read_log(path: Path) -> str:
    """Return the server log when it exists."""
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        return "<server log was not created>"


def _wait_for_server(
    process: subprocess.Popen[str], log_path: Path, server_url: str
) -> None:
    """Wait for the local server to report healthy or fail with its log."""
    deadline = time.monotonic() + 60
    health_url = f"{server_url}/health/live"
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"Kitaru server exited with {process.returncode}.")
        try:
            with urllib.request.urlopen(health_url, timeout=1) as response:
                if response.status == 200:
                    return
        except (TimeoutError, urllib.error.URLError):
            time.sleep(1)
    raise RuntimeError("Kitaru server did not become healthy.")


def _stop_process(process: subprocess.Popen[str]) -> None:
    """Stop a child process without leaving it orphaned."""
    if process.poll() is not None:
        return
    if os.name == "posix":
        os.killpg(process.pid, signal.SIGTERM)
    else:
        process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        if os.name == "posix":
            os.killpg(process.pid, signal.SIGKILL)
        else:
            process.kill()
        process.wait(timeout=10)


def _get_server_environment() -> dict[str, str]:
    """Build an isolated server environment from example-specific settings."""
    environment = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith("KITARU_SERVER_")
    }
    environment.update(
        {
            "KITARU_SERVER_AUTH_SCHEME": "none",
            "KITARU_SERVER_DB_HOST": "127.0.0.1",
            "KITARU_SERVER_DB_NAME": f"kitaru_example_{uuid.uuid4().hex}",
            "KITARU_SERVER_DB_PORT": os.environ.get("KITARU_EXAMPLE_DB_PORT", "5433"),
            "KITARU_SERVER_DB_PWD": "password",
            "KITARU_SERVER_SECRET_ENCRYPTION_KEY": (
                "ci-only-encryption-key-ci-only-encryption-key"
            ),
            "KITARU_SERVER_JWT_SIGNING_KEY": (
                "ci-only-signing-key-ci-only-signing-key"
            ),
            "KITARU_SERVER_ANALYTICS_OPT_IN": "false",
        }
    )
    return environment


async def _drop_database(environment: dict[str, str]) -> None:
    """Drop the unique database created for this example run."""
    database_name = environment["KITARU_SERVER_DB_NAME"]
    if not database_name.startswith("kitaru_example_"):
        raise RuntimeError(f"Refusing to drop unexpected database {database_name!r}.")
    connection = await asyncpg.connect(
        host=environment["KITARU_SERVER_DB_HOST"],
        port=int(environment["KITARU_SERVER_DB_PORT"]),
        user=environment.get("KITARU_SERVER_DB_USER", "postgres"),
        password=environment["KITARU_SERVER_DB_PWD"],
        database="postgres",
    )
    try:
        await connection.execute(
            f'DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)'
        )
    finally:
        await connection.close()


def _run_e2e_test(environment: dict[str, str]) -> int:
    """Run pytest in a process group so a timeout stops its worker descendants."""
    process = subprocess.Popen(
        [sys.executable, "-m", "pytest", "-q", "tests/test_e2e.py"],
        cwd=ROOT,
        env=environment,
        text=True,
        start_new_session=os.name == "posix",
    )
    try:
        return process.wait(timeout=600)
    except subprocess.TimeoutExpired:
        print("Quickstart example timed out after 600 seconds.", file=sys.stderr)
        _stop_process(process)
        return 1


def main() -> int:
    """Start Kitaru, run the end-to-end contract, and stop Kitaru."""
    server_port = os.environ.get("KITARU_EXAMPLE_SERVER_PORT", "8000")
    server_url = f"http://127.0.0.1:{server_port}"
    server_environment = _get_server_environment()

    test_environment = os.environ.copy()
    test_environment["KITARU_QUICKSTART_E2E"] = "1"
    test_environment["KITARU_QUICKSTART_SERVER_URL"] = server_url
    test_environment["KITARU_QUICKSTART_API_KEY"] = "quickstart-ci-worker"

    return_code = 1
    with tempfile.TemporaryDirectory(prefix="kitaru-example-e2e-") as temporary:
        log_path = Path(temporary) / "kitaru-server.log"
        worker_log_path = Path(temporary) / "kitaru-worker.log"
        test_environment["KITARU_QUICKSTART_WORKER_LOG"] = str(worker_log_path)
        with log_path.open("w", encoding="utf-8") as server_log:
            server = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "uvicorn",
                    "kitaru.server.api.main:app",
                    "--factory",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    server_port,
                ],
                cwd=ROOT,
                env=server_environment,
                text=True,
                stdout=server_log,
                stderr=subprocess.STDOUT,
                start_new_session=os.name == "posix",
            )
            try:
                _wait_for_server(server, log_path, server_url)
                return_code = _run_e2e_test(test_environment)
                if return_code != 0:
                    print(_read_log(log_path), file=sys.stderr)
                    print(_read_log(worker_log_path), file=sys.stderr)
                else:
                    print("Quickstart example end-to-end contract passed.")
            except RuntimeError as error:
                print(error, file=sys.stderr)
                print(_read_log(log_path), file=sys.stderr)
            finally:
                _stop_process(server)
        try:
            asyncio.run(_drop_database(server_environment))
        except Exception as error:
            print(f"Failed to drop the example database: {error}", file=sys.stderr)
            return 1
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
