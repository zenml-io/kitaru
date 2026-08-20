"""Local Kitaru server, database, and worker lifecycle helpers."""

import argparse
import asyncio
import json
import os
import signal
import socket
import subprocess
import sys
from pathlib import Path
from typing import Any

import asyncpg
import httpx

from kitaru.api_models.v1.api_key import ApiKeyCreateRequest
from kitaru.api_models.v1.info import AuthScheme
from kitaru.client.api_client import KitaruAPIClient
from kitaru.worker import Worker, WorkerConfig

DEVTOOLS_DIR = Path(__file__).resolve().parent
REPO_ROOT = DEVTOOLS_DIR.parent
RUN_DIR = DEVTOOLS_DIR / ".run"
STATE_FILE = RUN_DIR / "server.json"

DB_HOST = "localhost"
DB_PORT = 5433
DB_USER = "postgres"
DB_PWD = "password"
DB_CONTAINER = "kitaru_db"
DEFAULT_DB_NAME = "kitaru_dev"

DEFAULT_PORT = 8300
DEFAULT_ACCOUNT_NAME = "default"
DEFAULT_ACCOUNT_PASSWORD = "password"
JWT_SIGNING_KEY = "devtools-signing-key"
SECRET_ENCRYPTION_KEY = "devtools-encryption-key"

POSTGRES_TIMEOUT_SECONDS = 60.0
HEALTH_TIMEOUT_SECONDS = 30.0
POLL_INTERVAL_SECONDS = 0.2


async def _connect_admin() -> asyncpg.Connection:
    """Connect to the admin database on the local test Postgres."""
    return await asyncio.wait_for(
        asyncpg.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PWD,
            database="postgres",
        ),
        timeout=10.0,
    )


async def ensure_postgres(start_missing: bool = True) -> None:
    """Make the local test Postgres reachable, starting it when missing."""
    try:
        conn = await _connect_admin()
    except (TimeoutError, OSError, asyncpg.PostgresError):
        if not start_missing:
            raise
        print(f"Postgres not reachable on {DB_HOST}:{DB_PORT}, starting it ...")
        # The kitaru_db container may belong to another worktree's compose
        # project, so restart it by name before composing a new one.
        started = subprocess.run(["docker", "start", DB_CONTAINER], capture_output=True)
        if started.returncode != 0:
            subprocess.run(
                ["docker", "compose", "up", "-d", "db"], cwd=REPO_ROOT, check=True
            )
        deadline = asyncio.get_event_loop().time() + POSTGRES_TIMEOUT_SECONDS
        while True:
            try:
                conn = await _connect_admin()
                break
            except (TimeoutError, OSError, asyncpg.PostgresError):
                if asyncio.get_event_loop().time() >= deadline:
                    raise
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
    await conn.close()


async def create_database(name: str, drop_existing: bool = False) -> None:
    """Create a database, optionally dropping an existing one first."""
    conn = await _connect_admin()
    try:
        if drop_existing:
            await conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        exists = await conn.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1", name
        )
        if not exists:
            await conn.execute(f'CREATE DATABASE "{name}"')
    finally:
        await conn.close()


async def drop_database(name: str) -> None:
    """Drop a database when it exists."""
    conn = await _connect_admin()
    try:
        await conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
    finally:
        await conn.close()


async def list_databases(prefix: str = "kitaru") -> list[str]:
    """List databases whose names start with the prefix."""
    conn = await _connect_admin()
    try:
        rows = await conn.fetch(
            "SELECT datname FROM pg_database WHERE datname LIKE $1 ORDER BY datname",
            f"{prefix}%",
        )
    finally:
        await conn.close()
    return [row["datname"] for row in rows]


async def _terminate_backends(conn: asyncpg.Connection, name: str) -> None:
    """Terminate every backend connected to a database."""
    await conn.execute(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
        "WHERE datname = $1 AND pid <> pg_backend_pid()",
        name,
    )


async def snapshot_database(source: str, snapshot: str) -> None:
    """Copy a database into a snapshot, replacing an existing snapshot."""
    conn = await _connect_admin()
    try:
        await conn.execute(f'DROP DATABASE IF EXISTS "{snapshot}" WITH (FORCE)')
        # Template copies require the source to have no active connections, so
        # the server's pool is briefly disconnected and reconnects on use.
        await _terminate_backends(conn, source)
        await conn.execute(f'CREATE DATABASE "{snapshot}" TEMPLATE "{source}"')
    finally:
        await conn.close()


async def restore_database(snapshot: str, target: str) -> None:
    """Recreate a database from a snapshot, dropping the current contents."""
    conn = await _connect_admin()
    try:
        exists = await conn.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1", snapshot
        )
        if not exists:
            raise RuntimeError(f"Snapshot database {snapshot!r} does not exist")
        await conn.execute(f'DROP DATABASE IF EXISTS "{target}" WITH (FORCE)')
        await _terminate_backends(conn, snapshot)
        await conn.execute(f'CREATE DATABASE "{target}" TEMPLATE "{snapshot}"')
    finally:
        await conn.close()


async def clean_databases(prefixes: list[str], keep: set[str]) -> list[str]:
    """Drop databases matching any prefix, keeping the given names."""
    dropped: list[str] = []
    for prefix in prefixes:
        for name in await list_databases(prefix):
            if name in keep:
                continue
            await drop_database(name)
            dropped.append(name)
    return dropped


def get_free_port() -> int:
    """Find a free localhost TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def server_env(
    db_name: str,
    port: int,
    auth_scheme: str = "none",
    overrides: dict[str, str] | None = None,
) -> dict[str, str]:
    """Build the environment for a local server process."""
    env = dict(os.environ)
    env.update(
        {
            "KITARU_SERVER_DB_HOST": DB_HOST,
            "KITARU_SERVER_DB_PORT": str(DB_PORT),
            "KITARU_SERVER_DB_USER": DB_USER,
            "KITARU_SERVER_DB_PWD": DB_PWD,
            "KITARU_SERVER_DB_NAME": db_name,
            "KITARU_SERVER_AUTH_SCHEME": auth_scheme,
            "KITARU_SERVER_JWT_SIGNING_KEY": JWT_SIGNING_KEY,
            "KITARU_SERVER_SECRET_ENCRYPTION_KEY": SECRET_ENCRYPTION_KEY,
            "KITARU_SERVER_DEFAULT_ACCOUNT_PASSWORD": DEFAULT_ACCOUNT_PASSWORD,
            "KITARU_SERVER_HOST": "127.0.0.1",
            "KITARU_SERVER_PORT": str(port),
            "KITARU_SERVER_ANALYTICS_DEBUG": "true",
        }
    )
    env.update(overrides or {})
    return env


def start_server(
    db_name: str,
    port: int,
    log_path: Path,
    auth_scheme: str = "none",
    overrides: dict[str, str] | None = None,
) -> subprocess.Popen[bytes]:
    """Start the API server from source as a subprocess."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_file = log_path.open("wb")
    return subprocess.Popen(
        [sys.executable, "-m", "kitaru.server.api.main"],
        env=server_env(db_name, port, auth_scheme, overrides),
        cwd=REPO_ROOT,
        stdout=log_file,
        stderr=subprocess.STDOUT,
    )


async def wait_for_health(
    base_url: str,
    proc: subprocess.Popen[bytes] | None = None,
    log_path: Path | None = None,
    timeout: float = HEALTH_TIMEOUT_SECONDS,
) -> None:
    """Poll /health/live until it answers, bounded by a hard timeout."""
    deadline = asyncio.get_event_loop().time() + timeout

    def log_tail() -> str:
        """Return the tail of the server log when available."""
        if log_path is None or not log_path.exists():
            return ""
        return "\nLog tail:\n" + log_path.read_text(encoding="utf-8")[-4000:]

    async with httpx.AsyncClient() as http:
        while True:
            if proc is not None and proc.poll() is not None:
                raise RuntimeError(
                    f"Server exited early with code {proc.returncode}.{log_tail()}"
                )
            try:
                response = await http.get(f"{base_url}/health/live", timeout=2.0)
                if response.status_code == 200:
                    return
            except httpx.TransportError:
                pass
            if asyncio.get_event_loop().time() >= deadline:
                raise TimeoutError(
                    f"Server not healthy within {timeout:.0f}s.{log_tail()}"
                )
            await asyncio.sleep(POLL_INTERVAL_SECONDS)


async def bootstrap_api_key(base_url: str) -> str | None:
    """Issue an API key, or nothing when the server does not authenticate."""
    async with KitaruAPIClient(base_url=base_url) as anon_client:
        info = await anon_client.info.get()
        if info.auth_scheme is AuthScheme.NONE:
            return None
        token = await anon_client.auth.login(
            username=DEFAULT_ACCOUNT_NAME, password=DEFAULT_ACCOUNT_PASSWORD
        )
    async with KitaruAPIClient(
        base_url=base_url, api_key=token.access_token
    ) as jwt_client:
        issued = await jwt_client.api_keys.create(ApiKeyCreateRequest(name="devtools"))
    return issued.key


def _write_state(state: dict[str, Any]) -> None:
    """Persist the running server state."""
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def _read_state() -> dict[str, Any] | None:
    """Read the running server state when present."""
    if not STATE_FILE.exists():
        return None
    return json.loads(STATE_FILE.read_text(encoding="utf-8"))


def _pid_alive(pid: int) -> bool:
    """Check whether a process with the pid is alive."""
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


async def run_workers(
    count: int,
    concurrency: int,
    stop_event: asyncio.Event,
    name_prefix: str = "devtools-worker",
) -> list[asyncio.Task[None]]:
    """Start in-process workers polling the configured server."""
    tasks: list[asyncio.Task[None]] = []
    for index in range(count):
        worker = Worker(
            WorkerConfig(
                name=f"{name_prefix}-{index + 1}",
                concurrency=concurrency,
                poll_interval=0.2,
                heartbeat_interval=0.5,
            )
        )
        tasks.append(asyncio.create_task(worker.run(stop_event)))
    return tasks


async def _up(args: argparse.Namespace) -> None:
    """Start the database and server."""
    if args.docker:
        subprocess.run(["docker", "compose", "up", "-d"], cwd=REPO_ROOT, check=True)
        base_url = "http://localhost:8000"
        await wait_for_health(base_url, timeout=120.0)
        print(f"Server healthy at {base_url} (docker compose)")
        print(f"export KITARU_API_URL={base_url}")
        return

    state = _read_state()
    if state and _pid_alive(state["pid"]):
        print(f"Server already running at {state['base_url']} (pid {state['pid']})")
        print(f"export KITARU_API_URL={state['base_url']}")
        return

    await ensure_postgres()
    await create_database(args.db_name, drop_existing=args.fresh)
    port = args.port or get_free_port()
    base_url = f"http://127.0.0.1:{port}"
    log_path = RUN_DIR / "server.log"
    overrides = dict(pair.split("=", 1) for pair in args.env)
    proc = start_server(
        args.db_name, port, log_path, auth_scheme=args.auth, overrides=overrides
    )
    await wait_for_health(base_url, proc, log_path)
    api_key = await bootstrap_api_key(base_url)
    _write_state(
        {
            "pid": proc.pid,
            "port": port,
            "base_url": base_url,
            "db_name": args.db_name,
            "auth": args.auth,
            "log": str(log_path),
        }
    )
    print(f"Server healthy at {base_url} (pid {proc.pid}, db {args.db_name})")
    print(f"export KITARU_API_URL={base_url}")
    if api_key is not None:
        print(f"export KITARU_API_KEY={api_key}")


async def _down(args: argparse.Namespace) -> None:
    """Stop the server and optionally drop its database."""
    if args.docker:
        subprocess.run(
            ["docker", "compose", "stop", "server"], cwd=REPO_ROOT, check=True
        )
        return
    state = _read_state()
    if state is None:
        print("No server state recorded.")
        return
    if _pid_alive(state["pid"]):
        os.kill(state["pid"], signal.SIGTERM)
        print(f"Stopped server pid {state['pid']}.")
    else:
        print(f"Server pid {state['pid']} already gone.")
    STATE_FILE.unlink()
    if args.drop_db:
        await drop_database(state["db_name"])
        print(f"Dropped database {state['db_name']}.")


async def _status(args: argparse.Namespace) -> None:
    """Report the local stack status."""
    _ = args
    state = _read_state()
    if state is None:
        print("No process-mode server recorded.")
    else:
        alive = _pid_alive(state["pid"])
        print(
            f"Server pid {state['pid']} ({'alive' if alive else 'dead'}) "
            f"at {state['base_url']}, db {state['db_name']}"
        )
    try:
        await ensure_postgres(start_missing=False)
        names = await list_databases()
        print(f"Postgres reachable on {DB_HOST}:{DB_PORT}, databases: {names}")
    except (TimeoutError, OSError, asyncpg.PostgresError):
        print(f"Postgres not reachable on {DB_HOST}:{DB_PORT}.")


async def _workers(args: argparse.Namespace) -> None:
    """Run in-process workers until interrupted."""
    if "KITARU_API_URL" not in os.environ:
        state = _read_state()
        if state is None:
            raise RuntimeError("KITARU_API_URL is not set and no server is recorded")
        os.environ["KITARU_API_URL"] = state["base_url"]
    stop_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, stop_event.set)
    loop.add_signal_handler(signal.SIGTERM, stop_event.set)
    print(
        f"Running {args.count} worker(s) with concurrency {args.concurrency} "
        f"against {os.environ['KITARU_API_URL']}, Ctrl-C to stop."
    )
    tasks = await run_workers(args.count, args.concurrency, stop_event)
    await asyncio.gather(*tasks)


def resolve_db_name() -> str:
    """Return the running stack's database name, or the default."""
    state = _read_state()
    if state is not None:
        return state["db_name"]
    return DEFAULT_DB_NAME


async def database_exists(name: str) -> bool:
    """Check whether a database exists on the local test Postgres."""
    conn = await _connect_admin()
    try:
        return bool(
            await conn.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", name)
        )
    finally:
        await conn.close()


def _exec_psql(name: str) -> None:
    """Replace the process with an interactive psql inside the db container."""
    os.execvp(
        "docker",
        ["docker", "exec", "-it", DB_CONTAINER, "psql", "-U", DB_USER, "-d", name],
    )


async def _db(args: argparse.Namespace) -> None:
    """Manage databases on the local test Postgres."""
    if args.db_command == "create":
        await create_database(args.name or DEFAULT_DB_NAME)
        print(f"Created database {args.name or DEFAULT_DB_NAME}.")
    elif args.db_command == "drop":
        if args.name is None:
            raise RuntimeError("db drop requires a database name")
        await drop_database(args.name)
        print(f"Dropped database {args.name}.")
    elif args.db_command == "snapshot":
        if args.name is None:
            raise RuntimeError("db snapshot requires a snapshot name")
        source = args.source or resolve_db_name()
        await snapshot_database(source, args.name)
        print(f"Snapshot {args.name} created from {source}.")
    elif args.db_command == "restore":
        if args.name is None:
            raise RuntimeError("db restore requires a snapshot name")
        target = args.target or resolve_db_name()
        await restore_database(args.name, target)
        print(f"Restored {target} from snapshot {args.name}.")
    elif args.db_command == "clean":
        keep = {resolve_db_name()}
        dropped = await clean_databases(args.prefix or ["kitaru_seed_"], keep)
        for name in dropped:
            print(f"Dropped {name}.")
        if not dropped:
            print("Nothing to drop.")
    elif args.db_command == "psql":
        _exec_psql(args.name or resolve_db_name())
    else:
        for name in await list_databases():
            print(name)


def main() -> int:
    """Run the stack CLI."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    up = commands.add_parser("up", help="Start Postgres and a local server.")
    up.add_argument("--port", type=int, default=DEFAULT_PORT, help="0 picks freely.")
    up.add_argument("--db-name", default=DEFAULT_DB_NAME)
    up.add_argument("--fresh", action="store_true", help="Drop the database first.")
    up.add_argument("--auth", choices=("none", "local"), default="none")
    up.add_argument(
        "--env",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Extra server environment variable, repeatable.",
    )
    up.add_argument(
        "--docker",
        action="store_true",
        help="Run server and database via docker compose instead.",
    )
    up.set_defaults(handler=_up)

    down = commands.add_parser("down", help="Stop the local server.")
    down.add_argument("--drop-db", action="store_true")
    down.add_argument("--docker", action="store_true")
    down.set_defaults(handler=_down)

    status = commands.add_parser("status", help="Report the local stack status.")
    status.set_defaults(handler=_status)

    workers = commands.add_parser("workers", help="Run in-process workers.")
    workers.add_argument("--count", type=int, default=1)
    workers.add_argument("--concurrency", type=int, default=8)
    workers.set_defaults(handler=_workers)

    db = commands.add_parser("db", help="Manage local test databases.")
    db.add_argument(
        "db_command",
        choices=("create", "drop", "list", "snapshot", "restore", "clean", "psql"),
    )
    db.add_argument("name", nargs="?", default=None)
    db.add_argument(
        "--source",
        default=None,
        help="Database a snapshot copies, the running stack's db when omitted.",
    )
    db.add_argument(
        "--target",
        default=None,
        help="Database a restore replaces, the running stack's db when omitted.",
    )
    db.add_argument(
        "--prefix",
        action="append",
        default=None,
        help="Prefix clean matches, repeatable, kitaru_seed_ when omitted.",
    )
    db.set_defaults(handler=_db)

    args = parser.parse_args()
    asyncio.run(args.handler(args))
    return 0


if __name__ == "__main__":
    sys.exit(main())
