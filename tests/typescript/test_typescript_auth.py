"""Cross-runtime proofs for read-only TypeScript CLI-login reuse."""

import os
import stat
import subprocess
import time
from pathlib import Path

import pytest

from kitaru.client.config import set_server_url
from kitaru.client.credential_store import CredentialStore
from kitaru.client.credentials import ApiToken

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module", autouse=True)
def build_typescript_core() -> None:
    """Build the Node entry consumed by the cross-runtime checks."""
    subprocess.run(
        ["pnpm", "--filter", "@zenml-io/kitaru", "build"],
        cwd=REPO_ROOT,
        check=True,
    )


def run_node(script: str, config_dir: Path) -> None:
    """Run a Node probe against a Python-produced config directory."""
    process_environment = os.environ.copy()
    process_environment["AUTH_CONFIG_DIR"] = str(config_dir)
    subprocess.run(
        ["node", "--input-type=module", "--eval", script],
        cwd=REPO_ROOT,
        env=process_environment,
        check=True,
    )


def test_typescript_reads_cli_credentials_read_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Consume Python serialization from compiled TypeScript without mutation."""
    server_url = "https://STORED.example/base/"
    monkeypatch.setenv("KITARU_CONFIG_DIR", str(tmp_path))
    store = CredentialStore(path=tmp_path / "credentials.json")
    store.set_api_key(server_url, "KITKEY_cross_runtime")
    set_server_url(server_url)
    credentials_path = tmp_path / "credentials.json"
    before = credentials_path.read_bytes()
    before_mtime = credentials_path.stat().st_mtime_ns

    script = """
import { createKitaruClient } from './packages/core/dist/node/index.js';
globalThis.fetch = async (input, init) => {
  if (String(input) !== 'https://stored.example/base/v1/replays/probe') {
    throw new Error(`unexpected URL ${input}`);
  }
  if (init.headers.Authorization !== 'Bearer KITKEY_cross_runtime') {
    throw new Error('stored API key was not used');
  }
  return new Response(JSON.stringify({ detail: 'expected probe' }), { status: 404 });
};
const client = await createKitaruClient({
  environment: { KITARU_CONFIG_DIR: process.env.AUTH_CONFIG_DIR },
});
try { await client.getReplay('probe'); } catch (error) {
  if (error.status !== 404) throw error;
}
"""
    run_node(script, tmp_path)

    assert credentials_path.read_bytes() == before
    assert credentials_path.stat().st_mtime_ns == before_mtime
    if os.name != "nt":
        assert stat.S_IMODE(credentials_path.stat().st_mode) == 0o600


def test_typescript_rejects_cli_remote_cleartext_url(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep Node's remote cleartext protection around Python-produced config."""
    server_url = "http://api.example"
    monkeypatch.setenv("KITARU_CONFIG_DIR", str(tmp_path))
    CredentialStore(path=tmp_path / "credentials.json").set_api_key(
        server_url, "KITKEY_cross_runtime"
    )
    set_server_url(server_url)

    run_node(
        """
import { createKitaruClient } from './packages/core/dist/node/index.js';
try {
  await createKitaruClient({
    environment: { KITARU_CONFIG_DIR: process.env.AUTH_CONFIG_DIR },
  });
  throw new Error('unsafe stored URL was accepted');
} catch (error) {
  if (!String(error).includes('config server_url is unsafe')) throw error;
}
""",
        tmp_path,
    )


def test_live_typescript_client_rejects_python_relogin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fail closed after Python rotates a token, then allow a fresh client."""
    server_url = "https://stored.example"
    monkeypatch.setenv("KITARU_CONFIG_DIR", str(tmp_path))
    store = CredentialStore(path=tmp_path / "credentials.json")
    store.set_token(server_url, ApiToken(access_token="before-login"))
    set_server_url(server_url)
    ready = tmp_path / "request-ready"
    resume = tmp_path / "resume-request"
    script = """
import { writeFile, stat } from 'node:fs/promises';
import { createKitaruClient } from './packages/core/dist/node/index.js';

const waitFor = async (path) => {
  for (let attempt = 0; attempt < 500; attempt += 1) {
    try { await stat(path); return; } catch {}
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`timed out waiting for ${path}`);
};

let request = 0;
globalThis.fetch = async (_input, init) => {
  request += 1;
  if (request === 1) {
    if (init.headers.Authorization !== 'Bearer before-login') {
      throw new Error('initial stored token was not used');
    }
    await writeFile(process.env.READY_PATH, 'ready');
    await waitFor(process.env.RESUME_PATH);
    return new Response(JSON.stringify({ detail: 'expired' }), { status: 401 });
  }
  if (init.headers.Authorization !== 'Bearer after-login') {
    throw new Error('fresh client did not use the replacement token');
  }
  return new Response(JSON.stringify({ detail: 'expected probe' }), { status: 404 });
};

const options = {
  environment: { KITARU_CONFIG_DIR: process.env.AUTH_CONFIG_DIR },
};
const liveClient = await createKitaruClient(options);
try {
  await liveClient.getReplay('probe');
  throw new Error('live client silently adopted a replacement login');
} catch (error) {
  if (!String(error).includes('identity changed; create a new client')) throw error;
}
const freshClient = await createKitaruClient(options);
try { await freshClient.getReplay('probe'); } catch (error) {
  if (error.status !== 404) throw error;
}
"""
    environment = os.environ.copy()
    environment.update(
        {
            "AUTH_CONFIG_DIR": str(tmp_path),
            "READY_PATH": str(ready),
            "RESUME_PATH": str(resume),
        }
    )
    process = subprocess.Popen(
        ["node", "--input-type=module", "--eval", script],
        cwd=REPO_ROOT,
        env=environment,
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    deadline = time.monotonic() + 10
    while not ready.exists() and process.poll() is None:
        if time.monotonic() >= deadline:
            process.kill()
            raise AssertionError("Node probe did not reach its first request")
        time.sleep(0.01)

    store.set_token(server_url, ApiToken(access_token="after-login"))
    resume.write_text("resume", encoding="utf-8")
    stdout, stderr = process.communicate(timeout=10)
    assert process.returncode == 0, f"Node probe failed:\n{stdout}\n{stderr}"
