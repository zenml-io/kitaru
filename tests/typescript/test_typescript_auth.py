"""Cross-runtime proof for read-only TypeScript CLI-login reuse."""

import json
import os
import stat
import subprocess
from pathlib import Path

from kitaru.client.credential_store import CredentialStore


def test_typescript_reads_cli_credentials_read_only(tmp_path: Path) -> None:
    """Consume Python serialization from compiled TypeScript without mutation."""
    repo_root = Path(__file__).resolve().parents[2]
    server_url = "https://stored.example/base"
    store = CredentialStore(path=tmp_path / "credentials.json")
    store.set_api_key(server_url, "KITKEY_cross_runtime")
    (tmp_path / "config.json").write_text(
        json.dumps({"server_url": server_url}), encoding="utf-8"
    )
    credentials_path = tmp_path / "credentials.json"
    before = credentials_path.read_bytes()
    before_mtime = credentials_path.stat().st_mtime_ns

    subprocess.run(
        ["pnpm", "--filter", "@zenml-io/kitaru", "build"],
        cwd=repo_root,
        check=True,
    )
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
    environment = os.environ.copy()
    environment["AUTH_CONFIG_DIR"] = str(tmp_path)
    subprocess.run(
        ["node", "--input-type=module", "--eval", script],
        cwd=repo_root,
        env=environment,
        check=True,
    )

    assert credentials_path.read_bytes() == before
    assert credentials_path.stat().st_mtime_ns == before_mtime
    if os.name != "nt":
        assert stat.S_IMODE(credentials_path.stat().st_mode) == 0o600
