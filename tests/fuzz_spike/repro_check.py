"""Re-run minimal repros for the distinct fuzz findings with plain httpx.

Run from the repo root:
    uv run python tests/fuzz_spike/repro_check.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))
sys.path.insert(0, str(Path(__file__).parent))

import conftest  # noqa: F401  (registers settings/env, sys.modules alias)
import httpx
from fuzz_harness import FuzzServer


def main() -> None:
    server = FuzzServer()
    server.start()
    client = httpx.Client(
        base_url=server.base_url, headers=server.auth_headers, timeout=30
    )
    try:
        NUL = chr(0)
        repros = [
            ("F1 cursor", "GET", "/api/v1/accounts", {"params": {"cursor": ""}}),
            (
                "F1 sort",
                "GET",
                "/api/v1/jobs",
                {"params": {"sort": "j:asc"}},
            ),
            (
                "F1 empty name",
                "POST",
                "/api/v1/agents",
                {"json": {"name": "", "description": None}},
            ),
            (
                "F2 int32 overflow GET",
                "GET",
                "/api/v1/evaluators/41813831-a5f0-4833-8447-4a69fbff83e9/versions/681812836839",
                {},
            ),
            (
                "F2 int32 overflow PATCH",
                "PATCH",
                "/api/v1/importers/7b874ee6-b680-1161-aaeb-5869bea53829/versions/2812498722621",
                {"json": {"display_version": None}},
            ),
            (
                "F3 login empty username",
                "POST",
                "/api/v1/login",
                {
                    "data": {
                        "device_id": "e3e70682-c209-1cac-a29f-6fbed82c07cd",
                        "username": "",
                    }
                },
            ),
            (
                "F4 NUL in device_authorization form field",
                "POST",
                "/api/v1/device_authorization",
                {"data": {"python_version": "3.14\x00"}},
            ),
            (
                "F4 NUL in importer name",
                "POST",
                "/api/v1/imports",
                {
                    "json": {
                        "importer": "a" + NUL + "b",
                        "agent_id": "e3e70682-c209-1cac-a29f-6fbed82c07cd",
                        "payload_blob_id": "e3e70682-c209-1cac-a29f-6fbed82c07cd",
                        "params": {},
                    }
                },
            ),
        ]
        for label, method, path, kwargs in repros:
            response = client.request(method, path, **kwargs)
            print(f"{label}: {method} {path} -> {response.status_code}")
            print(f"    body: {response.text[:200]}")
        print(f"\ncaptured unhandled exceptions: {len(server.capture.records)}")
        for record in server.capture.records:
            print(
                f"  {record['method']} {record['path']} "
                f"{record['exc_type']}: {record['exc_msg'][:160]}"
            )
    finally:
        client.close()
        server.stop()


if __name__ == "__main__":
    main()
