"""Shared Docker CLI helpers used by the sandbox, proxy, and mock runner.

The three lifecycle managers (`DockerSandbox`, `DockerProxy`,
`DockerMockServices`) all need the same shape of `docker image inspect`,
`docker network create`, and `docker stop` plumbing. The helpers below
collapse those duplicate implementations into one place. Per-class
differences (image-specific build hints, container-specific names) are
passed in as arguments rather than baked into the helper.
"""

import subprocess

from kitaru.errors import KitaruRuntimeError


def ensure_image(image_name: str, build_hint: str) -> None:
    """Verify a Docker image exists locally; raise with a build hint if not.

    Args:
        image_name: The tag to look up (e.g. ``agent-harness-platform-sandbox``).
        build_hint: A multi-line message describing how to build the
            image. Surfaced as the body of the raised error so the user
            sees a copy-pasteable command.
    """
    result = subprocess.run(
        ["docker", "image", "inspect", image_name],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        return
    raise KitaruRuntimeError(
        f"Docker image {image_name!r} is not built locally.\n\n{build_hint}"
    )


def ensure_network(network_name: str) -> None:
    """Ensure a Docker network exists; race-safe under concurrent flow runs.

    Two concurrent flows may both inspect, both miss, and both attempt
    create. The loser's create fails harmlessly; we re-inspect after
    create to confirm the network is up regardless of who won.
    """
    result = subprocess.run(
        ["docker", "network", "inspect", network_name],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        return
    subprocess.run(
        ["docker", "network", "create", network_name],
        capture_output=True,
    )
    confirm = subprocess.run(
        ["docker", "network", "inspect", network_name],
        capture_output=True,
        text=True,
    )
    if confirm.returncode != 0:
        raise KitaruRuntimeError(
            f"failed to create or find docker network {network_name!r}"
        )


def stop_container(container_name: str, *, timeout_seconds: int = 2) -> None:
    """Best-effort `docker stop` of a container by name.

    Swallows any error from the docker CLI — containers started with
    ``--rm`` are cleaned up by the daemon, and a missing container is
    not worth raising about during teardown.
    """
    subprocess.run(
        [
            "docker",
            "stop",
            "--time",
            str(timeout_seconds),
            container_name,
        ],
        capture_output=True,
        text=True,
    )
