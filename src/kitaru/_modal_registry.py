"""Shared registry helpers for Modal stack cloud connector validation."""

from __future__ import annotations

import re
from typing import Literal

ModalRegistryProvider = Literal["aws", "gcp", "azure"]

MODAL_NATIVE_REGISTRY_ERROR = (
    "Credentialed Modal stacks require a provider-native private registry: "
    "AWS ECR, Google Artifact Registry/GCR, or Azure Container Registry."
)

_ECR_HOST_PATTERN = re.compile(
    r"^\d+\.dkr\.ecr(?:-fips)?\.([a-z0-9-]+)\.amazonaws\.com(?:\.cn)?$"
)
_GCR_LOCATIONS = {
    "us.gcr.io": "us",
    "eu.gcr.io": "eu",
    "asia.gcr.io": "asia",
}


def container_registry_host(container_registry: str) -> str:
    """Return the host part from a container registry URI."""
    normalized_registry = re.sub(r"^[a-z]+://", "", container_registry.strip())
    normalized_registry = normalized_registry.rstrip("/")
    if not normalized_registry:
        raise ValueError("Container registry URI cannot be empty.")
    return normalized_registry.split("/", 1)[0]


def infer_modal_registry_provider(container_registry: str) -> ModalRegistryProvider:
    """Infer the cloud provider from a provider-native registry URI."""
    host = container_registry_host(container_registry)
    if _ECR_HOST_PATTERN.match(host):
        return "aws"
    if host in {"gcr.io", *_GCR_LOCATIONS} or host.endswith("-docker.pkg.dev"):
        return "gcp"
    if host.endswith(".azurecr.io"):
        return "azure"
    raise ValueError(f"{MODAL_NATIVE_REGISTRY_ERROR} Received '{container_registry}'.")


def ecr_region_from_registry(container_registry: str) -> str | None:
    """Return the AWS region encoded in an ECR registry host, if present."""
    match = _ECR_HOST_PATTERN.match(container_registry_host(container_registry))
    return match.group(1) if match else None


def gcp_location_from_registry(container_registry: str) -> str | None:
    """Return the GCP registry location encoded in GAR/GCR hosts, if present."""
    host = container_registry_host(container_registry)
    if host.endswith("-docker.pkg.dev"):
        return host.removesuffix("-docker.pkg.dev")
    return _GCR_LOCATIONS.get(host)
