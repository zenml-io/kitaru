#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Strict connection read and management inputs."""

import uuid
from datetime import datetime
from typing import Annotated, Literal, Self

from pydantic import (
    AfterValidator,
    Field,
    SecretStr,
    TypeAdapter,
    ValidationInfo,
    ValidatorFunctionWrapHandler,
    WrapValidator,
    field_validator,
    model_validator,
)

from kitaru.api_models.v1.base import PlainSerializedSecretStr
from kitaru.mcp.models.common import IDEMPOTENCY_KEY_DESCRIPTION, MCPModel, PageOptions
from kitaru.mcp.redaction import MASKED, is_credential_name


class ConnectionListRequest(PageOptions):
    """List one page of connections, optionally for one provider."""

    operation: Literal["list"]
    provider: str | None = None


class ConnectionGetRequest(MCPModel):
    """Get a connection by UUID or exact case-sensitive name."""

    operation: Literal["get"]
    reference: str = Field(min_length=1)


def _refuse_credential_name(name: str) -> str:
    # Check keys, not the map: a refused map would echo its values, which may
    # be the very credential, in the validation error.
    if is_credential_name(name):
        raise ValueError(
            "names a credential; move it to `secrets`, which are stored "
            "encrypted and never returned"
        )
    return name


PlainEnv = Annotated[
    dict[Annotated[str, AfterValidator(_refuse_credential_name)], str],
    Field(
        description="Plain settings, stored unencrypted and readable by anyone "
        "who can read the connection. Keys that name credentials are refused: "
        "put those in `secrets`."
    ),
]


def _refuse_keys(keys: list[str], message: str) -> None:
    # Fail on each key alone: an error on the map would echo its values, which
    # may be the very credential.
    def refuse(_name: str) -> str:
        raise ValueError(message)

    TypeAdapter(dict[Annotated[str, AfterValidator(refuse)], None]).validate_python(
        dict.fromkeys(keys)
    )


def _check_update_env(
    value: object, handler: ValidatorFunctionWrapHandler
) -> dict[str, str]:
    env = handler(value)
    sent = [
        key for key, item in env.items() if is_credential_name(key) and item != MASKED
    ]
    if sent:
        _refuse_keys(
            sent,
            f"names a credential, so `env` takes it only as `{MASKED}`, which "
            "keeps the value the connection stores; leaving it out of `env` "
            "deletes that value. Put a new credential in `secrets`, which on "
            "update replaces every stored secret.",
        )
    return env


def _check_update_secrets(
    value: object, handler: ValidatorFunctionWrapHandler
) -> dict[str, SecretStr]:
    secrets = handler(value)
    masked = [key for key, item in secrets.items() if item.get_secret_value() == MASKED]
    if masked:
        _refuse_keys(
            masked,
            f"is `{MASKED}`, but a stored secret cannot be kept that way: "
            "`secrets` replaces every stored secret, so send every value.",
        )
    return secrets


UpdateEnv = Annotated[dict[str, str], WrapValidator(_check_update_env)]
UpdateSecrets = Annotated[
    dict[str, PlainSerializedSecretStr], WrapValidator(_check_update_secrets)
]


ConnectionReadRequest = Annotated[
    ConnectionListRequest | ConnectionGetRequest,
    Field(discriminator="operation"),
]


class ConnectionCreate(MCPModel):
    """Create a connection carrying provider environment and secret values."""

    operation: Literal["create"]
    name: str = Field(min_length=1)
    provider: str = Field(min_length=1)
    env: PlainEnv = Field(default_factory=dict)
    secrets: dict[str, PlainSerializedSecretStr] = Field(default_factory=dict)
    default: bool = False
    idempotency_key: str | None = Field(
        default=None,
        description=IDEMPOTENCY_KEY_DESCRIPTION,
    )


class ConnectionUpdate(MCPModel):
    """Replace environment values, secret values, or the default flag."""

    operation: Literal["update"]
    connection_id: uuid.UUID
    env: UpdateEnv | None = Field(
        default=None,
        description="Replaces every plain setting. A value sent exactly as "
        f"`kitaru_connection_read` shows it, such as `{MASKED}` or "
        f"`Bearer {MASKED}`, keeps the value the connection stores; leaving a key "
        "out deletes it. A key that names a credential is accepted only as "
        f"`{MASKED}`: put new credentials in `secrets`.",
    )
    secrets: UpdateSecrets | None = Field(
        default=None,
        description="Replaces every stored secret. Stored values cannot be read "
        "back, so send every secret the connection should keep, not only new "
        "ones, and name each stored secret to delete in `remove_secrets`; an "
        "update that would drop a stored secret otherwise is refused.",
    )
    remove_secrets: list[str] | None = Field(
        default=None,
        description="Names of stored secrets this update deletes on purpose; "
        "goes with `secrets`. `secrets: {}` with every stored name here deletes "
        "them all.",
    )
    default: bool | None = None
    expected_updated: datetime | None = Field(
        default=None,
        validate_default=True,
        description="The connection's `updated` time as `kitaru_connection_read` "
        "shows it; required with `env` or `secrets`, and checked on any update "
        "that sends it. The update is refused if `env` or `default` changed "
        "after that read. A change to secrets alone does not move `updated`, so "
        "it is not detected.",
    )

    @field_validator("expected_updated")
    @classmethod
    def _require_expected_updated(
        cls, value: datetime | None, info: ValidationInfo
    ) -> datetime | None:
        # A field error, not a model one: the model's error would echo every
        # input, secret values included.
        if value is None and any(
            info.data.get(field) is not None for field in ("env", "secrets")
        ):
            raise ValueError(
                "an update of `env` or `secrets` needs the connection's `updated` "
                "time from `kitaru_connection_read`"
            )
        return value

    @model_validator(mode="after")
    def _validate_update(self) -> Self:
        for field in ("env", "secrets", "default"):
            if field in self.model_fields_set and getattr(self, field) is None:
                raise ValueError(f"{field} cannot be null")
        if not {"env", "secrets", "default"} & self.model_fields_set:
            raise ValueError("connection update must change at least one field")
        removed = self.remove_secrets or []
        if removed and self.secrets is None:
            _refuse_keys(
                removed,
                "is listed in `remove_secrets`, which goes with `secrets`: send "
                "every stored secret to keep in `secrets`.",
            )
        if both := sorted(set(removed) & set(self.secrets or {})):
            _refuse_keys(
                both, "is both sent in `secrets` and listed in `remove_secrets`."
            )
        return self


class ConnectionSetDefault(MCPModel):
    """Make one connection the default for its provider."""

    operation: Literal["set_default"]
    connection_id: uuid.UUID


ConnectionsManageRequest = Annotated[
    ConnectionCreate | ConnectionUpdate | ConnectionSetDefault,
    Field(discriminator="operation"),
]
