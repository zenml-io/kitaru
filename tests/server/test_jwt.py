#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Tests for server-issued JWTs."""

import uuid
from datetime import UTC, datetime, timedelta

import jwt as pyjwt
import pytest

from conftest import local_settings
from kitaru.server.adapters.auth.jwt import (
    DEFAULT_JWT_ALGORITHM,
    AccountSubject,
    JWTToken,
    TaskSubject,
    TokenError,
    WorkerSubject,
)
from kitaru.server.application.models.auth import GrantKind
from kitaru.server.domain.account import Account


def _raw_claims(settings, **claims: object) -> str:
    """Encode arbitrary claims directly, bypassing JWTToken's own shape.

    Args:
        settings: Runtime settings supplying the issuer, audience, and key.
        **claims: Claims to encode, merged over the required iss/aud.

    Returns:
        Encoded token.
    """
    payload: dict[str, object] = {
        "iss": settings.JWT_ISSUER,
        "aud": settings.JWT_AUDIENCE,
        **claims,
    }
    return pyjwt.encode(
        payload=payload, key=settings.JWT_SIGNING_KEY, algorithm=DEFAULT_JWT_ALGORITHM
    )


def test_account_token_round_trips() -> None:
    """Encoding then decoding an account token recovers its claims."""
    settings = local_settings()
    account = Account(id=uuid.uuid4(), name="ann")
    expires_at = datetime.now(UTC) + timedelta(hours=1)
    token = JWTToken(
        subject=AccountSubject(account_id=account.id, csrf_token="csrf"),
        expires_at=expires_at,
    )

    decoded = JWTToken.decode(token.encode(settings), settings)

    assert isinstance(decoded.subject, AccountSubject)
    assert decoded.subject.account_id == account.id
    assert decoded.subject.csrf_token == "csrf"
    assert decoded.expires_at == expires_at.replace(microsecond=0)


def test_worker_token_round_trips() -> None:
    """Encoding then decoding a worker token recovers its claims."""
    settings = local_settings()
    worker_id = uuid.uuid4()
    account_id = uuid.uuid4()
    expires_at = datetime.now(UTC) + timedelta(hours=1)
    token = JWTToken(
        subject=WorkerSubject(worker_id=worker_id, account_id=account_id),
        expires_at=expires_at,
    )

    decoded = JWTToken.decode(token.encode(settings), settings)

    assert isinstance(decoded.subject, WorkerSubject)
    assert decoded.subject.worker_id == worker_id
    assert decoded.subject.account_id == account_id
    assert decoded.expires_at == expires_at.replace(microsecond=0)


def test_task_token_round_trips() -> None:
    """Encoding then decoding a task token recovers its claims."""
    settings = local_settings()
    task_id = uuid.uuid4()
    worker_id = uuid.uuid4()
    account_id = uuid.uuid4()
    job_id = uuid.uuid4()
    expires_at = datetime.now(UTC) + timedelta(hours=1)
    token = JWTToken(
        subject=TaskSubject(
            task_id=task_id,
            attempt=3,
            worker_id=worker_id,
            account_id=account_id,
            job_id=job_id,
        ),
        expires_at=expires_at,
    )

    decoded = JWTToken.decode(token.encode(settings), settings)

    assert isinstance(decoded.subject, TaskSubject)
    assert decoded.subject.task_id == task_id
    assert decoded.subject.attempt == 3
    assert decoded.subject.worker_id == worker_id
    assert decoded.subject.account_id == account_id
    assert decoded.subject.job_id == job_id
    assert decoded.subject.grants == {}
    assert decoded.expires_at == expires_at.replace(microsecond=0)


def test_task_token_round_trips_grants() -> None:
    """Encoding then decoding a task token recovers its granted resource ids."""
    settings = local_settings()
    session_id = uuid.uuid4()
    blob_id = uuid.uuid4()
    expires_at = datetime.now(UTC) + timedelta(hours=1)
    token = JWTToken(
        subject=TaskSubject(
            task_id=uuid.uuid4(),
            attempt=1,
            worker_id=uuid.uuid4(),
            account_id=uuid.uuid4(),
            job_id=uuid.uuid4(),
            grants={
                GrantKind.SESSION: frozenset({session_id}),
                GrantKind.BLOB: frozenset({blob_id}),
            },
        ),
        expires_at=expires_at,
    )

    decoded = JWTToken.decode(token.encode(settings), settings)

    assert isinstance(decoded.subject, TaskSubject)
    assert decoded.subject.grants == {
        GrantKind.SESSION: frozenset({session_id}),
        GrantKind.BLOB: frozenset({blob_id}),
    }


def test_decode_rejects_an_unrecognized_subject_kind() -> None:
    """Reject a subject whose kind this server does not know."""
    settings = local_settings()
    token = _raw_claims(
        settings,
        sub=f"agent:{uuid.uuid4()}",
        exp=int((datetime.now(UTC) + timedelta(hours=1)).timestamp()),
    )
    with pytest.raises(TokenError, match="subject kind 'agent' is not recognized"):
        JWTToken.decode(token, settings)


def test_decode_rejects_a_missing_subject() -> None:
    """Reject a token with no subject claim at all."""
    settings = local_settings()
    token = _raw_claims(
        settings, exp=int((datetime.now(UTC) + timedelta(hours=1)).timestamp())
    )
    with pytest.raises(TokenError, match='missing the "sub" claim'):
        JWTToken.decode(token, settings)


def test_decode_rejects_a_worker_token_missing_account_id() -> None:
    """Reject a worker-subject token whose account_id claim is missing."""
    settings = local_settings()
    token = _raw_claims(
        settings,
        sub=f"worker:{uuid.uuid4()}",
        exp=int((datetime.now(UTC) + timedelta(hours=1)).timestamp()),
    )
    with pytest.raises(TokenError, match="Invalid session token claims"):
        JWTToken.decode(token, settings)


def test_decode_rejects_a_task_token_missing_attempt() -> None:
    """Reject a task-subject token whose attempt claim is missing."""
    settings = local_settings()
    token = _raw_claims(
        settings,
        sub=f"task:{uuid.uuid4()}",
        account_id=str(uuid.uuid4()),
        worker_id=str(uuid.uuid4()),
        exp=int((datetime.now(UTC) + timedelta(hours=1)).timestamp()),
    )
    with pytest.raises(TokenError, match="Invalid session token claims"):
        JWTToken.decode(token, settings)


def test_decode_rejects_a_task_token_with_a_non_integer_attempt() -> None:
    """Reject a task-subject token whose attempt claim is not an integer."""
    settings = local_settings()
    token = _raw_claims(
        settings,
        sub=f"task:{uuid.uuid4()}",
        account_id=str(uuid.uuid4()),
        worker_id=str(uuid.uuid4()),
        attempt="not-an-int",
        exp=int((datetime.now(UTC) + timedelta(hours=1)).timestamp()),
    )
    with pytest.raises(TokenError, match="attempt claim is not an integer"):
        JWTToken.decode(token, settings)


def test_decode_rejects_a_task_token_with_an_invalid_grant_id() -> None:
    """Reject a task-subject token whose grants claim holds a non-UUID id."""
    settings = local_settings()
    token = _raw_claims(
        settings,
        sub=f"task:{uuid.uuid4()}",
        account_id=str(uuid.uuid4()),
        worker_id=str(uuid.uuid4()),
        job_id=str(uuid.uuid4()),
        attempt=1,
        grants={"session": ["not-a-uuid"]},
        exp=int((datetime.now(UTC) + timedelta(hours=1)).timestamp()),
    )
    with pytest.raises(TokenError, match="Invalid session token claims"):
        JWTToken.decode(token, settings)


def test_decode_rejects_a_task_token_missing_worker_id() -> None:
    """Reject a task-subject token whose worker_id claim is missing."""
    settings = local_settings()
    token = _raw_claims(
        settings,
        sub=f"task:{uuid.uuid4()}",
        account_id=str(uuid.uuid4()),
        attempt=1,
        exp=int((datetime.now(UTC) + timedelta(hours=1)).timestamp()),
    )
    with pytest.raises(TokenError, match="Invalid session token claims"):
        JWTToken.decode(token, settings)


def test_decode_rejects_an_expired_token() -> None:
    """Reject a token whose exp claim has already passed."""
    settings = local_settings()
    token = JWTToken(
        subject=WorkerSubject(worker_id=uuid.uuid4(), account_id=uuid.uuid4()),
        expires_at=datetime.now(UTC) - timedelta(minutes=1),
    )
    with pytest.raises(TokenError, match="Invalid session token"):
        JWTToken.decode(token.encode(settings), settings)


def test_decode_rejects_a_token_signed_with_a_different_key() -> None:
    """Reject a token whose signature does not match this server's key."""
    settings = local_settings()
    other_settings = local_settings(
        JWT_SIGNING_KEY="a-different-signing-key-0123456789"
    )
    token = JWTToken(
        subject=WorkerSubject(worker_id=uuid.uuid4(), account_id=uuid.uuid4()),
        expires_at=datetime.now(UTC) + timedelta(hours=1),
    )
    with pytest.raises(TokenError, match="Invalid session token"):
        JWTToken.decode(token.encode(other_settings), settings)
