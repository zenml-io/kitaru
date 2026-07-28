#!/usr/bin/env python3
"""Check that Alembic migrations match the ORM schema."""

import asyncio
import difflib
import os
import pprint
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from alembic.autogenerate import compare_metadata  # noqa: E402
from alembic.runtime.migration import MigrationContext  # noqa: E402
from sqlalchemy import inspect  # noqa: E402
from sqlalchemy.engine import Connection  # noqa: E402
from sqlalchemy.ext.asyncio import create_async_engine  # noqa: E402
from sqlalchemy.sql import text  # noqa: E402

from kitaru.server.adapters.db.orm.base import Base  # noqa: E402
from kitaru.server.config import Settings  # noqa: E402
from kitaru.server.database import DatabaseService  # noqa: E402
from kitaru.server.database.migrations.alembic import (  # noqa: E402
    Alembic,
    include_object,
)

ORM_DB = "kitaru_migration_check_orm"
ALEMBIC_DB = "kitaru_migration_check_alembic"


def check_settings(db_name: str) -> Settings:
    """Build settings pointing at the local check database."""
    return Settings(
        DB_HOST=os.environ.get("KITARU_TEST_DB_HOST", "localhost"),
        DB_PORT=int(os.environ.get("KITARU_TEST_DB_PORT", "5433")),
        DB_NAME=db_name,
        DATABASE_URL=None,
    )


def check_linear_history(alembic: Alembic) -> list[str]:
    """Return failures when the migration history is not one linear chain."""
    failures: list[str] = []
    heads = alembic.script_directory.get_heads()
    if len(heads) != 1:
        failures.append(f"Expected exactly one head revision, found {sorted(heads)}.")
    down_revisions: dict[str | None, str] = {}
    for script in alembic.script_directory.walk_revisions():
        down = script.down_revision
        if down is not None and not isinstance(down, str):
            failures.append(f"Revision {script.revision} merges revisions {down}.")
            continue
        if down in down_revisions:
            failures.append(
                f"Revisions {script.revision} and {down_revisions[down]} both "
                f"branch off {down or 'base'}."
            )
        down_revisions[down] = script.revision
    return failures


def snapshot_schema(conn: Connection) -> dict[str, Any]:
    """Return a comparable snapshot of the connected database schema."""
    inspector = inspect(conn)
    schema: dict[str, Any] = {}
    for table in sorted(inspector.get_table_names()):
        if table == "alembic_version":
            continue
        primary_key = inspector.get_pk_constraint(table)
        schema[table] = {
            "columns": {
                column["name"]: {
                    "type": str(column["type"]),
                    "nullable": column["nullable"],
                    "default": column["default"],
                }
                for column in inspector.get_columns(table)
            },
            "primary_key": (
                primary_key["name"],
                tuple(primary_key["constrained_columns"]),
            ),
            "foreign_keys": sorted(
                (
                    fk["name"],
                    tuple(fk["constrained_columns"]),
                    fk["referred_table"],
                    tuple(fk["referred_columns"]),
                    fk.get("options", {}).get("ondelete"),
                )
                for fk in inspector.get_foreign_keys(table)
            ),
            "indexes": sorted(
                (index["name"], tuple(index["column_names"]), index["unique"])
                for index in inspector.get_indexes(table)
            ),
            "unique_constraints": sorted(
                (constraint["name"], tuple(constraint["column_names"]))
                for constraint in inspector.get_unique_constraints(table)
            ),
            "check_constraints": sorted(
                (constraint["name"], constraint["sqltext"])
                for constraint in inspector.get_check_constraints(table)
            ),
        }
    return schema


def autogenerate_diffs(conn: Connection) -> list[Any]:
    """Return schema operations autogenerate would emit on the connection."""
    context = MigrationContext.configure(
        conn,
        opts={"compare_type": True, "include_object": include_object},
    )
    return compare_metadata(context, Base.metadata)


async def drop_db(settings: Settings) -> None:
    """Drop the check database."""
    database_name = DatabaseService.application_database_name(settings)
    engine = create_async_engine(
        DatabaseService.generate_database_uri(settings, use_default_db=True)
    )
    try:
        async with engine.execution_options(
            isolation_level="AUTOCOMMIT"
        ).begin() as conn:
            await conn.execute(
                text(f'DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)')
            )
    finally:
        await engine.dispose()


async def main() -> int:
    """Run all migration checks and return the process exit code."""
    orm_settings = check_settings(ORM_DB)
    alembic_settings = check_settings(ALEMBIC_DB)

    try:
        await DatabaseService.create_db(orm_settings, force_drop=True)
    except OSError as error:
        print(f"Cannot reach PostgreSQL: {error}")
        print("Start the local database with: docker compose up -d db")
        return 1
    await DatabaseService.create_db(alembic_settings, force_drop=True)

    failures: list[str] = []
    orm_db = DatabaseService(orm_settings)
    alembic_db = DatabaseService(alembic_settings)
    try:
        alembic = Alembic(alembic_db.engine)
        failures += check_linear_history(alembic)

        async with orm_db.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        await alembic.upgrade()

        async with orm_db.engine.connect() as conn:
            orm_schema = await conn.run_sync(snapshot_schema)
        async with alembic_db.engine.connect() as conn:
            alembic_schema = await conn.run_sync(snapshot_schema)
            diffs = await conn.run_sync(autogenerate_diffs)

        if orm_schema != alembic_schema:
            schema_diff = "\n".join(
                difflib.unified_diff(
                    pprint.pformat(alembic_schema).splitlines(),
                    pprint.pformat(orm_schema).splitlines(),
                    fromfile="alembic upgrade",
                    tofile="orm create_all",
                    lineterm="",
                )
            )
            failures.append(
                f"Migrated and freshly created schemas differ:\n{schema_diff}"
            )
        if diffs:
            failures.append(
                "Autogenerate is not empty against the migrated database:\n"
                + pprint.pformat(diffs)
            )
    finally:
        await orm_db.cleanup()
        await alembic_db.cleanup()
        await drop_db(orm_settings)
        await drop_db(alembic_settings)

    if failures:
        for failure in failures:
            print(f"FAILED: {failure}\n")
        return 1
    print("Migration checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
