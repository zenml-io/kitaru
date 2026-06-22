"""Local SQLite state for the reference-agent example."""

import json
import sqlite3
from pathlib import Path
from typing import Any

from .config import FIXTURES_DIR

DEFAULT_DB_PATH = FIXTURES_DIR / "reference_agent.sqlite"
DEFAULT_SEED_PATH = FIXTURES_DIR / "seed_data.json"


def connect(db_path: Path = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """Open a SQLite connection with row dictionaries enabled."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    return connection


def load_seed(seed_path: Path = DEFAULT_SEED_PATH) -> dict[str, Any]:
    """Load deterministic local seed data."""
    with seed_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Expected object seed data in {seed_path}")
    return data


def reset_database(
    db_path: Path = DEFAULT_DB_PATH,
    seed_path: Path = DEFAULT_SEED_PATH,
) -> None:
    """Recreate local tables and insert deterministic seed rows."""
    seed = load_seed(seed_path)
    with connect(db_path) as connection:
        cursor = connection.cursor()
        cursor.executescript(
            """
            DROP TABLE IF EXISTS customers;
            DROP TABLE IF EXISTS customer_settings;
            DROP TABLE IF EXISTS tickets;
            DROP TABLE IF EXISTS audit_log;

            CREATE TABLE customers (
                customer_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                plan TEXT NOT NULL,
                account_tier TEXT NOT NULL,
                owner_email TEXT NOT NULL,
                permission_role TEXT NOT NULL
            );

            CREATE TABLE customer_settings (
                customer_id TEXT NOT NULL,
                setting TEXT NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY (customer_id, setting),
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
            );

            CREATE TABLE tickets (
                ticket_id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id TEXT NOT NULL,
                summary TEXT NOT NULL,
                priority TEXT NOT NULL,
                status TEXT NOT NULL,
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
            );

            CREATE TABLE audit_log (
                audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                tool_name TEXT NOT NULL,
                customer_id TEXT NOT NULL,
                action TEXT NOT NULL,
                details_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        cursor.executemany(
            """
            INSERT INTO customers (
                customer_id, name, plan, account_tier, owner_email, permission_role
            )
            VALUES (
                :customer_id, :name, :plan, :account_tier, :owner_email,
                :permission_role
            )
            """,
            seed["customers"],
        )
        settings_rows = [
            {
                "customer_id": customer_id,
                "setting": setting,
                "value": str(value),
            }
            for customer_id, settings in seed["customer_settings"].items()
            for setting, value in settings.items()
        ]
        cursor.executemany(
            """
            INSERT INTO customer_settings (customer_id, setting, value)
            VALUES (:customer_id, :setting, :value)
            """,
            settings_rows,
        )
        connection.commit()


def lookup_customer(
    email_or_id: str, db_path: Path = DEFAULT_DB_PATH
) -> dict[str, Any]:
    """Find one customer by id, name fragment, or owner email."""
    with connect(db_path) as connection:
        row = connection.execute(
            """
            SELECT * FROM customers
            WHERE customer_id = ?
               OR lower(owner_email) = lower(?)
               OR lower(name) LIKE lower(?)
            LIMIT 1
            """,
            (email_or_id, email_or_id, f"%{email_or_id}%"),
        ).fetchone()
        if row is None:
            return {"found": False, "query": email_or_id}
        settings = connection.execute(
            """
            SELECT setting, value FROM customer_settings
            WHERE customer_id = ?
            ORDER BY setting
            """,
            (row["customer_id"],),
        ).fetchall()
    result = dict(row)
    result["found"] = True
    result["settings"] = {setting["setting"]: setting["value"] for setting in settings}
    return result


def create_support_ticket(
    customer_id: str,
    summary: str,
    priority: str,
    db_path: Path = DEFAULT_DB_PATH,
) -> dict[str, Any]:
    """Create a ticket and record an audit row."""
    with connect(db_path) as connection:
        cursor = connection.execute(
            """
            INSERT INTO tickets (customer_id, summary, priority, status)
            VALUES (?, ?, ?, 'open')
            """,
            (customer_id, summary, priority),
        )
        ticket_id = int(cursor.lastrowid)
        _insert_audit_row(
            connection,
            tool_name="create_support_ticket",
            customer_id=customer_id,
            action="ticket_created",
            details={"ticket_id": ticket_id, "summary": summary, "priority": priority},
        )
        connection.commit()
    return {
        "ticket_id": ticket_id,
        "customer_id": customer_id,
        "summary": summary,
        "priority": priority,
        "status": "open",
    }


def escalate_to_human(
    customer_id: str,
    reason: str,
    db_path: Path = DEFAULT_DB_PATH,
) -> dict[str, Any]:
    """Record a safe human escalation in the audit log."""
    with connect(db_path) as connection:
        _insert_audit_row(
            connection,
            tool_name="escalate_to_human",
            customer_id=customer_id,
            action="human_escalation_recorded",
            details={"reason": reason},
        )
        connection.commit()
    return {"customer_id": customer_id, "escalated": True, "reason": reason}


def update_customer_setting(
    customer_id: str,
    setting: str,
    value: str,
    db_path: Path = DEFAULT_DB_PATH,
) -> dict[str, Any]:
    """Dangerous write tool used to make permission regressions visible."""
    with connect(db_path) as connection:
        connection.execute(
            """
            INSERT INTO customer_settings (customer_id, setting, value)
            VALUES (?, ?, ?)
            ON CONFLICT(customer_id, setting) DO UPDATE SET value = excluded.value
            """,
            (customer_id, setting, value),
        )
        _insert_audit_row(
            connection,
            tool_name="update_customer_setting",
            customer_id=customer_id,
            action="customer_setting_updated",
            details={"setting": setting, "value": value},
        )
        connection.commit()
    return {"customer_id": customer_id, "setting": setting, "value": value}


def get_audit_log(db_path: Path = DEFAULT_DB_PATH) -> list[dict[str, Any]]:
    """Return audit rows in insertion order."""
    with connect(db_path) as connection:
        rows = connection.execute(
            "SELECT * FROM audit_log ORDER BY audit_id"
        ).fetchall()
    return [_row_with_json_details(row) for row in rows]


def get_tickets(db_path: Path = DEFAULT_DB_PATH) -> list[dict[str, Any]]:
    """Return support tickets in insertion order."""
    with connect(db_path) as connection:
        rows = connection.execute("SELECT * FROM tickets ORDER BY ticket_id").fetchall()
    return [dict(row) for row in rows]


def _insert_audit_row(
    connection: sqlite3.Connection,
    *,
    tool_name: str,
    customer_id: str,
    action: str,
    details: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT INTO audit_log (tool_name, customer_id, action, details_json)
        VALUES (?, ?, ?, ?)
        """,
        (tool_name, customer_id, action, json.dumps(details, sort_keys=True)),
    )


def _row_with_json_details(row: sqlite3.Row) -> dict[str, Any]:
    result = dict(row)
    result["details"] = json.loads(result.pop("details_json"))
    return result
