"""Exercise DB syntax diagnostics without importing the database runtime."""

from pathlib import Path

import pytest
from scripts.check_python_standards import DB_PATH, check_source, main


@pytest.mark.parametrize(
    ("source", "directory", "codes", "line"),
    [
        ("session.commit()", "repositories", ["KIT001"], 1),
        ("result.first()", "repositories", ["KIT002"], 1),
        (
            "from sqlalchemy.orm import mapped_column as column\n"
            "x = column(nullable=False, index=True, unique=False)",
            "orm",
            ["KIT003"],
            2,
        ),
        (
            "import sqlalchemy as sa\nimport sqlalchemy.orm as orm\n"
            "x = orm.mapped_column(sa.ForeignKey('other.id'))",
            "orm",
            ["KIT004"],
            3,
        ),
        (
            "from sqlalchemy import ForeignKey as FK\n"
            "from sqlalchemy.orm import mapped_column as column\n"
            "x = column(FK('other.id'), nullable=True)",
            "orm",
            ["KIT003", "KIT004"],
            3,
        ),
    ],
)
def test_prohibited_syntax(
    source: str, directory: str, codes: list[str], line: int
) -> None:
    """Report stable codes, repository-relative paths, lines, and remediation."""
    path = DB_PATH / directory / "example.py"
    findings = check_source(source, path)
    assert [finding.code for finding in findings] == codes
    assert all(
        finding.path == path.as_posix() and finding.line == line for finding in findings
    )
    assert all(finding.message for finding in findings)


@pytest.mark.parametrize("directory", ["repositories", "orm", "other"])
def test_allowed_syntax(directory: str) -> None:
    """Allow named constraints, bare columns, flush, and explicit top-one SQL."""
    source = """
from sqlalchemy import ForeignKeyConstraint, Index, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
id: Mapped[int]
x = mapped_column(String(64))
constraints = (
    Index('ix', 'x', unique=True), UniqueConstraint('x', name='uq'),
    ForeignKeyConstraint(['x'], ['other.id'], name='fk')
)
session.flush()
result.one_or_none()
result.scalar_one_or_none()
session.execute(select(Item).limit(1)).one_or_none()
"""
    assert check_source(source, DB_PATH / directory / "example.py") == []


def test_rules_are_path_scoped() -> None:
    """Ignore similarly named calls outside their specific DB directories."""
    assert (
        check_source("result.first(); session.commit()", DB_PATH / "orm/example.py")
        == []
    )
    assert (
        check_source(
            "from sqlalchemy.orm import mapped_column\n"
            "x = mapped_column(nullable=True)",
            DB_PATH / "repositories/example.py",
        )
        == []
    )
    assert check_source("result.first(); session.commit()", Path("src/other.py")) == []


def test_cli_reports_all_sorted_findings(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Fail once with every violation sorted by path and line."""
    directory = tmp_path / DB_PATH / "repositories"
    directory.mkdir(parents=True)
    (tmp_path / DB_PATH / "orm").mkdir()
    (directory / "z.py").write_text("result.first()\n", encoding="utf-8")
    (directory / "a.py").write_text(
        "session.commit()\nresult.first()\n", encoding="utf-8"
    )
    assert main(["--root", str(tmp_path)]) == 1
    lines = capsys.readouterr().out.splitlines()
    assert [line.split(": ")[0] for line in lines] == [
        f"{DB_PATH}/repositories/a.py:1",
        f"{DB_PATH}/repositories/a.py:2",
        f"{DB_PATH}/repositories/z.py:1",
    ]
    assert [line.split(": ")[1].split()[0] for line in lines] == [
        "KIT001",
        "KIT002",
        "KIT002",
    ]


def test_cli_clean_and_malformed_source(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Return zero for clean input and a clear failure for invalid Python."""
    directory = tmp_path / DB_PATH / "orm"
    directory.mkdir(parents=True)
    (tmp_path / DB_PATH / "repositories").mkdir()
    path = directory / "example.py"
    path.write_text("x = 1\n", encoding="utf-8")
    assert main(["--root", str(tmp_path)]) == 0
    assert capsys.readouterr().out == ""
    path.write_text("def broken(\n", encoding="utf-8")
    assert main(["--root", str(tmp_path)]) == 2
    error = capsys.readouterr().err
    assert f"{DB_PATH}/orm/example.py: checker failed:" in error
    assert "Traceback" not in error


def test_cli_rejects_missing_or_empty_input(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A wrong root or empty DB tree must not look like a passing check."""
    assert main(["--root", str(tmp_path)]) == 2
    assert "DB directory is missing" in capsys.readouterr().err
    (tmp_path / DB_PATH).mkdir(parents=True)
    assert main(["--root", str(tmp_path)]) == 2
    assert "directory is missing" in capsys.readouterr().err
    (tmp_path / DB_PATH / "repositories").mkdir()
    (tmp_path / DB_PATH / "orm").mkdir()
    assert main(["--root", str(tmp_path)]) == 2
    assert "no Python files found" in capsys.readouterr().err
