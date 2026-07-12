"""Markdown knowledge-base search for the reference-agent example."""

from pathlib import Path
from typing import Any

from pydantic import BaseModel

from .config import EXAMPLE_DIR

DEFAULT_KB_DIR = EXAMPLE_DIR / "knowledge_base"


class KnowledgeResult(BaseModel):
    """One local knowledge-base search hit."""

    document_id: str
    snippet: str
    score: int


def search_kb(
    query: str, kb_dir: Path = DEFAULT_KB_DIR, limit: int = 3
) -> list[dict[str, Any]]:
    """Return relevant Markdown snippets with stable document ids."""
    terms = {term.strip(".,:;!?()[]").lower() for term in query.split()}
    terms = {term for term in terms if len(term) >= 3}
    results: list[KnowledgeResult] = []
    for path in sorted(kb_dir.glob("*.md")):
        for heading_slug, text in _sections(path).items():
            lowered = text.lower()
            score = sum(1 for term in terms if term in lowered)
            if score == 0:
                continue
            results.append(
                KnowledgeResult(
                    document_id=f"{path.name}#{heading_slug}",
                    snippet=" ".join(text.split())[:500],
                    score=score,
                )
            )
    ranked = sorted(results, key=lambda item: (-item.score, item.document_id))
    return [item.model_dump() for item in ranked[:limit]]


def _sections(path: Path) -> dict[str, str]:
    sections: dict[str, list[str]] = {}
    current = "overview"
    sections[current] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("## "):
            current = _slugify(line.removeprefix("## "))
            sections.setdefault(current, [])
            continue
        sections[current].append(line)
    return {key: "\n".join(lines).strip() for key, lines in sections.items()}


def _slugify(value: str) -> str:
    return "-".join(
        part.strip().lower()
        for part in value.replace("/", " ").replace("&", " ").split()
        if part.strip()
    )
