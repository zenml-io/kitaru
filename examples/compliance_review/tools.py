"""Deterministic JSON-backed retrieval tools for the compliance review example.

The functions in this module are intentionally boring: they load the synthetic
JSON files shipped next to the example and perform exact ID lookup plus simple
case-insensitive keyword search. There are no model calls, embeddings, vector
stores, or external services involved.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

DATA_DIR = Path(__file__).resolve().parent / "data"
DOCUMENTS_DIR = DATA_DIR / "documents"
STANDARDS_DIR = DATA_DIR / "standards"

SearchResult = dict[str, Any]
JsonRecord = dict[str, Any]


def search_documents(query: str) -> list[SearchResult]:
    """Search sections and requirements in the local synthetic JSON data.

    The search is a simple deterministic token match:

    1. split the query into lowercase alphanumeric terms
    2. count term occurrences in each searchable section/requirement
    3. sort by score descending, then by record ID and match ID

    Both company documents and standards are searched. This lets later stages
    ask questions such as "data retention SOC 2" and discover both the policy
    document and the relevant standard.
    """
    terms = _query_terms(query)
    if not terms:
        return []

    results: list[SearchResult] = []
    for record in _load_searchable_records():
        results.extend(_search_record(record=record, terms=terms))

    return sorted(
        results,
        key=lambda result: (
            -int(result["score"]),
            str(result["doc_id"]),
            str(result.get("section_id") or result.get("requirement_id") or ""),
        ),
    )


def read_document(doc_id: str, section: str | None = None) -> str:
    """Read a full local JSON document or one named section.

    Args:
        doc_id: Stable ID from `company.json`, `data/documents/*.json`, or
            `data/standards/*.json`.
        section: Optional section ID to read from the record's `sections`
            array.

    Returns:
        Human-readable text assembled from the JSON record.
    """
    if section is not None:
        return read_section(doc_id=doc_id, section=section)

    record = _load_record_by_id(doc_id)
    lines = _record_header(record)

    requirements = record.get("requirements", [])
    if requirements:
        lines.append("\nRequirements:")
        for requirement in requirements:
            lines.extend(_format_requirement(requirement))

    sections = record.get("sections", [])
    if sections:
        lines.append("\nSections:")
        for item in sections:
            lines.extend(_format_section(item))

    return "\n".join(lines)


def read_section(doc_id: str, section: str) -> str:
    """Read one named section from a local JSON record."""
    record = _load_record_by_id(doc_id)
    for item in record.get("sections", []):
        if item.get("id") == section:
            lines = [
                f"Document: {record.get('title', record['id'])} ({record['id']})",
                f"Section: {item.get('title', section)} ({section})",
                "",
                str(item.get("content", "")),
            ]
            return "\n".join(lines)

    available = ", ".join(item["id"] for item in record.get("sections", []))
    raise ValueError(
        f"Unknown section '{section}' for document '{doc_id}'. "
        f"Available sections: {available or '<none>'}"
    )


def list_documents() -> list[dict[str, Any]]:
    """List available company documents with stable metadata.

    Standards are intentionally not returned here. They are searchable and
    readable by ID, but this listing mirrors the example's company-document
    catalog from `company.json`.
    """
    company = _load_json(DATA_DIR / "company.json")
    documents_by_id = {
        record["id"]: record for record in _load_records_from_dir(DOCUMENTS_DIR)
    }

    items: list[dict[str, Any]] = []
    for doc_id in company.get("documents", []):
        record = documents_by_id[doc_id]
        items.append(
            {
                "id": record["id"],
                "title": record.get("title"),
                "domain": record.get("domain"),
                "department": record.get("department"),
                "effective_date": record.get("effective_date"),
                "last_reviewed": record.get("last_reviewed"),
                "status": record.get("status"),
                "summary": record.get("summary"),
                "metadata": record.get("metadata", {}),
            }
        )
    return items


def get_company_info() -> dict[str, Any]:
    """Return the synthetic Acme Corp company profile."""
    return _load_json(DATA_DIR / "company.json")


def _load_searchable_records() -> list[JsonRecord]:
    """Load records included in deterministic search."""
    return [
        *_load_records_from_dir(DOCUMENTS_DIR),
        *_load_records_from_dir(STANDARDS_DIR),
    ]


def _load_record_by_id(record_id: str) -> JsonRecord:
    """Load one known JSON record by stable ID."""
    records = {
        "acme_corp": _load_json(DATA_DIR / "company.json"),
        **{record["id"]: record for record in _load_records_from_dir(DOCUMENTS_DIR)},
        **{record["id"]: record for record in _load_records_from_dir(STANDARDS_DIR)},
    }
    try:
        return records[record_id]
    except KeyError as exc:
        available = ", ".join(sorted(records))
        raise ValueError(
            f"Unknown document ID '{record_id}'. Available IDs: {available}"
        ) from exc


def _load_records_from_dir(directory: Path) -> list[JsonRecord]:
    """Load JSON records from a directory in deterministic filename order."""
    return [_load_json(path) for path in sorted(directory.glob("*.json"))]


def _load_json(path: Path) -> JsonRecord:
    """Load one JSON object from disk."""
    with path.open(encoding="utf-8") as file:
        data = json.load(file)
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return data


def _query_terms(query: str) -> list[str]:
    """Normalize a free-text query into deterministic search terms."""
    return sorted({term for term in re.findall(r"[a-z0-9]+", query.lower()) if term})


def _search_record(record: JsonRecord, terms: list[str]) -> list[SearchResult]:
    """Search one JSON record and return scored matches."""
    results: list[SearchResult] = []

    for section in record.get("sections", []):
        searchable_text = _join_text(
            record.get("id"),
            record.get("title"),
            record.get("summary"),
            record.get("description"),
            record.get("domain"),
            record.get("department"),
            _metadata_keywords(record),
            section.get("id"),
            section.get("title"),
            section.get("content"),
        )
        result = _score_match(
            record=record,
            match_type="section",
            match_id=section.get("id", ""),
            match_title=section.get("title", ""),
            text=str(section.get("content", "")),
            searchable_text=searchable_text,
            terms=terms,
        )
        if result is not None:
            results.append(result)

    for requirement in record.get("requirements", []):
        searchable_text = _join_text(
            record.get("id"),
            record.get("title"),
            record.get("description"),
            record.get("domain"),
            _metadata_keywords(record),
            requirement.get("id"),
            requirement.get("title"),
            requirement.get("severity"),
            requirement.get("keywords"),
            requirement.get("requirement"),
            requirement.get("expected_evidence"),
        )
        result = _score_match(
            record=record,
            match_type="requirement",
            match_id=requirement.get("id", ""),
            match_title=requirement.get("title", ""),
            text=str(requirement.get("requirement", "")),
            searchable_text=searchable_text,
            terms=terms,
        )
        if result is not None:
            results.append(result)

    return results


def _score_match(
    *,
    record: JsonRecord,
    match_type: str,
    match_id: str,
    match_title: str,
    text: str,
    searchable_text: str,
    terms: list[str],
) -> SearchResult | None:
    """Score one candidate search match."""
    lowered = searchable_text.lower()
    matched_terms = [term for term in terms if term in lowered]
    if not matched_terms:
        return None

    score = sum(lowered.count(term) for term in matched_terms)
    result: SearchResult = {
        "doc_id": record["id"],
        "title": record.get("title", record["id"]),
        "kind": record.get("kind"),
        "domain": record.get("domain"),
        "match_type": match_type,
        "score": score,
        "matched_terms": matched_terms,
        "snippet": _snippet(text=text, terms=matched_terms),
    }
    if match_type == "section":
        result["section_id"] = match_id
        result["section_title"] = match_title
    else:
        result["requirement_id"] = match_id
        result["requirement_title"] = match_title
    return result


def _metadata_keywords(record: JsonRecord) -> list[str]:
    """Return keyword strings from a record's metadata."""
    metadata = record.get("metadata", {})
    keywords = metadata.get("keywords", []) if isinstance(metadata, dict) else []
    return [str(keyword) for keyword in keywords]


def _join_text(*parts: object) -> str:
    """Flatten nested text-ish values into one searchable string."""
    flattened: list[str] = []
    for part in parts:
        if part is None:
            continue
        if isinstance(part, list):
            flattened.append(_join_text(*part))
        elif isinstance(part, dict):
            flattened.append(_join_text(*part.values()))
        else:
            flattened.append(str(part))
    return " ".join(flattened)


def _snippet(*, text: str, terms: list[str], width: int = 240) -> str:
    """Return a compact deterministic snippet around the first matched term."""
    if len(text) <= width:
        return text

    lowered = text.lower()
    positions = [lowered.find(term) for term in terms if lowered.find(term) >= 0]
    if not positions:
        return text[: width - 1].rstrip() + "…"

    start = max(min(positions) - 60, 0)
    end = min(start + width, len(text))
    snippet = text[start:end].strip()
    if start > 0:
        snippet = "…" + snippet
    if end < len(text):
        snippet += "…"
    return snippet


def _record_header(record: JsonRecord) -> list[str]:
    """Format stable top-level metadata for `read_document()`."""
    lines = [
        f"Title: {record.get('title', record['id'])}",
        f"ID: {record['id']}",
        f"Kind: {record.get('kind', 'unknown')}",
    ]
    for key in (
        "domain",
        "department",
        "effective_date",
        "last_reviewed",
        "status",
        "version",
        "description",
        "summary",
    ):
        if record.get(key) is not None:
            lines.append(f"{key.replace('_', ' ').title()}: {record[key]}")
    return lines


def _format_requirement(requirement: JsonRecord) -> list[str]:
    """Format one standard requirement."""
    lines = [
        f"- {requirement.get('title', requirement['id'])} ({requirement['id']})",
        f"  Severity: {requirement.get('severity', 'unknown')}",
        f"  Requirement: {requirement.get('requirement', '')}",
    ]
    expected_evidence = requirement.get("expected_evidence", [])
    if expected_evidence:
        lines.append(f"  Expected evidence: {'; '.join(expected_evidence)}")
    return lines


def _format_section(section: JsonRecord) -> list[str]:
    """Format one section from a company document or standard."""
    return [
        f"- {section.get('title', section['id'])} ({section['id']})",
        f"  {section.get('content', '')}",
    ]
