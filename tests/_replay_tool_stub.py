"""Stub tool implementations for replay override tests."""


def lookup_policy(**kwargs: object) -> dict[str, object]:
    del kwargs
    return {"source": "stub"}


not_callable = {"source": "not-callable"}
