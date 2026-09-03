# Changelog

## Unreleased

- Fetch traces directly from the Braintrust API through the `adapter` extra.

## 0.2.0

- Add the Braintrust importer-backed adapter, installed through the `adapter` extra.
- Contain malformed numeric, nested payload, graph, and Unicode failures while preserving unrelated sessions.
- Bound parent validation and nested tool scanning; make trace identity and grouping independent of record order.

## 0.1.0

- First stable release of the Braintrust trace importer.

## 0.1.0rc0

- Initial release candidate for the Braintrust trace importer.
