# Changelog

## 0.2.0 - 2026-09-21

- Convert the legacy flat indexed JSONL representation to external node IDs and parent external IDs before import. Require Kitaru 0.27.0 or later for the new node contract.

## 0.1.2 - 2026-09-10

- Remove the importer payload size cap. Uploads are bounded by the server blob limit only.

## 0.1.1

- Isolate numeric validation, deeply nested JSON, and invalid Unicode failures per line while retaining flat indexed node support.

## 0.1.0

- First stable release of the Kitaru JSONL importer.

## 0.1.0rc0

- Initial release candidate for the Kitaru JSONL importer.
