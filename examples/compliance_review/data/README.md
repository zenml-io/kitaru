# Compliance review data

This directory contains synthetic, self-contained JSON data for the compliance
review example.

The data is deliberately plain and inspectable so the deterministic retrieval
layer can load it without PDF parsing, embeddings, or external services.

## Shape

- `company.json` describes Acme Corp and lists the available standards and
  documents.
- `standards/*.json` files contain compliance requirements with stable
  requirement IDs, keywords, severity, and expected evidence.
- `documents/*.json` files contain document metadata, ordered sections, and a
  `known_planted_findings` array.

Each document section has:

- `id`
- `title`
- `content`

Those stable section IDs are intended to make Item 3's `read_document()`,
`read_section()`, and `search_documents()` helpers straightforward.

## Planted findings

The synthetic review outcomes are intentionally visible in the data:

| Document | Expected outcome |
|---|---|
| `employee_handbook` | Gap: missing updated 2024 parental leave policy |
| `it_security_policy` | Gap: no data retention schedule; outdated incident response procedures |
| `vendor_contract_alpha` | Pass: standard clauses present |
| `vendor_contract_beta` | Gap: missing indemnification and liability cap |
| `insurance_policy` | Gap: missing cyber incident coverage |
| `financial_statements_2024` | Gap: Q3 revenue summary does not match detailed breakdown |
| `data_privacy_policy` | Pass: GDPR-aligned for this synthetic review |
| `disaster_recovery_plan` | Gap: last full test was three years ago; no cloud failover documented |
