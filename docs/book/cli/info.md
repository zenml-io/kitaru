---
description: "Show detailed environment information for the current setup."
---

# kitaru info

Use `--all` to include active stack/project provenance. The provenance shows whether the effective context came from environment variables, repo-local `.kitaru/config.yaml`, or global config, and is also included in JSON output and exported diagnostics files.

## Usage

```bash
kitaru info [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `--all`, `-a` | `bool` | No | `False` | Full diagnostic: include all packages, environment type, and active stack/project provenance. |
| `--all-packages` | `bool` | No | `False` | Include all installed package versions. |
| `--packages`, `-p` | `tuple[str, Ellipsis]` | No | `()` | Include specific package versions (repeatable). |
| `--file`, `-f` | `str` | No | `None` | Export diagnostics to file (format from extension: .json, .yaml). |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |

