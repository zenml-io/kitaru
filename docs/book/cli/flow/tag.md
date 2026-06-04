---
description: "Attach or move a public tag on one deployment version."
---

# kitaru flow tag

Use this after `kitaru deploy` when you want to add another route or move an existing one without creating a new deployment version.

## Usage

```bash
kitaru flow tag FLOW TAG [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `FLOW` | `str` | Yes |  | Deployment-backed flow name. |
| `TAG` | `str` | Yes |  | Public deployment tag to attach or move after deploy time. |
| `--version` | `int` | Yes |  | Exact deployment version to tag. |
| `--exclusive` | `bool` | No | `False` | Move this tag away from older versions. |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |

