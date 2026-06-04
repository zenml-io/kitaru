---
description: "Build an immutable deployment version from a flow target."
---

# kitaru build

## Usage

```bash
kitaru build TARGET [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `TARGET` | `str` | Yes |  | Flow target `&lt;module_or_file&gt;:&lt;flow_name&gt;`. |
| `--input` | `str` | No | `None` | Deployment-time default flow inputs as JSON or `@file`. |
| `--image` | `str` | No | `None` | Deployment-time image override as a base image string, JSON object, or `@file`. |
| `--stack` | `str` | No | `None` | Optional stack override. |
| `--cache` | `bool` | No | `None` | Optional cache override. |
| `--retries` | `int` | No | `None` | Optional retry override. |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |

