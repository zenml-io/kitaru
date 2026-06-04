---
description: "Deploy a new flow version and attach one routing tag."
---

# kitaru deploy

`kitaru deploy` attaches exactly one routing tag at deploy time. To add or move more tags later, use `kitaru flow tag`.

## Usage

```bash
kitaru deploy TARGET [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `TARGET` | `str` | Yes |  | Flow target `&lt;module_or_file&gt;:&lt;flow_name&gt;`. |
| `--tag` | `str` | No | `"default"` | Single routing tag to attach at deploy time. Use `kitaru flow tag` to add or move tags later. |
| `--exclusive` | `bool` | No | `False` | Make this deploy-time tag exclusive. |
| `--input` | `str` | No | `None` | Representative deployment-time flow inputs as JSON or `@file`. |
| `--image` | `str` | No | `None` | Deployment-time image override as a base image string, JSON object, or `@file`. |
| `--stack` | `str` | No | `None` | Optional stack override. |
| `--cache` | `bool` | No | `None` | Optional cache override. |
| `--retries` | `int` | No | `None` | Optional retry override. |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |

