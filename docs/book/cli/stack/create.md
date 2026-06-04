---
description: "Create a local, Kubernetes-backed, Vertex AI, SageMaker, or AzureML stack."
---

# kitaru stack create

## Usage

```bash
kitaru stack create [NAME] [OPTIONS]
```

## Parameters

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `NAME` | `str` | No | `None` | Stack name. Required unless provided in --file. |
| `--file`, `-f` | `Path` | No | `None` | Load stack configuration from a YAML file. |
| `--no-activate` | `bool` | No | `None` | Create without activating the stack. |
| `--type` | `str` | No | `None` | Stack type: local, kubernetes, vertex, sagemaker, or azureml. |
| `--artifact-store` | `str` | No | `None` | Artifact store URI for remote stacks (Kubernetes: s3:// or gs://; Vertex: gs://; SageMaker: s3://; AzureML: az://, abfs://, or abfss://). |
| `--container-registry` | `str` | No | `None` | Container registry URI for Kubernetes, Vertex, SageMaker, or AzureML stacks. |
| `--cluster` | `str` | No | `None` | Kubernetes cluster name. |
| `--region` | `str` | No | `None` | Cloud region for Kubernetes, Vertex, SageMaker, or AzureML stacks. Optional for AzureML. |
| `--subscription-id` | `str` | No | `None` | Azure subscription ID for AzureML stacks. |
| `--resource-group` | `str` | No | `None` | Azure resource group for AzureML stacks. |
| `--workspace` | `str` | No | `None` | AzureML workspace name for AzureML stacks. |
| `--execution-role` | `str` | No | `None` | SageMaker execution role ARN. |
| `--namespace` | `str` | No | `None` | Kubernetes namespace (defaults to `default`). |
| `--credentials` | `str` | No | `None` | Optional credentials reference for Kubernetes, Vertex, SageMaker, or AzureML stacks. |
| `--extra` | `list[str]` | No | `None` | Advanced component defaults as TARGET.FIELD=VALUE. Valid targets: orchestrator, artifact_store, container_registry. VALUE uses YAML parsing, so booleans, numbers, lists, and objects are accepted. |
| `--async` | `bool` | No | `None` | Run remote stacks asynchronously by default (equivalent to `--extra orchestrator.synchronous=false`). |
| `--no-verify` | `bool` | No | `None` | Skip credential verification for Kubernetes, Vertex, SageMaker, or AzureML stacks. |
| `--output`, `-o` | `str` | No | `"text"` | Output format: "text" (default) or "json". |

