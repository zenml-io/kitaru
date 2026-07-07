---
description: Create, inspect, and use Modal-backed stacks with your remote storage and registry
icon: cloud
---

# Modal

A Modal stack runs each Kitaru execution on [Modal](https://modal.com), so a flow you develop, run, replay, and diff locally executes the same way on Modal's managed compute. Kitaru builds the execution image and pushes it to the remote container registry you configure, and checkpoints and saved artifacts go to the remote storage you configure. Modal records a sandbox ID for each run, which Kitaru stores as run metadata.

Modal is a managed-runner path, so there is no `--cluster` or `--namespace` flag. Unlike the other cloud stacks, Modal is not tied to a single cloud provider: you pick the storage and registry pair you already have (AWS, GCP, or Azure), and Kitaru infers the provider from your artifact-store URI. If you want the broader stack model first, start with [Stacks](README.md).

## Before you start

Before creating the stack, make sure these already exist:

- a Kitaru server you are connected to with `kitaru login ...`
- Kitaru installed with the Modal extra: `uv add "kitaru[modal]"` or `pip install "kitaru[modal]"`
- a Modal account with authentication configured (see the [Modal token docs](https://modal.com/docs/reference/cli/token))
- a remote storage URI for artifacts, such as `s3://my-bucket/kitaru`, `gs://my-bucket/kitaru`, or `az://my-container/kitaru`
- a remote container registry that Modal can pull from, such as an ECR, Artifact Registry, or ACR repository
- Docker CLI available on the machine where you submit flows, with `DOCKER_BUILDKIT=1` set for image builds (see the image-builder note below)
- cloud permissions for the bucket and registry you point at

Kitaru creates the stack definition and component records. It does not create your bucket, registry repository, Modal account, or IAM permissions for you. If matching server-side ZenML service connectors already exist for the bucket and registry, Kitaru links those connectors to the storage and registry components. If you pass explicit cloud credential flags, Kitaru creates a new cloud service connector instead.

The `kitaru[modal]` extra installs the Python packages needed for Kitaru to
validate and create Modal components. It does not create or configure your Modal
account, Modal token, bucket, registry, Docker setup, or cloud permissions.

### Modal credentials and cloud credentials are different

There are two separate credential channels:

- **Modal API credentials** let Kitaru ask Modal to run work. Use your normal Modal CLI configuration, or pass `--extra orchestrator.token_id=...` and `--extra orchestrator.token_secret=...` together.
- **Cloud credentials** let Kitaru attach credentials to the storage and registry components. Later, when a run starts, ZenML can hand registry credentials to Modal so Modal can pull the private image instead of trying an anonymous pull.

If you do not pass cloud credential flags, Kitaru first looks for existing server-side ZenML service connectors that match the storage bucket and the registry. For AWS, that means an `aws` connector for the `s3-bucket` resource and an `aws` connector for the `docker-registry` resource. The same pattern applies to GCP and Azure with their storage resource types plus `docker-registry`.

There are three possible outcomes:

1. Kitaru finds exactly one storage connector and exactly one registry connector. It links those existing connectors to the new Modal stack.
2. Kitaru finds none. It creates a connectorless stack. That keeps public resources and manually configured environments working exactly as before.
3. Kitaru finds only one connector, or finds multiple possible connectors for one resource. It stops and tells you which connector is missing or ambiguous instead of creating a stack that will probably fail later.

In a connectorless setup:

- The machine that submits the flow must be able to build the image, push it to the registry, and write build metadata.
- The Modal runtime must be able to pull the image and read/write the artifact store by some other means.

A good failure story to keep in mind: your laptop builds and pushes `123456789012.dkr.ecr.eu-west-1.amazonaws.com/kitaru:...` successfully, then Modal starts remotely and tries to pull that image anonymously. The run fails with an authentication error even though the stack was created successfully. Reusing matching server-side connectors, or passing explicit cloud credential flags, avoids that failure: Kitaru links credentials to the artifact store and registry components, not to the Modal runner.

### Why Modal needs an image builder

Modal runs a container image, and something has to build that image before Modal can start the run. For Modal stacks, Kitaru stores an explicit local image-builder component on the stack. That component builds the image on the machine where you submit the flow, so **Docker CLI must be available wherever you run the flow**.

Kitaru configures this image builder with `use_subprocess_call=True`. In plain terms, Kitaru tells ZenML to call the Docker command-line tool instead of using the Docker Python SDK directly. Set this environment variable before submitting Modal runs:

```bash
export DOCKER_BUILDKIT=1
```

The reason is specific and practical. Kitaru builds the image, pushes it to your registry, and then Modal imports that registry image. Docker SDK builds can produce a mixed Docker/OCI media-type image shape that Modal rejects while unpacking. In the ECR + Modal test, Docker CLI with BuildKit produced OCI layers that Modal accepted.

Kitaru also defaults Modal flow images to `linux/amd64` unless you explicitly set another image platform. This matters on Apple Silicon machines: without an explicit platform, Docker can build an ARM image tag that pushes successfully, but Modal later rejects it because the tag has no Linux/amd64 image to run.

If you would rather build images somewhere other than your submitting machine, read the [ZenML image-builder docs](https://docs.zenml.io/stacks/stack-components/image-builders) and configure a cloud-side builder for advanced setups.

## Create the stack

All three commands below create the same kind of Modal stack. The only thing that changes is the storage URI and the registry URL. Pick the cloud pair you already use. Kitaru reads the provider from the artifact-store URI, so `s3://` builds an AWS-backed pair, `gs://` a GCP-backed pair, and `az://` an Azure-backed pair.

{% tabs %}
{% tab title="AWS" %}
```bash
kitaru stack create prod-modal \
  --type modal \
  --artifact-store s3://my-bucket/kitaru \
  --container-registry 123456789012.dkr.ecr.eu-west-1.amazonaws.com/kitaru \
  --sandbox modal
```
{% endtab %}

{% tab title="GCP" %}
```bash
kitaru stack create prod-modal \
  --type modal \
  --artifact-store gs://my-bucket/kitaru \
  --container-registry us-central1-docker.pkg.dev/my-project/kitaru-images \
  --sandbox modal
```
{% endtab %}

{% tab title="Azure" %}
```bash
kitaru stack create prod-modal \
  --type modal \
  --artifact-store az://my-container/kitaru \
  --container-registry myregistry.azurecr.io/kitaru \
  --sandbox modal
```
{% endtab %}
{% endtabs %}

`--sandbox modal` is optional. It attaches a Modal sandbox component so your agent flows can execute code in an isolated Modal container. It is recommended for agent flows, but if you leave it off, Kitaru creates the Modal runner stack without a sandbox, consistent with the other remote stacks, which attach a sandbox only when you ask for one.

The required Modal fields are:

| Field | Meaning |
|---|---|
| `--artifact-store` | Remote storage URI where Kitaru writes checkpoint outputs and saved artifacts |
| `--container-registry` | Remote registry where Kitaru pushes the flow image |

The cloud credential fields are optional. Use them when you want Kitaru to create a new connector instead of reusing existing server-side connectors:

| Field | Meaning |
|---|---|
| `--region` | Cloud provider region when the provider needs one. For AWS-backed Modal stacks, this is the S3/ECR connector region and must match the ECR registry host. For GCP-backed Modal stacks, Kitaru can check it against GAR/GCR when the registry host includes a location. |
| `--subscription-id` | Azure subscription ID for Azure Blob/ADLS + ACR Modal stacks |
| `--credentials` | Cloud credential reference, such as `aws-profile:ml-team`, `gcp-service-account:/path/to/key.json`, or `azure-access-token:...` |
| `--no-verify` | Skip cloud connector verification when Kitaru is creating a connector. It does not affect existing-connector discovery and does not request a connector by itself; pair it with the needed cloud input, such as `--region`, `--subscription-id`, or `--credentials`. |

Top-level `--region` is **not** Modal placement. If you want to steer where Modal places work, set it through `--extra orchestrator.region=...` / `--extra orchestrator.cloud=...` instead.

`aws-profile:PROFILE` is only safe when the ZenML connector runtime can see that AWS profile. On a remote ZenML server, your laptop's SSO profile is usually invisible to the server. Prefer reusing an existing server-side connector for remote Modal stacks, or pass credentials that the server can use directly.

## Set advanced Modal defaults

Named flags cover the common setup. Use `--extra` for lower-level component fields that Kitaru does not expose as first-class flags.

```bash
kitaru stack create prod-modal \
  --type modal \
  --artifact-store s3://my-bucket/kitaru \
  --container-registry 123456789012.dkr.ecr.eu-west-1.amazonaws.com/kitaru \
  --sandbox modal \
  --async \
  --extra orchestrator.modal_environment=production \
  --extra orchestrator.timeout=7200 \
  --extra sandbox.app_name=kitaru-agent-sandbox \
  --extra sandbox.timeout=1800
```

Useful `--extra` keys for a Modal stack:

- **Runner behavior**: `orchestrator.region`, `orchestrator.cloud`, `orchestrator.modal_environment`, and `orchestrator.timeout` for Modal placement and timeout.
- **Modal credentials**: `orchestrator.token_id` and `orchestrator.token_secret`. These must be set **together**; providing only one is rejected before Kitaru talks to Modal.
- **Sandbox settings**: `sandbox.app_name`, `sandbox.timeout`, `sandbox.image`, `sandbox.cpu`, and `sandbox.memory`. These only apply when you also pass `--sandbox modal`; sandbox overrides without a sandbox are rejected.

`--async` is shorthand for `--extra orchestrator.synchronous=false`. If you provide both, the explicit `--extra orchestrator.synchronous=...` value wins.

If you need provider-specific component credentials or settings that are not shown here, keep them in a reviewed stack YAML template and pass them through `extra:` / `--extra`. Treat those settings as advanced ZenML component configuration: test them with a small run before using the stack for production work.

## Use YAML for repeatable setup

Keep the same inputs in a YAML file. Swap the artifact store and registry values for your own cloud provider. Everything else stays the same across AWS, GCP, and Azure.

```yaml
name: prod-modal
type: modal
artifact_store: s3://my-bucket/kitaru
container_registry: 123456789012.dkr.ecr.eu-west-1.amazonaws.com/kitaru
sandbox: modal
async: true
extra:
  orchestrator:
    modal_environment: production
    timeout: 7200
  sandbox:
    app_name: kitaru-agent-sandbox
    timeout: 1800
```

Create it with:

```bash
kitaru stack create -f stack.yaml
```

CLI flags override YAML values, and CLI `--extra` values merge on top of the YAML `extra:` block instead of replacing it.

## Inspect and use it

```bash
kitaru stack show prod-modal
kitaru stack use prod-modal
kitaru stack current
```

`kitaru stack show` reports the translated Kitaru view: Modal as the runner, your configured remote storage, your configured remote registry, active status, and whether the stack was created by Kitaru. You will only see `sandbox: modal` if you created the stack with `--sandbox modal`.

Once active, normal flow runs use the Modal stack unless a flow-level or run-level stack override is present.

## Delete it

```bash
kitaru stack delete prod-modal
```

Use `--recursive` if you want Kitaru to remove Kitaru-managed component records too. Kitaru deletes only the Kitaru-managed stack records and components. It does not delete your bucket, registry repository, Modal account, cloud IAM, or any other cloud resource.

## Safety and cost notes

{% hint style="warning" %}
Modal runs remotely, so it cannot read a local artifact store, a local container registry, or any component that points at a local filesystem path. A flow running on Modal must also be able to reach your Kitaru/ZenML server. Use remote storage and a remote registry for every Modal stack.
{% endhint %}

{% hint style="danger" %}
A Modal sandbox keeps billing while it is alive. Calling `close()` on a sandbox session does **not** stop billing; only `destroy()` or a configured TTL does. Set a sandbox `timeout` (TTL) so idle sandboxes do not keep charging. See the [Modal sandboxes guide](https://modal.com/docs/guide/sandboxes).
{% endhint %}

## Related

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Stacks</strong></td><td>The shared stack model, precedence rules, YAML, --extra, and --async</td><td><a href="README.md">README.md</a></td></tr><tr><td><strong>Containerization</strong></td><td>How Kitaru builds and configures remote execution images</td><td><a href="../guides/containerization.md">../guides/containerization.md</a></td></tr></tbody></table>

For deeper Modal reference, see the [ZenML Modal orchestrator docs](https://docs.zenml.io/stacks/stack-components/orchestrators/modal), the [ZenML Modal sandbox docs](https://docs.zenml.io/stacks/stack-components/sandboxes/modal), the [Modal token docs](https://modal.com/docs/reference/cli/token), and the [Modal configuration docs](https://modal.com/docs/reference/modal.config).
