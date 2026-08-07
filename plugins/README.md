# Kitaru plugins

Each directory under `packages/` is an independently versioned Python distribution for one built-in Kitaru importer or evaluator. A release of one package cannot create new versions for the other default plugins.

Every distribution exposes one catalog through the `kitaru.default_plugins` entry-point group. At startup, Kitaru records the owning distribution and exact version as the package requirement for that plugin.

## Local development

Install Kitaru and all plugin workspace members from the repository root:

```bash
uv sync --all-packages --extra server --extra otel
```

This exposes the same catalogs used by published wheels without requiring a PyPI release. Build one distribution by passing its project directory:

```bash
uv build --project plugins/packages/importer-langfuse --out-dir plugins/dist
```

## Releases

The `Release plugin` workflow accepts a package directory name and its committed version. It tests and publishes only that distribution, then creates a package-specific tag such as `importer-langfuse-v0.2.0`.

`default-requirements.txt` pins the plugin versions bundled into Kitaru server images. Update only the line for the plugin whose published version should become the server default.
