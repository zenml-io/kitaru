# Kitaru Plugins

This directory is the source of the independently versioned `kitaru-plugins` distribution. Its wheel contains the official importers and evaluators. These files are not included in the `kitaru` wheel.

The distribution exposes its catalog through the `kitaru.default_plugins` Python entry-point group. A Kitaru server discovers installed catalogs at startup and stores each definition as an exact package source such as `kitaru-plugins==0.1.0`. Workers install that requirement when they execute the importer or evaluator.

## Local development

Install all workspace packages from the repository root:

```bash
uv sync --all-packages --extra server --extra otel
```

The editable `kitaru-plugins` package then exposes the same catalog used by a published wheel. Starting the server through the root environment registers the local package version without a PyPI release.

Build and inspect the standalone artifacts with:

```bash
uv build --project plugins --out-dir plugins/dist
uv run --project plugins python -c "from kitaru_plugins.catalog import get_definitions; print(get_definitions())"
```

## Release order

1. Update the version and raise the minimum Kitaru version when the plugin contract requires it.
2. Add the release notes to `plugins/CHANGELOG.md`.
3. Run the `Release plugins` workflow in dry-run mode.
4. Publish the plugin release.
5. Use that published version when building Kitaru server images.

Plugin releases use `plugins-v<version>` tags. Kitaru releases continue to use `v<version>` tags.
