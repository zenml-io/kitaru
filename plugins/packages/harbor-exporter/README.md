# Kitaru Harbor exporter

Export a Kitaru experiment cohort as a Harbor 0.20.0 task dataset.

## Install

Install the exporter in the same Python environment as Kitaru:

```bash
uv add kitaru-harbor-exporter
```

The existing `kitaru experiment export` CLI and MCP export tools discover the package automatically. Generated projects contain the Harbor adapter and evaluation runtime they need, so running an export does not require this package in the Harbor consumer environment.
