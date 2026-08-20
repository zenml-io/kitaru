# Kitaru Verifiers exporter

Export a Kitaru experiment cohort as a Verifiers 0.3.0 benchmark with a bundled Harness and PrimeRL 0.8.0 source configuration.

## Install

Install the exporter in the same Python environment as Kitaru:

```bash
uv add kitaru-verifiers-exporter
```

The existing `kitaru experiment export` CLI and MCP export tools discover the package automatically. Generated projects contain their Taskset, default Harness, scoring assets, and runtime bridge, so running an exported benchmark does not require this exporter package in the Verifiers environment.
