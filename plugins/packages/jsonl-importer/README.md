# Kitaru JSONL importer

Import sessions that already match Kitaru's canonical JSONL session contract. This package backs the built-in `kitaru/kitaru-jsonl` importer and validates each line independently on a Kitaru worker.

Most users do not install or call this package directly. Start a Kitaru worker, then select the built-in importer:

```bash
kitaru session import sessions.jsonl \
  --importer kitaru/kitaru-jsonl@latest \
  --agent support-agent@latest \
  --wait
```

Write one complete Kitaru session object per line. Unknown fields and invalid records are rejected independently, so valid lines in the same upload can still import. Use a provider-specific importer when the input is a raw observability export rather than canonical Kitaru JSONL.

See the [Kitaru JSONL guide](https://docs.zenml.io/kitaru/guides/importing-sessions) for the full session and node schema.

Nodes must use the flat indexed representation; no nested parent-depth limit applies to those indexes. Costs must be finite and nonnegative, and token counts must be nonnegative. A line whose data cannot decode or serialize is reported as a failure without preventing valid neighboring lines from importing.

## Links

- [Kitaru documentation](https://docs.zenml.io/kitaru)
- [Source code](https://github.com/zenml-io/kitaru)
- [Issue tracker](https://github.com/zenml-io/kitaru/issues)

Licensed under Apache-2.0.
