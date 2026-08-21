# DABstep analysis skill C

Treat each task as a small, reproducible data-analysis problem.

Read `task.md` first, then inspect `data/manual.md` and only the schemas or small samples needed to identify the relevant files and fields. Prefer a short Python script over long shell pipelines or printing whole datasets.

Translate every condition in the question into an explicit filter. When a documented rule says that a missing or `null` condition is a wildcard, include that rule for every matching value instead of discarding it. Do not add population weights, mappings, or constraints unless the question or manual requires them.

Before writing the answer, check the units, aggregation, rounding, and requested output format. Write only the requested final value or values to `answer.txt`.
