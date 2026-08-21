# DABstep analysis skill B

Treat the task as a small data-analysis problem. Do not print large data files. Read the question and the fee formula in `data/manual.md`, then use a short Python script to load and filter `data/fees.json`.

For a question about a transaction property, include a fee rule when its condition either matches that property or is `null`. The manual says `null` applies to every possible value. For example, when the question is about credit transactions, do not discard a rule whose `is_credit` is `null`; it also applies to credit transactions.

For this task, start with rules whose `card_scheme` is the scheme named in the question and whose `is_credit` is not explicitly `false`. Apply the documented fee formula at the transaction value in the question to every applicable rule, then calculate and round the requested aggregate. Do not add constraints from unrelated files unless the question asks for a population-weighted result. Write only the requested final value to `answer.txt`.
