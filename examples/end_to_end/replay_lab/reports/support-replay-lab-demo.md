# Replay Lab Report: Support Replay Lab demo

Generated: 2026-05-14T01:08:56.701240+00:00
Candidate: Cheaper deterministic support agent

Synthetic customer-support regression cohort. Observed executions use the champion profile over a deterministic production-like history; Replay Lab then compares baseline replay against a cheaper candidate profile.

Candidate notes: Replay the same support cases from draft_response with a shorter deterministic candidate profile. It should reduce cost and latency, but the regulated medical case should make reviewers look twice.

## Summary

- Cases: 12
- Completed candidate lanes: 10
- Failed or timed-out lanes: 2
- Cases with changed candidate output: 10
- Cases with replay drift warning: 6

## Case comparison

| Case | Observed | Baseline | Candidate | Candidate cost Δ | Candidate quality Δ | Output changed? |
|---|---:|---:|---:|---:|---:|---|
| support-refund-delay | 0.42 | 0.42 | 0.25 | -0.17 (-40.5%) | +0 (+0.0%) | yes |
| support-refund-delay--hist-01 | 0.44 | 0.44 | 0.26 | -0.18 (-40.9%) | +0 (+0.0%) | yes |
| support-refund-delay--hist-02 | 0.47 | 0.47 | 0.28 | -0.19 (-40.4%) | +0 (+0.0%) | yes |
| support-refund-delay--hist-03 | 0.4 | 0.4 | 0.24 | -0.16 (-40.0%) | +0 (+0.0%) | yes |
| regulated-medical-claim | 0.48 | 0.48 | 0.23 | -0.25 (-52.1%) | -0.12 (-12.0%) | yes |
| regulated-medical-claim--hist-01 | 0.5 | 0.5 | 0.24 | -0.26 (-52.0%) | -0.12 (-12.0%) | yes |
| regulated-medical-claim--hist-02 | 0.54 | 0.54 | 0.26 | -0.28 (-51.9%) | -0.12 (-12.0%) | yes |
| regulated-medical-claim--hist-03 | 0.46 | 0.46 | 0.22 | -0.24 (-52.2%) | -0.12 (-12.0%) | yes |
| shipping-tool-loop | 0.31 | 0.31 | n/a | n/a | n/a | unknown |
| shipping-tool-loop--hist-01 | 0.32 | 0.32 | 0.19 | -0.13 (-40.6%) | +0 (+0.0%) | yes |
| shipping-tool-loop--hist-02 | 0.35 | 0.35 | 0.2 | -0.15 (-42.9%) | +0 (+0.0%) | yes |
| shipping-tool-loop--hist-03 | 0.3 | 0.3 | n/a | n/a | n/a | unknown |

## support-refund-delay

Reason: Recent expensive reply for an enterprise refund complaint.

| Lane | Execution | Status | Cost | Duration | Latency | Quality |
|---|---|---|---:|---:|---:|---:|
| observed | 7a5e84ac-f424-411b-9fb5-0ecd3eacd634 | completed | 0.42 | 14.00s | 4.80s | 1 |
| baseline_replay | f3fe61e4-202e-4884-8753-0d3008480769 | completed | 0.42 | 12.00s | 4.80s | 1 |
| candidate_replay | ccec0330-76c5-4c8b-be30-c5a4b384520f | completed | 0.25 | 11.00s | 2.70s | 1 |

Limitations:
- observed: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.
- baseline_replay: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.
- candidate_replay: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.

## support-refund-delay--hist-01

Reason: Cost Outlier for enterprise refund delay case from Aster Cloud.

| Lane | Execution | Status | Cost | Duration | Latency | Quality |
|---|---|---|---:|---:|---:|---:|
| observed | 42bd6afb-96a6-47c1-b845-fd38fbf68feb | completed | 0.44 | 12.00s | 5.20s | 1 |
| baseline_replay | 6c301d83-e90a-4122-93e0-e7b026cdfac7 | completed | 0.44 | 14.00s | 5.20s | 1 |
| candidate_replay | 7b2c2b8c-6d5c-4ecf-a7fd-f2241e1d0491 | completed | 0.26 | 10.00s | 2.90s | 1 |

Limitations:
- observed: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.
- baseline_replay: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.
- candidate_replay: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.

## support-refund-delay--hist-02

Reason: Customer Complaint for enterprise refund delay case from Northwind Health.

| Lane | Execution | Status | Cost | Duration | Latency | Quality |
|---|---|---|---:|---:|---:|---:|
| observed | 15e4deba-51e4-4735-8252-901911a8612e | completed | 0.47 | 12.00s | 5.70s | 1 |
| baseline_replay | 9490007f-03d6-439a-be01-e5a1c10d81bc | completed | 0.47 | 9.00s | 5.70s | 1 |
| candidate_replay | 889b6baa-6298-4b94-beb9-42091d6d5ef8 | completed | 0.28 | 11.00s | 3.20s | 1 |

Limitations:
- observed: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.
- baseline_replay: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.
- candidate_replay: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.
- Observed-to-baseline replay drift is large; candidate effect has lower confidence.

## support-refund-delay--hist-03

Reason: Slow Resolution for enterprise refund delay case from Harbor Retail.

| Lane | Execution | Status | Cost | Duration | Latency | Quality |
|---|---|---|---:|---:|---:|---:|
| observed | 5debdf93-acdb-4628-9871-d6badbd8ed11 | completed | 0.4 | 12.00s | 4.40s | 1 |
| baseline_replay | 7cef3402-91ca-477a-91c7-2e132ffdaa44 | completed | 0.4 | 12.00s | 4.40s | 1 |
| candidate_replay | 9f8879aa-b277-4e96-bc03-1db546f31a7b | completed | 0.24 | 12.00s | 2.50s | 1 |

Limitations:
- observed: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.
- baseline_replay: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.
- candidate_replay: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.

## regulated-medical-claim

Reason: Quality-risk case where a terse answer can be unsafe.

| Lane | Execution | Status | Cost | Duration | Latency | Quality |
|---|---|---|---:|---:|---:|---:|
| observed | eb00758d-10b2-47e9-bc6b-453a83acb70b | completed | 0.48 | 12.00s | 5.40s | 1 |
| baseline_replay | 57242877-9ce2-4568-888f-72afd5bd78a7 | completed | 0.48 | 11.00s | 5.40s | 1 |
| candidate_replay | 5b9c1240-243d-468e-aaa2-82d2819c0604 | completed | 0.23 | 15.00s | 2.40s | 0.88 |

Limitations:
- observed: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.
- baseline_replay: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.
- candidate_replay: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.

## regulated-medical-claim--hist-01

Reason: Cost Outlier for regulated medical claim routing case from Aster Cloud.

| Lane | Execution | Status | Cost | Duration | Latency | Quality |
|---|---|---|---:|---:|---:|---:|
| observed | 63674209-e985-4638-b4cd-10306e83f1ae | completed | 0.5 | 11.00s | 5.80s | 1 |
| baseline_replay | b37c6952-c310-436b-b09b-ec1e1208997f | completed | 0.5 | 11.00s | 5.80s | 1 |
| candidate_replay | 139cdbe0-75de-4a55-83c7-def907721b0a | completed | 0.24 | 10.00s | 2.60s | 0.88 |

Limitations:
- observed: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.
- baseline_replay: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.
- candidate_replay: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.

## regulated-medical-claim--hist-02

Reason: Customer Complaint for regulated medical claim routing case from Northwind Health.

| Lane | Execution | Status | Cost | Duration | Latency | Quality |
|---|---|---|---:|---:|---:|---:|
| observed | 944c31a1-07cc-4503-8591-a10ff6666e5b | completed | 0.54 | 12.00s | 6.40s | 1 |
| baseline_replay | 67f0041a-20f0-4741-93e3-f179f0f8c224 | completed | 0.54 | 9.00s | 6.40s | 1 |
| candidate_replay | 6585eeeb-b6ed-4e03-8546-518ca2246cb0 | completed | 0.26 | 10.00s | 2.80s | 0.88 |

Limitations:
- observed: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.
- baseline_replay: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.
- candidate_replay: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.
- Observed-to-baseline replay drift is large; candidate effect has lower confidence.

## regulated-medical-claim--hist-03

Reason: Slow Resolution for regulated medical claim routing case from Harbor Retail.

| Lane | Execution | Status | Cost | Duration | Latency | Quality |
|---|---|---|---:|---:|---:|---:|
| observed | 6f5a7f64-b319-423c-9cd0-359003623e81 | completed | 0.46 | 12.00s | 5.00s | 1 |
| baseline_replay | a2144a13-c7fc-47ac-9ad8-6e87c1fb0300 | completed | 0.46 | 11.00s | 5.00s | 1 |
| candidate_replay | 8dc395ce-582a-47ec-a8bb-1cc02b43d999 | completed | 0.22 | 11.00s | 2.20s | 0.88 |

Limitations:
- observed: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.
- baseline_replay: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.
- candidate_replay: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.

## shipping-tool-loop

Reason: Tool-loop style case with avoidable cost and latency.

| Lane | Execution | Status | Cost | Duration | Latency | Quality |
|---|---|---|---:|---:|---:|---:|
| observed | 62a3edf5-e0b4-427f-a646-dadc5087f576 | completed | 0.31 | 11.00s | 4.20s | 0.88 |
| baseline_replay | 46882836-9d90-4e89-a617-e40cb42282e5 | completed | 0.31 | 18.00s | 4.20s | 0.88 |
| candidate_replay | n/a | failed | n/a | n/a | n/a | n/a |

Limitations:
- observed: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.
- baseline_replay: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.
- candidate_replay: candidate_replay replay could not be started (KitaruBackendError).
- candidate_replay lane ended with status `failed`.
- Observed-to-baseline replay drift is large; candidate effect has lower confidence.

## shipping-tool-loop--hist-01

Reason: Cost Outlier for standard shipping status loop case from Aster Cloud.

| Lane | Execution | Status | Cost | Duration | Latency | Quality |
|---|---|---|---:|---:|---:|---:|
| observed | c43de6f0-2ee9-4796-9cf5-6a154e9efd2f | completed | 0.32 | 11.00s | 4.50s | 0.88 |
| baseline_replay | 7cc18c2a-7ab6-4cb6-9d67-5fe8fd935598 | completed | 0.32 | 4493.00s | 4.50s | 0.88 |
| candidate_replay | c3ed8983-4967-460e-93fe-f94bc4eecddc | completed | 0.19 | 1975.00s | 2.30s | 0.88 |

Limitations:
- observed: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.
- baseline_replay: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.
- candidate_replay: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.
- Observed-to-baseline replay drift is large; candidate effect has lower confidence.

## shipping-tool-loop--hist-02

Reason: Customer Complaint for standard shipping status loop case from Northwind Health.

| Lane | Execution | Status | Cost | Duration | Latency | Quality |
|---|---|---|---:|---:|---:|---:|
| observed | 01e34631-b49d-496e-aa55-ba840c89b124 | completed | 0.35 | 12.00s | 5.00s | 0.88 |
| baseline_replay | ac62ea82-e5fa-4106-aa88-733d04144483 | completed | 0.35 | 910.00s | 5.00s | 0.88 |
| candidate_replay | 8d493623-e318-4bb4-a58d-9591ff181632 | completed | 0.2 | 911.00s | 2.50s | 0.88 |

Limitations:
- observed: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.
- baseline_replay: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.
- candidate_replay: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.
- Observed-to-baseline replay drift is large; candidate effect has lower confidence.

## shipping-tool-loop--hist-03

Reason: Slow Resolution for standard shipping status loop case from Harbor Retail.

| Lane | Execution | Status | Cost | Duration | Latency | Quality |
|---|---|---|---:|---:|---:|---:|
| observed | bdf6191d-3ddc-47ba-b949-4e13d386cb1a | completed | 0.3 | 12.00s | 3.90s | 0.88 |
| baseline_replay | 57127261-af32-4d06-be34-aacffce37f39 | completed | 0.3 | 7386.00s | 3.90s | 0.88 |
| candidate_replay | n/a | failed | n/a | n/a | n/a | n/a |

Limitations:
- observed: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.
- baseline_replay: Could not read execution logs (KitaruLogRetrievalError: Failed to retrieve runtime logs for source 'step': Files in a local artifact store cannot be accessed from the server.); log-derived metrics are unavailable.
- candidate_replay: candidate_replay replay could not be started (KitaruBackendError).
- candidate_replay lane ended with status `failed`.
- Observed-to-baseline replay drift is large; candidate effect has lower confidence.
