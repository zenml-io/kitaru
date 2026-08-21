// The Python driver injects these exact strings as the task input and replay
// override, and the deterministic model asserts that they reached the model.
export const BASELINE_PROMPT =
  "Investigate account acct-1001 and delayed order ord-1001. The customer reports a suspected duplicate charge.";
export const REPLAY_PROMPT =
  "Priority escalation: investigate account acct-1001 and order ord-1001. Confirm the delayed order and suspected duplicate charge from tool evidence.";
export const REPLAY_INSTRUCTIONS =
  "Follow the configured support workflow. Use the account and order lookup tools and queue one refund review for a delayed duplicate charge. Answer with a JSON object only, using exactly the keys decision, evidence, risk, and nextAction, and record the queued refund review under evidence.";
