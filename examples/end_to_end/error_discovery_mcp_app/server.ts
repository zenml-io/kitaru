import {
  registerAppResource,
  registerAppTool,
  RESOURCE_MIME_TYPE,
} from "@modelcontextprotocol/ext-apps/server";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type {
  CallToolResult,
  ReadResourceResult,
} from "@modelcontextprotocol/sdk/types.js";
import fs from "node:fs/promises";
import path from "node:path";
import { z } from "zod";

import { ReviewStore, WorkflowError } from "./src/state.js";

const appDirectory = import.meta.filename.endsWith(".ts")
  ? import.meta.dirname
  : path.resolve(import.meta.dirname, "..");
const distDirectory = path.join(appDirectory, "dist");
const resourceUri = "ui://kitaru-error-discovery/mcp-app.html";

const judgmentSchema = z.enum(["acceptable", "problematic", "uncertain"]);
const labelSchema = z.enum(["Pass", "Fail"]);
const hypothesisSchema = z.object({
  id: z.string(),
  title: z.string(),
  definition: z.string(),
  evidenceTraceIds: z.array(z.string()),
  counterexampleTraceIds: z.array(z.string()),
  ambiguity: z.string(),
});
const failureModeSchema = z.object({
  name: z.string(),
  definition: z.string(),
  inScope: z.array(z.object({ traceId: z.string(), why: z.string() })),
  outOfScope: z.array(z.object({ traceId: z.string(), why: z.string() })),
});
const scorerSchema = z.object({
  name: z.string(),
  criterion: z.string(),
  passDefinition: z.string(),
  failDefinition: z.string(),
  demonstrations: z.array(
    z.object({
      traceId: z.string(),
      label: labelSchema,
      why: z.string(),
    }),
  ),
});
const predictionSchema = z.object({
  traceId: z.string(),
  prediction: labelSchema,
  rationale: z.string(),
});

function result(data: object, text: string): CallToolResult {
  return {
    content: [{ type: "text", text }],
    structuredContent: data as Record<string, unknown>,
  };
}

function formatError(error: unknown): never {
  if (error instanceof WorkflowError) {
    throw new Error(`${error.code}: ${error.message}`);
  }
  throw error;
}

export function createServer(store = new ReviewStore()): McpServer {
  const server = new McpServer({
    name: "Kitaru baby-vp error discovery",
    version: "0.1.0",
  });

  server.registerPrompt(
    "error-discovery",
    {
      title: "Run trace-grounded error discovery",
      description:
        "Loads the repository's authoritative error-discovery skill and starts the bounded Act 3 workflow.",
    },
    async () => {
      const skill = await fs.readFile(
        path.resolve(
          appDirectory,
          "../../../.agents/skills/error-discovery/SKILL.md",
        ),
        "utf8",
      );
      return {
        messages: [
          {
            role: "user",
            content: {
              type: "text",
              text: `Follow this repository skill exactly for the current conversation.\n\n${skill}\n\nStart the error-discovery workflow now.`,
            },
          },
        ],
      };
    },
  );

  registerAppTool(
    server,
    "start_error_discovery",
    {
      title: "Start error discovery",
      description:
        "Starts or resumes the bounded 12-trace discovery review and opens its MCP App. Use this first when the user invokes the error-discovery skill.",
      inputSchema: { reset: z.boolean().optional() },
      _meta: { ui: { resourceUri } },
    },
    ({ reset = false }) => {
      try {
        const snapshot = store.start(reset);
        const trace = snapshot.currentTrace;
        return result(
          {
            schemaVersion: "baby-vp.start.v1",
            snapshot,
            reviewGoal:
              "Notice observable failures in full traces before naming a category.",
          },
          [
            `Error-discovery review ${snapshot.sessionId} is ready.`,
            `Start with trace ${trace.id}: ${trace.scenarioTitle}.`,
            `User request: ${trace.userRequest}`,
            `Final response: ${trace.finalResponse}`,
            "Read backward from the final response through its tool calls and results. Record free-text observations in the app. Manual fallback: discuss each observation in chat, then say “I finished this batch.”",
          ].join("\n\n"),
        );
      } catch (error) {
        return formatError(error);
      }
    },
  );

  registerAppTool(
    server,
    "read_review_state",
    {
      title: "Read accumulated review state",
      description:
        "Reads only the human observations, provisional hypotheses, accepted criteria, and current workflow state for one session.",
      inputSchema: { sessionId: z.string() },
      _meta: { ui: { visibility: ["model"] } },
    },
    ({ sessionId }) => {
      try {
        const snapshot = store.snapshot(sessionId);
        return result(
          { schemaVersion: "baby-vp.state.v1", snapshot },
          `${snapshot.progress.reviewed}/${snapshot.progress.total} traces reviewed across ${snapshot.progress.scenarios} scenarios. Next: ${snapshot.nextAction}`,
        );
      } catch (error) {
        return formatError(error);
      }
    },
  );

  registerAppTool(
    server,
    "save_provisional_hypotheses",
    {
      title: "Save provisional failure hypotheses",
      description:
        "Saves one to three explicitly provisional hypotheses synthesized only from human-reviewed traces. Every hypothesis must cite reviewed evidence and a reviewed counterexample.",
      inputSchema: {
        sessionId: z.string(),
        revision: z.number().int().nonnegative(),
        hypotheses: z.array(hypothesisSchema).min(1).max(3),
      },
      _meta: { ui: { visibility: ["model"] } },
    },
    ({ sessionId, revision, hypotheses }) => {
      try {
        const snapshot = store.saveHypotheses(
          sessionId,
          revision,
          hypotheses,
        );
        return result(
          { schemaVersion: "baby-vp.hypotheses.v1", snapshot },
          `Saved ${hypotheses.length} provisional hypothesis or hypotheses. These are agent suggestions, not accepted categories. Revisit the cited traces before asking the human to accept one definition.`,
        );
      } catch (error) {
        return formatError(error);
      }
    },
  );

  registerAppTool(
    server,
    "commit_accepted_failure_mode",
    {
      title: "Commit accepted failure mode",
      description:
        "Commits the exact human-confirmed binary failure-mode draft. It cannot accept an unconfirmed agent suggestion.",
      inputSchema: {
        sessionId: z.string(),
        revision: z.number().int().nonnegative(),
        confirmationToken: z.string(),
      },
      _meta: { ui: { visibility: ["model"] } },
    },
    ({ sessionId, revision, confirmationToken }) => {
      try {
        const snapshot = store.commitFailureMode(
          sessionId,
          revision,
          confirmationToken,
        );
        return result(
          { schemaVersion: "baby-vp.failure-mode.v1", snapshot },
          `Accepted failure mode: ${snapshot.acceptedFailureMode?.name}. Next draft one narrow binary scorer for this mode only.`,
        );
      } catch (error) {
        return formatError(error);
      }
    },
  );

  registerAppTool(
    server,
    "commit_scorer_rubric",
    {
      title: "Commit accepted scorer rubric",
      description:
        "Commits the exact human-confirmed narrow Pass/Fail rubric and its discovery-only demonstrations.",
      inputSchema: {
        sessionId: z.string(),
        revision: z.number().int().nonnegative(),
        confirmationToken: z.string(),
      },
      _meta: { ui: { visibility: ["model"] } },
    },
    ({ sessionId, revision, confirmationToken }) => {
      try {
        const snapshot = store.commitScorer(
          sessionId,
          revision,
          confirmationToken,
        );
        return result(
          { schemaVersion: "baby-vp.scorer.v1", snapshot },
          `Accepted scorer ${snapshot.acceptedScorer?.name} with immutable rubric hash ${snapshot.acceptedScorer?.hash}. Held-out examples have not been exposed.`,
        );
      } catch (error) {
        return formatError(error);
      }
    },
  );

  registerAppTool(
    server,
    "get_unlabeled_heldout_traces",
    {
      title: "Get unlabeled held-out traces",
      description:
        "Returns six held-out trace inputs for the accepted scorer. Gold labels, expected actions, and gold rationales are absent by construction.",
      inputSchema: { sessionId: z.string(), scorerId: z.string() },
      _meta: { ui: { visibility: ["model"] } },
    },
    ({ sessionId, scorerId }) => {
      try {
        const heldout = store.getUnlabeledHeldout(sessionId, scorerId);
        return result(
          heldout,
          [
            `Retrieved ${heldout.traces.length} unlabeled held-out traces for scorer ${scorerId}.`,
            `Rubric hash: ${heldout.scorerHash}.`,
            "Score every trace as Pass or Fail with a short rationale. Do not infer gold labels from variant or expected-action metadata; those fields are not present.",
            ...heldout.traces.map(
              (trace) =>
                `${trace.id} | ${trace.scenarioTitle} | ${trace.userRequest}\nFinal response: ${trace.finalResponse}`,
            ),
          ].join("\n\n"),
        );
      } catch (error) {
        return formatError(error);
      }
    },
  );

  registerAppTool(
    server,
    "record_scorer_run",
    {
      title: "Record held-out scorer run",
      description:
        "Atomically records exactly one Pass/Fail prediction and short rationale for each of the six held-out traces before labels may be revealed.",
      inputSchema: {
        sessionId: z.string(),
        revision: z.number().int().nonnegative(),
        scorerId: z.string(),
        scorerHash: z.string(),
        predictions: z.array(predictionSchema).length(6),
      },
      _meta: { ui: { visibility: ["model"] } },
    },
    ({ sessionId, revision, scorerId, scorerHash, predictions }) => {
      try {
        const recorded = store.recordScorerRun(
          sessionId,
          revision,
          scorerId,
          scorerHash,
          predictions,
        );
        return result(
          {
            schemaVersion: "baby-vp.scorer-run.v1",
            scorerRun: recorded.scorerRun,
            snapshot: recorded.snapshot,
          },
          `Recorded all six predictions as immutable scorer run ${recorded.scorerRun.id}. Human labels are still hidden. Reveal them only now.`,
        );
      } catch (error) {
        return formatError(error);
      }
    },
  );

  registerAppTool(
    server,
    "reveal_validation_results",
    {
      title: "Reveal validation results",
      description:
        "Reveals held-out human labels only for an already recorded immutable scorer run, then identifies false passes and false fails.",
      inputSchema: { sessionId: z.string(), scorerRunId: z.string() },
      _meta: { ui: { visibility: ["model"] } },
    },
    ({ sessionId, scorerRunId }) => {
      try {
        const validation = store.revealValidation(sessionId, scorerRunId);
        const disagreements = validation.rows.filter(
          (row) => row.result !== "agreement",
        );
        return result(
          validation,
          [
            `Labels revealed for scorer run ${scorerRunId}.`,
            `${disagreements.length} disagreement(s): ${disagreements.map((row) => `${row.traceId} ${row.result}`).join(", ") || "none"}.`,
            validation.caveat,
          ].join("\n\n"),
        );
      } catch (error) {
        return formatError(error);
      }
    },
  );

  const appOnlyMeta = { ui: { visibility: ["app"] as const } };

  registerAppTool(
    server,
    "get_review_snapshot",
    {
      title: "Refresh review UI",
      description: "Returns the current UI snapshot. App-only.",
      inputSchema: { sessionId: z.string() },
      _meta: appOnlyMeta,
    },
    ({ sessionId }) => {
      const snapshot = store.snapshot(sessionId);
      return result({ snapshot }, snapshot.nextAction);
    },
  );

  registerAppTool(
    server,
    "load_discovery_trace",
    {
      title: "Load discovery trace",
      description: "Loads one discovery trace in the review UI. App-only.",
      inputSchema: {
        sessionId: z.string(),
        revision: z.number().int().nonnegative(),
        traceId: z.string(),
      },
      _meta: appOnlyMeta,
    },
    ({ sessionId, revision, traceId }) =>
      result(
        { snapshot: store.loadTrace(sessionId, revision, traceId) },
        `Loaded ${traceId}.`,
      ),
  );

  registerAppTool(
    server,
    "upsert_annotation",
    {
      title: "Save editable observation",
      description:
        "Saves or edits one free-text observation and optional holistic judgment. App-only.",
      inputSchema: {
        sessionId: z.string(),
        revision: z.number().int().nonnegative(),
        traceId: z.string(),
        note: z.string(),
        judgment: judgmentSchema.optional(),
      },
      _meta: appOnlyMeta,
    },
    ({ sessionId, revision, traceId, note, judgment }) =>
      result(
        {
          snapshot: store.upsertAnnotation(
            sessionId,
            revision,
            traceId,
            note,
            judgment,
          ),
        },
        `Saved observation for ${traceId}.`,
      ),
  );

  registerAppTool(
    server,
    "delete_annotation",
    {
      title: "Clear observation",
      description: "Clears the editable observation for one trace. App-only.",
      inputSchema: {
        sessionId: z.string(),
        revision: z.number().int().nonnegative(),
        traceId: z.string(),
      },
      _meta: appOnlyMeta,
    },
    ({ sessionId, revision, traceId }) =>
      result(
        { snapshot: store.deleteAnnotation(sessionId, revision, traceId) },
        `Cleared observation for ${traceId}.`,
      ),
  );

  registerAppTool(
    server,
    "suggest_similar_traces",
    {
      title: "Suggest related trace",
      description:
        "Proposes a high-recall related trace for human comparison. The suggestion is provisional. App-only.",
      inputSchema: {
        sessionId: z.string(),
        revision: z.number().int().nonnegative(),
        traceId: z.string(),
      },
      _meta: appOnlyMeta,
    },
    ({ sessionId, revision, traceId }) =>
      result(
        { snapshot: store.suggestSimilar(sessionId, revision, traceId) },
        "Added one provisional related-trace suggestion.",
      ),
  );

  registerAppTool(
    server,
    "review_suggestion",
    {
      title: "Review trace suggestion",
      description:
        "Lets the human compare or dismiss a provisional trace suggestion. App-only.",
      inputSchema: {
        sessionId: z.string(),
        revision: z.number().int().nonnegative(),
        suggestionId: z.string(),
        decision: z.enum(["compare", "dismissed"]),
      },
      _meta: appOnlyMeta,
    },
    ({ sessionId, revision, suggestionId, decision }) =>
      result(
        {
          snapshot: store.reviewSuggestion(
            sessionId,
            revision,
            suggestionId,
            decision,
          ),
        },
        `Suggestion ${decision}.`,
      ),
  );

  registerAppTool(
    server,
    "set_comparison_trace",
    {
      title: "Set comparison trace",
      description: "Opens or closes pairwise comparison. App-only.",
      inputSchema: {
        sessionId: z.string(),
        revision: z.number().int().nonnegative(),
        traceId: z.string().optional(),
      },
      _meta: appOnlyMeta,
    },
    ({ sessionId, revision, traceId }) =>
      result(
        { snapshot: store.setComparison(sessionId, revision, traceId) },
        traceId ? `Comparing with ${traceId}.` : "Closed comparison.",
      ),
  );

  registerAppTool(
    server,
    "mark_batch_reviewed",
    {
      title: "Finish review batch",
      description:
        "Marks a sufficiently broad human review batch ready for model synthesis. App-only.",
      inputSchema: {
        sessionId: z.string(),
        revision: z.number().int().nonnegative(),
      },
      _meta: appOnlyMeta,
    },
    ({ sessionId, revision }) =>
      result(
        { snapshot: store.markBatchReviewed(sessionId, revision) },
        "Human review batch is ready for synthesis.",
      ),
  );

  registerAppTool(
    server,
    "confirm_hypothesis_example",
    {
      title: "Confirm re-reviewed example",
      description:
        "Marks a cited trace as re-reviewed against the provisional criterion. App-only.",
      inputSchema: {
        sessionId: z.string(),
        revision: z.number().int().nonnegative(),
        traceId: z.string(),
      },
      _meta: appOnlyMeta,
    },
    ({ sessionId, revision, traceId }) =>
      result(
        {
          snapshot: store.confirmHypothesisExample(
            sessionId,
            revision,
            traceId,
          ),
        },
        `Confirmed re-review of ${traceId}.`,
      ),
  );

  registerAppTool(
    server,
    "confirm_failure_mode_draft",
    {
      title: "Confirm failure-mode draft",
      description:
        "Records a human-confirmed exact failure-mode draft and returns a one-use commit token. App-only.",
      inputSchema: {
        sessionId: z.string(),
        revision: z.number().int().nonnegative(),
        draft: failureModeSchema,
      },
      _meta: appOnlyMeta,
    },
    ({ sessionId, revision, draft }) => {
      const confirmed = store.confirmFailureMode(sessionId, revision, draft);
      return result(
        {
          snapshot: confirmed.snapshot,
          confirmationToken: confirmed.confirmationToken,
          kind: "failure-mode",
        },
        `Human confirmed the exact failure-mode draft. Commit token: ${confirmed.confirmationToken}.`,
      );
    },
  );

  registerAppTool(
    server,
    "confirm_scorer_rubric_draft",
    {
      title: "Confirm scorer rubric draft",
      description:
        "Records a human-confirmed exact binary scorer rubric and returns a one-use commit token. App-only.",
      inputSchema: {
        sessionId: z.string(),
        revision: z.number().int().nonnegative(),
        draft: scorerSchema,
      },
      _meta: appOnlyMeta,
    },
    ({ sessionId, revision, draft }) => {
      const confirmed = store.confirmScorer(sessionId, revision, draft);
      return result(
        {
          snapshot: confirmed.snapshot,
          confirmationToken: confirmed.confirmationToken,
          kind: "scorer",
        },
        `Human confirmed the exact scorer rubric. Commit token: ${confirmed.confirmationToken}.`,
      );
    },
  );

  registerAppResource(
    server,
    resourceUri,
    resourceUri,
    {
      mimeType: RESOURCE_MIME_TYPE,
      description:
        "Responsive trace review UI for Kitaru's bounded error-discovery prototype.",
    },
    async (): Promise<ReadResourceResult> => {
      const html = await fs.readFile(
        path.join(distDirectory, "mcp-app.html"),
        "utf8",
      );
      return {
        contents: [
          {
            uri: resourceUri,
            mimeType: RESOURCE_MIME_TYPE,
            text: html,
          },
        ],
      };
    },
  );

  return server;
}

export { resourceUri };
