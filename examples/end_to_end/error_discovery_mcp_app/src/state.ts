import { createHash, randomUUID } from "node:crypto";

import {
  discoveryById,
  discoveryTraces,
  heldoutById,
  heldoutTraces,
  privateLabelsByFamily,
} from "./fixtures.js";
import type {
  AcceptedFailureMode,
  AcceptedScorerRubric,
  Annotation,
  BinaryLabel,
  FailureModeDraft,
  Hypothesis,
  Judgment,
  PendingConfirmation,
  Prediction,
  PrivateHeldoutLabel,
  ProvisionalSuggestion,
  PublicTrace,
  ReviewSession,
  ReviewSnapshot,
  ScorerRubricDraft,
  ScorerRun,
  ValidationRow,
} from "./types.js";

const CONFIRMATION_TTL_MS = 10 * 60 * 1000;
const MINIMUM_NOTES = 4;
const MINIMUM_SCENARIOS = 3;

export class WorkflowError extends Error {
  constructor(
    readonly code: string,
    message: string,
  ) {
    super(message);
  }
}

function hash(value: unknown): string {
  return createHash("sha256")
    .update(JSON.stringify(value))
    .digest("hex")
    .slice(0, 16);
}

function assertText(value: string, field: string, maximum = 2_000): string {
  const normalized = value.trim();
  if (!normalized) {
    throw new WorkflowError("INVALID_DRAFT", `${field} must not be empty.`);
  }
  if (normalized.length > maximum) {
    throw new WorkflowError(
      "INVALID_DRAFT",
      `${field} must be at most ${maximum} characters.`,
    );
  }
  return normalized;
}

function getDiscoveryTrace(traceId: string): PublicTrace {
  const trace = discoveryById.get(traceId);
  if (!trace) {
    throw new WorkflowError(
      "INVALID_TRACE",
      `Trace ${traceId} is not in the discovery set.`,
    );
  }
  return trace;
}

function clone<T>(value: T): T {
  return structuredClone(value);
}

export class ReviewStore {
  private sessions = new Map<string, ReviewSession>();
  private activeSessionId?: string;

  start(reset = false): ReviewSnapshot {
    if (this.activeSessionId && !reset) {
      return this.snapshot(this.activeSessionId);
    }

    const id = randomUUID();
    const now = new Date().toISOString();
    const session: ReviewSession = {
      id,
      revision: 0,
      phase: "discovery",
      createdAt: now,
      discoveryOrder: discoveryTraces.map((trace) => trace.id),
      currentTraceId: discoveryTraces[0]!.id,
      annotations: {},
      suggestions: {},
      hypotheses: [],
      reReviewRequired: [],
    };
    this.sessions.set(id, session);
    this.activeSessionId = id;
    return this.snapshot(id);
  }

  snapshot(sessionId: string): ReviewSnapshot {
    const session = this.getSession(sessionId);
    const reviewed = Object.values(session.annotations).filter(
      (annotation) => annotation.note.trim().length > 0,
    );
    const scenarioCount = new Set(
      reviewed.map((annotation) => getDiscoveryTrace(annotation.traceId).scenarioId),
    ).size;
    const currentTrace = getDiscoveryTrace(session.currentTraceId);
    const comparisonTrace = session.comparisonTraceId
      ? getDiscoveryTrace(session.comparisonTraceId)
      : undefined;

    return {
      schemaVersion: "baby-vp.review.v1",
      sessionId: session.id,
      revision: session.revision,
      phase: session.phase,
      progress: {
        reviewed: reviewed.length,
        total: discoveryTraces.length,
        scenarios: scenarioCount,
        minimumReached:
          reviewed.length >= MINIMUM_NOTES && scenarioCount >= MINIMUM_SCENARIOS,
      },
      queue: session.discoveryOrder.map((traceId) => {
        const trace = getDiscoveryTrace(traceId);
        const annotation = session.annotations[traceId];
        return {
          traceId,
          scenarioTitle: trace.scenarioTitle,
          configuration: trace.configuration,
          reviewed: Boolean(annotation?.note.trim()),
          judgment: annotation?.judgment,
        };
      }),
      currentTrace: clone(currentTrace),
      comparisonTrace: comparisonTrace ? clone(comparisonTrace) : undefined,
      annotations: clone(Object.values(session.annotations)),
      suggestions: clone(Object.values(session.suggestions)),
      hypotheses: clone(session.hypotheses),
      reReviewRequired: [...session.reReviewRequired],
      acceptedFailureMode: session.acceptedFailureMode
        ? clone(session.acceptedFailureMode)
        : undefined,
      acceptedScorer: session.acceptedScorer
        ? clone(session.acceptedScorer)
        : undefined,
      validationRows:
        session.phase === "revealed" && session.validationRows
          ? clone(session.validationRows)
          : undefined,
      nextAction: this.nextAction(session),
    };
  }

  loadTrace(
    sessionId: string,
    revision: number,
    traceId: string,
  ): ReviewSnapshot {
    const session = this.getMutableSession(sessionId, revision);
    getDiscoveryTrace(traceId);
    session.currentTraceId = traceId;
    this.advance(session);
    return this.snapshot(sessionId);
  }

  upsertAnnotation(
    sessionId: string,
    revision: number,
    traceId: string,
    note: string,
    judgment?: Judgment,
  ): ReviewSnapshot {
    const session = this.getMutableSession(sessionId, revision);
    this.assertPhase(session, ["discovery", "hypotheses"]);
    getDiscoveryTrace(traceId);
    const normalized = assertText(note, "Observation");
    session.annotations[traceId] = {
      traceId,
      note: normalized,
      judgment,
      updatedAt: new Date().toISOString(),
    };
    session.reReviewRequired = session.reReviewRequired.filter(
      (id) => id !== traceId,
    );
    this.advance(session);
    return this.snapshot(sessionId);
  }

  deleteAnnotation(
    sessionId: string,
    revision: number,
    traceId: string,
  ): ReviewSnapshot {
    const session = this.getMutableSession(sessionId, revision);
    this.assertPhase(session, ["discovery", "hypotheses"]);
    getDiscoveryTrace(traceId);
    delete session.annotations[traceId];
    this.advance(session);
    return this.snapshot(sessionId);
  }

  suggestSimilar(
    sessionId: string,
    revision: number,
    sourceTraceId: string,
  ): ReviewSnapshot {
    const session = this.getMutableSession(sessionId, revision);
    const source = getDiscoveryTrace(sourceTraceId);
    const candidate =
      discoveryTraces.find(
        (trace) =>
          trace.id !== source.id &&
          trace.scenarioId === source.scenarioId &&
          !Object.values(session.suggestions).some(
            (suggestion) =>
              suggestion.sourceTraceId === source.id &&
              suggestion.candidateTraceId === trace.id,
          ),
      ) ??
      discoveryTraces.find(
        (trace) =>
          trace.id !== source.id &&
          trace.outcome.policyLabel === source.outcome.policyLabel,
      );
    if (!candidate) {
      throw new WorkflowError(
        "NO_SUGGESTION",
        "No related discovery trace is available.",
      );
    }

    const suggestion: ProvisionalSuggestion = {
      id: randomUUID(),
      sourceTraceId,
      candidateTraceId: candidate.id,
      reason:
        candidate.scenarioId === source.scenarioId
          ? "Same request, different agent configuration."
          : "Different request with the same observed policy family.",
      status: "provisional",
    };
    session.suggestions[suggestion.id] = suggestion;
    this.advance(session);
    return this.snapshot(sessionId);
  }

  reviewSuggestion(
    sessionId: string,
    revision: number,
    suggestionId: string,
    decision: "compare" | "dismissed",
  ): ReviewSnapshot {
    const session = this.getMutableSession(sessionId, revision);
    const suggestion = session.suggestions[suggestionId];
    if (!suggestion) {
      throw new WorkflowError("INVALID_SUGGESTION", "Suggestion not found.");
    }
    suggestion.status = decision;
    if (decision === "compare") {
      session.comparisonTraceId = suggestion.candidateTraceId;
    }
    this.advance(session);
    return this.snapshot(sessionId);
  }

  setComparison(
    sessionId: string,
    revision: number,
    traceId?: string,
  ): ReviewSnapshot {
    const session = this.getMutableSession(sessionId, revision);
    if (traceId) {
      getDiscoveryTrace(traceId);
    }
    session.comparisonTraceId = traceId;
    this.advance(session);
    return this.snapshot(sessionId);
  }

  markBatchReviewed(sessionId: string, revision: number): ReviewSnapshot {
    const session = this.getMutableSession(sessionId, revision);
    this.assertPhase(session, ["discovery", "hypotheses"]);
    this.assertMinimumReview(session);
    session.batchReviewedAt = new Date().toISOString();
    this.advance(session);
    return this.snapshot(sessionId);
  }

  saveHypotheses(
    sessionId: string,
    revision: number,
    hypotheses: Hypothesis[],
  ): ReviewSnapshot {
    const session = this.getMutableSession(sessionId, revision);
    this.assertPhase(session, ["discovery", "hypotheses"]);
    this.assertMinimumReview(session);
    if (!session.batchReviewedAt) {
      throw new WorkflowError(
        "BATCH_NOT_FINISHED",
        "The human must finish the current review batch before synthesis.",
      );
    }
    if (hypotheses.length < 1 || hypotheses.length > 3) {
      throw new WorkflowError(
        "INVALID_HYPOTHESES",
        "Save between one and three trace-grounded hypotheses.",
      );
    }

    const normalized = hypotheses.map((hypothesis, index) => {
      const evidenceTraceIds = this.assertReviewedTraceIds(
        session,
        hypothesis.evidenceTraceIds,
        "evidence",
      );
      const counterexampleTraceIds = this.assertReviewedTraceIds(
        session,
        hypothesis.counterexampleTraceIds,
        "counterexample",
      );
      if (evidenceTraceIds.length === 0 || counterexampleTraceIds.length === 0) {
        throw new WorkflowError(
          "INVALID_HYPOTHESES",
          "Every hypothesis needs reviewed evidence and a reviewed counterexample.",
        );
      }
      return {
        id: hypothesis.id || `hypothesis-${index + 1}`,
        title: assertText(hypothesis.title, "Hypothesis title", 120),
        definition: assertText(hypothesis.definition, "Hypothesis definition"),
        evidenceTraceIds,
        counterexampleTraceIds,
        ambiguity: assertText(hypothesis.ambiguity, "Remaining ambiguity", 600),
      };
    });

    session.hypotheses = normalized;
    session.phase = "hypotheses";
    session.reReviewRequired = [
      ...new Set(
        normalized.flatMap((hypothesis) => [
          ...hypothesis.evidenceTraceIds,
          ...hypothesis.counterexampleTraceIds,
        ]),
      ),
    ];
    this.advance(session);
    return this.snapshot(sessionId);
  }

  confirmHypothesisExample(
    sessionId: string,
    revision: number,
    traceId: string,
  ): ReviewSnapshot {
    const session = this.getMutableSession(sessionId, revision);
    this.assertPhase(session, ["hypotheses"]);
    if (session.hypotheses.length === 0) {
      throw new WorkflowError(
        "NO_HYPOTHESES",
        "There are no hypotheses to confirm.",
      );
    }
    if (!session.reReviewRequired.includes(traceId)) {
      throw new WorkflowError(
        "NOT_PENDING_REVIEW",
        "This trace is not waiting for hypothesis re-review.",
      );
    }
    session.reReviewRequired = session.reReviewRequired.filter(
      (id) => id !== traceId,
    );
    this.advance(session);
    return this.snapshot(sessionId);
  }

  confirmFailureMode(
    sessionId: string,
    revision: number,
    draft: FailureModeDraft,
  ): { snapshot: ReviewSnapshot; confirmationToken: string } {
    const session = this.getMutableSession(sessionId, revision);
    this.assertPhase(session, ["hypotheses"]);
    if (session.hypotheses.length === 0) {
      throw new WorkflowError(
        "NO_HYPOTHESES",
        "Synthesize trace-grounded hypotheses before defining a failure mode.",
      );
    }
    const normalized = this.normalizeFailureMode(session, draft);
    const token = randomUUID();
    session.pendingFailureMode = this.pending(token, normalized);
    this.advance(session);
    return { snapshot: this.snapshot(sessionId), confirmationToken: token };
  }

  commitFailureMode(
    sessionId: string,
    revision: number,
    confirmationToken: string,
  ): ReviewSnapshot {
    const session = this.getMutableSession(sessionId, revision);
    this.assertPhase(session, ["hypotheses"]);
    const pending = this.consumePending(
      session.pendingFailureMode,
      confirmationToken,
      "failure-mode",
    );
    const accepted: AcceptedFailureMode = {
      ...pending.draft,
      id: randomUUID(),
      hash: pending.hash,
      acceptedAt: new Date().toISOString(),
    };
    session.acceptedFailureMode = accepted;
    session.pendingFailureMode = undefined;
    session.phase = "definition";
    this.advance(session);
    return this.snapshot(sessionId);
  }

  confirmScorer(
    sessionId: string,
    revision: number,
    draft: ScorerRubricDraft,
  ): { snapshot: ReviewSnapshot; confirmationToken: string } {
    const session = this.getMutableSession(sessionId, revision);
    this.assertPhase(session, ["definition"]);
    if (!session.acceptedFailureMode) {
      throw new WorkflowError(
        "NO_ACCEPTED_FAILURE_MODE",
        "Accept a failure mode before drafting its scorer.",
      );
    }
    const normalized = this.normalizeScorer(session, draft);
    const token = randomUUID();
    session.pendingScorer = this.pending(token, normalized);
    this.advance(session);
    return { snapshot: this.snapshot(sessionId), confirmationToken: token };
  }

  commitScorer(
    sessionId: string,
    revision: number,
    confirmationToken: string,
  ): ReviewSnapshot {
    const session = this.getMutableSession(sessionId, revision);
    this.assertPhase(session, ["definition"]);
    const pending = this.consumePending(
      session.pendingScorer,
      confirmationToken,
      "scorer",
    );
    const accepted: AcceptedScorerRubric = {
      ...pending.draft,
      id: randomUUID(),
      failureModeId: session.acceptedFailureMode!.id,
      failureModeHash: session.acceptedFailureMode!.hash,
      hash: pending.hash,
      acceptedAt: new Date().toISOString(),
    };
    session.acceptedScorer = accepted;
    session.pendingScorer = undefined;
    session.phase = "rubric";
    this.advance(session);
    return this.snapshot(sessionId);
  }

  getUnlabeledHeldout(
    sessionId: string,
    scorerId: string,
  ): {
    schemaVersion: "baby-vp.heldout.v1";
    sessionId: string;
    scorerId: string;
    scorerHash: string;
    traces: PublicTrace[];
    predictionContract: string;
  } {
    const session = this.getSession(sessionId);
    this.assertPhase(session, ["rubric", "validation"]);
    if (!session.acceptedScorer || session.acceptedScorer.id !== scorerId) {
      throw new WorkflowError(
        "INVALID_SCORER",
        "The scorer is not the accepted scorer for this session.",
      );
    }
    const failureMode = session.acceptedFailureMode;
    if (
      !failureMode ||
      session.acceptedScorer.failureModeId !== failureMode.id ||
      session.acceptedScorer.failureModeHash !== failureMode.hash
    ) {
      throw new WorkflowError(
        "ARTIFACT_MISMATCH",
        "The accepted scorer is not bound to the accepted failure mode.",
      );
    }
    const binding = {
      scorerId: session.acceptedScorer.id,
      scorerHash: session.acceptedScorer.hash,
      failureModeHash: failureMode.hash,
      family: this.getFixtureFamily(failureMode),
      retrievedAt: new Date().toISOString(),
    };
    if (session.heldoutBinding) {
      if (
        session.heldoutBinding.scorerId !== binding.scorerId ||
        session.heldoutBinding.scorerHash !== binding.scorerHash ||
        session.heldoutBinding.failureModeHash !== binding.failureModeHash ||
        session.heldoutBinding.family !== binding.family
      ) {
        throw new WorkflowError(
          "HELDOUT_BINDING_MISMATCH",
          "Held-out inputs are already bound to a different accepted scorer.",
        );
      }
    } else {
      session.heldoutBinding = binding;
    }
    if (session.phase === "rubric") {
      session.phase = "validation";
      this.advance(session);
    }
    return {
      schemaVersion: "baby-vp.heldout.v1",
      sessionId,
      scorerId,
      scorerHash: session.acceptedScorer.hash,
      traces: clone(heldoutTraces),
      predictionContract:
        "Return exactly one Pass or Fail prediction and a short trace-grounded rationale for each trace. Gold labels are not present.",
    };
  }

  recordScorerRun(
    sessionId: string,
    revision: number,
    scorerId: string,
    scorerHash: string,
    predictions: Prediction[],
  ): { scorerRun: ScorerRun; snapshot: ReviewSnapshot } {
    const session = this.getMutableSession(sessionId, revision);
    this.assertPhase(session, ["validation"]);
    const scorer = session.acceptedScorer;
    if (!scorer || scorer.id !== scorerId || scorer.hash !== scorerHash) {
      throw new WorkflowError(
        "SCORER_MISMATCH",
        "The scorer ID or immutable rubric hash does not match.",
      );
    }
    const failureMode = session.acceptedFailureMode;
    const binding = session.heldoutBinding;
    if (
      !failureMode ||
      !binding ||
      binding.scorerId !== scorer.id ||
      binding.scorerHash !== scorer.hash ||
      binding.failureModeHash !== failureMode.hash ||
      scorer.failureModeId !== failureMode.id ||
      scorer.failureModeHash !== failureMode.hash
    ) {
      throw new WorkflowError(
        "HELDOUT_NOT_RETRIEVED",
        "Retrieve held-out inputs bound to this exact failure mode and scorer before recording predictions.",
      );
    }
    const normalized = this.normalizePredictions(predictions);
    if (session.scorerRun) {
      if (JSON.stringify(session.scorerRun.predictions) === JSON.stringify(normalized)) {
        return {
          scorerRun: clone(session.scorerRun),
          snapshot: this.snapshot(sessionId),
        };
      }
      throw new WorkflowError(
        "RUN_ALREADY_RECORDED",
        "A different immutable scorer run is already recorded.",
      );
    }

    const scorerRun: ScorerRun = {
      id: randomUUID(),
      scorerId,
      scorerHash,
      failureModeHash: failureMode.hash,
      predictions: normalized,
      recordedAt: new Date().toISOString(),
    };
    session.scorerRun = scorerRun;
    session.phase = "validation";
    this.advance(session);
    return { scorerRun: clone(scorerRun), snapshot: this.snapshot(sessionId) };
  }

  revealValidation(
    sessionId: string,
    scorerRunId: string,
  ): {
    schemaVersion: "baby-vp.validation.v1";
    scorerRunId: string;
    rows: ValidationRow[];
    caveat: string;
    snapshot: ReviewSnapshot;
  } {
    const session = this.getSession(sessionId);
    if (!session.scorerRun || session.scorerRun.id !== scorerRunId) {
      throw new WorkflowError(
        "PREDICTIONS_REQUIRED",
        "Record the complete scorer run before revealing human labels.",
      );
    }
    this.assertPhase(session, ["validation", "revealed"]);
    if (!session.validationRows) {
      const privateLabels = session.heldoutBinding
        ? privateLabelsByFamily.get(session.heldoutBinding.family)
        : undefined;
      if (!privateLabels) {
        throw new WorkflowError(
          "FIXTURE_INTEGRITY",
          "No private label family was selected before scoring.",
        );
      }
      session.validationRows = session.scorerRun.predictions.map((prediction) => {
        const gold = privateLabels.get(prediction.traceId);
        if (!gold) {
          throw new WorkflowError(
            "FIXTURE_INTEGRITY",
            `No private label exists for ${prediction.traceId}.`,
          );
        }
        const result: ValidationRow["result"] =
          prediction.prediction === gold.label
            ? "agreement"
            : prediction.prediction === "Pass"
              ? "false-pass"
              : "false-fail";
        return {
          ...prediction,
          humanLabel: gold.label,
          humanRationale: gold.rationale,
          result,
        };
      });
      session.phase = "revealed";
      this.advance(session);
    }

    return {
      schemaVersion: "baby-vp.validation.v1",
      scorerRunId,
      rows: clone(session.validationRows),
      caveat:
        "Six deliberately selected traces are a rubric debugging check, not evidence of production accuracy or generalization.",
      snapshot: this.snapshot(sessionId),
    };
  }

  private getSession(sessionId: string): ReviewSession {
    const session = this.sessions.get(sessionId);
    if (!session) {
      throw new WorkflowError(
        "SESSION_EXPIRED",
        "This in-memory review session is unavailable. Start a new review.",
      );
    }
    return session;
  }

  private getMutableSession(
    sessionId: string,
    revision: number,
  ): ReviewSession {
    const session = this.getSession(sessionId);
    if (session.revision !== revision) {
      throw new WorkflowError(
        "REVISION_CONFLICT",
        `Expected revision ${session.revision}, received ${revision}. Refresh the review snapshot.`,
      );
    }
    return session;
  }

  private advance(session: ReviewSession): void {
    session.revision += 1;
  }

  private assertPhase(
    session: ReviewSession,
    allowed: ReviewSession["phase"][],
  ): void {
    if (!allowed.includes(session.phase)) {
      throw new WorkflowError(
        "INVALID_PHASE",
        `This action requires phase ${allowed.join(" or ")}; the session is in ${session.phase}. Start a new session to revise an accepted upstream artifact.`,
      );
    }
  }

  private assertMinimumReview(session: ReviewSession): void {
    const annotations = Object.values(session.annotations).filter(
      (annotation) => annotation.note.trim().length > 0,
    );
    const scenarios = new Set(
      annotations.map((annotation) => getDiscoveryTrace(annotation.traceId).scenarioId),
    );
    if (
      annotations.length < MINIMUM_NOTES ||
      scenarios.size < MINIMUM_SCENARIOS
    ) {
      throw new WorkflowError(
        "REVIEW_TOO_NARROW",
        `Open-code at least ${MINIMUM_NOTES} traces across ${MINIMUM_SCENARIOS} scenarios before synthesis.`,
      );
    }
  }

  private assertReviewedTraceIds(
    session: ReviewSession,
    traceIds: string[],
    role: string,
  ): string[] {
    const unique = [...new Set(traceIds)];
    for (const traceId of unique) {
      getDiscoveryTrace(traceId);
      if (!session.annotations[traceId]?.note.trim()) {
        throw new WorkflowError(
          "UNREVIEWED_EVIDENCE",
          `${role} trace ${traceId} has no human observation.`,
        );
      }
    }
    return unique;
  }

  private normalizeFailureMode(
    session: ReviewSession,
    draft: FailureModeDraft,
  ): FailureModeDraft {
    if (draft.inScope.length < 1 || draft.outOfScope.length < 1) {
      throw new WorkflowError(
        "INVALID_DRAFT",
        "The definition needs at least one in-scope and one out-of-scope example.",
      );
    }
    if (draft.inScope.length > 4 || draft.outOfScope.length > 4) {
      throw new WorkflowError(
        "INVALID_DRAFT",
        "Use at most four in-scope and four out-of-scope examples.",
      );
    }
    const normalizeExamples = (
      examples: Array<{ traceId: string; why: string }>,
    ) =>
      examples.map((example) => {
        this.assertReviewedTraceIds(session, [example.traceId], "definition");
        if (session.reReviewRequired.includes(example.traceId)) {
          throw new WorkflowError(
            "REVIEW_CONFIRMATION_REQUIRED",
            `Re-review trace ${example.traceId} against the proposed criterion first.`,
          );
        }
        return {
          traceId: example.traceId,
          why: assertText(example.why, "Example explanation", 500),
        };
      });

    const inScope = normalizeExamples(draft.inScope);
    const outOfScope = normalizeExamples(draft.outOfScope);
    const inScenarios = new Set(
      inScope.map((example) => getDiscoveryTrace(example.traceId).scenarioId),
    );
    if (
      !outOfScope.some(
        (example) => !inScenarios.has(getDiscoveryTrace(example.traceId).scenarioId),
      )
    ) {
      throw new WorkflowError(
        "COUNTEREXAMPLE_TOO_SIMILAR",
        "Include one reviewed out-of-scope example from a different scenario before acceptance.",
      );
    }
    return {
      name: assertText(draft.name, "Failure-mode name", 120),
      definition: assertText(draft.definition, "Failure-mode definition"),
      inScope,
      outOfScope,
    };
  }

  private normalizeScorer(
    session: ReviewSession,
    draft: ScorerRubricDraft,
  ): ScorerRubricDraft {
    if (draft.demonstrations.length < 2 || draft.demonstrations.length > 4) {
      throw new WorkflowError(
        "INVALID_SCORER",
        "Use two to four human-reviewed discovery examples.",
      );
    }
    const labels = new Set<BinaryLabel>();
    const seen = new Set<string>();
    const demonstrations = draft.demonstrations.map((example) => {
      this.assertReviewedTraceIds(session, [example.traceId], "demonstration");
      if (seen.has(example.traceId)) {
        throw new WorkflowError(
          "INVALID_SCORER",
          "Each demonstration trace must be unique.",
        );
      }
      seen.add(example.traceId);
      labels.add(example.label);
      return {
        traceId: example.traceId,
        label: example.label,
        why: assertText(example.why, "Demonstration explanation", 500),
      };
    });
    if (!labels.has("Pass") || !labels.has("Fail")) {
      throw new WorkflowError(
        "INVALID_SCORER",
        "The demonstrations need at least one Pass and one Fail.",
      );
    }
    return {
      name: assertText(draft.name, "Scorer name", 120),
      criterion: assertText(draft.criterion, "Observable criterion"),
      passDefinition: assertText(draft.passDefinition, "Pass definition"),
      failDefinition: assertText(draft.failDefinition, "Fail definition"),
      demonstrations,
    };
  }

  private normalizePredictions(predictions: Prediction[]): Prediction[] {
    if (predictions.length !== heldoutTraces.length) {
      throw new WorkflowError(
        "INCOMPLETE_RUN",
        `Record exactly ${heldoutTraces.length} held-out predictions atomically.`,
      );
    }
    const byId = new Map<string, Prediction>();
    for (const prediction of predictions) {
      if (!heldoutById.has(prediction.traceId) || byId.has(prediction.traceId)) {
        throw new WorkflowError(
          "INVALID_PREDICTIONS",
          "Predictions must contain each held-out trace exactly once.",
        );
      }
      byId.set(prediction.traceId, {
        traceId: prediction.traceId,
        prediction: prediction.prediction,
        rationale: assertText(
          prediction.rationale,
          "Prediction rationale",
          500,
        ),
      });
    }
    return heldoutTraces.map((trace) => byId.get(trace.id)!);
  }

  private pending<T>(token: string, draft: T): PendingConfirmation<T> {
    return {
      token,
      draft,
      hash: hash(draft),
      expiresAt: Date.now() + CONFIRMATION_TTL_MS,
    };
  }

  private consumePending<T>(
    pending: PendingConfirmation<T> | undefined,
    token: string,
    kind: string,
  ): PendingConfirmation<T> {
    if (!pending || pending.token !== token || pending.expiresAt < Date.now()) {
      throw new WorkflowError(
        "INVALID_CONFIRMATION",
        `The ${kind} confirmation is missing, stale, or does not match the human-confirmed draft.`,
      );
    }
    return pending;
  }

  private getFixtureFamily(
    mode: AcceptedFailureMode,
  ): PrivateHeldoutLabel["family"] {
    const text = `${mode.name} ${mode.definition}`.toLowerCase();
    if (
      /(permission|authori[sz]|restricted|human review|human approval)/.test(text)
    ) {
      return "permission-boundary";
    }
    if (/(evidence|tool budget|insufficient|unsupported|overconfident)/.test(text)) {
      return "insufficient-evidence";
    }
    throw new WorkflowError(
      "NO_MATCHING_HELDOUT_FIXTURE",
      "This baby-vp has blinded held-out labels only for permission-boundary and insufficient-evidence failures. Keep the accepted mode, but validate it with a suitable future fixture.",
    );
  }

  private nextAction(session: ReviewSession): string {
    if (session.phase === "revealed") {
      return "Inspect false passes and false fails. Question the rubric, examples, or human label before changing the scorer.";
    }
    if (session.scorerRun) {
      return "Reveal the held-out human labels for the recorded scorer run.";
    }
    if (session.heldoutBinding) {
      return "Apply the accepted rubric to all six unlabeled traces, then record one atomic prediction run.";
    }
    if (session.acceptedScorer) {
      return "Retrieve the six unlabeled held-out traces.";
    }
    if (session.acceptedFailureMode) {
      return "Draft one narrow binary scorer with explicit Pass and Fail definitions and two to four reviewed examples.";
    }
    if (session.pendingFailureMode) {
      return "Commit the exact human-confirmed failure-mode draft using its one-use confirmation token.";
    }
    if (session.hypotheses.length > 0) {
      return session.reReviewRequired.length > 0
        ? "Revisit cited traces against the provisional criterion, then accept or revise one definition."
        : "Accept or revise one binary failure-mode definition with in-scope and out-of-scope examples.";
    }
    if (session.batchReviewedAt) {
      return "Synthesize one to three provisional, trace-grounded hypotheses from reviewed observations only.";
    }
    return "Open-code varied traces. Read the final answer first, then trace backward through grouped tool calls and results.";
  }
}
