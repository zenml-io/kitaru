import type { ScorerRun } from "@mastra/core/evals";
import {
  type Evaluator,
  type EvaluatorParams,
  type SessionView,
  validateEvaluationResults,
} from "@zenml-io/kitaru/evaluator";

/** The native scorer operation required by the evaluator bridge. */
export interface RunnableMastraScorer<TInput, TOutput> {
  run(
    input: ScorerRun<TInput, TOutput>,
  ): Promise<{ score: number; reason?: string }>;
}

export interface MastraEvaluatorOptions<TInput, TOutput> {
  /** Build or select scorers using evaluator-version and per-run parameters. */
  scorers: (
    params: EvaluatorParams,
  ) =>
    | Record<string, RunnableMastraScorer<TInput, TOutput>>
    | Promise<Record<string, RunnableMastraScorer<TInput, TOutput>>>;
  /**
   * Map the full session to the scorer's native input and output. Include the
   * conversation history and tool records relevant to the scoring contract.
   * No message format, turn boundary, or final-answer selection is inferred.
   */
  mapInput: (
    session: SessionView,
    params: EvaluatorParams,
  ) => ScorerRun<TInput, TOutput> | Promise<ScorerRun<TInput, TOutput>>;
}

/** Run native Mastra scorers and use each record key as its Kitaru result name. */
export function createMastraEvaluator<TInput, TOutput>(
  options: MastraEvaluatorOptions<TInput, TOutput>,
): Evaluator {
  return async (session, params) => {
    const scorers = await options.scorers(params);
    const entries = Object.entries(scorers);
    // Validate names before making any judge calls, including empty selections.
    validateEvaluationResults(entries.map(([name]) => ({ name, score: 0 })));
    const input = await options.mapInput(session, params);
    const results = [];
    for (const [name, scorer] of entries) {
      const result = await scorer.run(input);
      results.push({ name, score: result.score, explanation: result.reason });
    }
    return validateEvaluationResults(results);
  };
}
