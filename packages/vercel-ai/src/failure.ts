import { isStepCount, type StopCondition, type ToolSet } from "ai";

interface FailureState {
  readonly failure: unknown;
}

export function composeStopConditions<TOOLS extends ToolSet>(
  state: FailureState,
  callerConditions?: StopCondition<TOOLS> | readonly StopCondition<TOOLS>[],
): StopCondition<TOOLS>[] {
  const conditions =
    callerConditions === undefined
      ? [isStepCount(1)]
      : Array.isArray(callerConditions)
        ? [...callerConditions]
        : [callerConditions];
  return [() => state.failure !== undefined, ...conditions];
}
