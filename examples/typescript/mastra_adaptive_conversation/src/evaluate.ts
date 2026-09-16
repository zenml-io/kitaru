import { runEvaluator } from "@zenml-io/kitaru/evaluator";
import { createConversationEvaluator } from "./scorers.js";

await runEvaluator(createConversationEvaluator());
