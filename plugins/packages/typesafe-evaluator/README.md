# Kitaru TypeSafe evaluator

Judge recorded and imported Kitaru sessions with jev, TypeSafe's hosted model that answers typed questions about a piece of state. You write the questions as evaluator params, the evaluator sends one request per session, and Kitaru stores one evaluation result per question.

## This package sends session content to TypeSafe

Every run of this evaluator sends part of a recorded session to TypeSafe's hosted API: what the user asked, the tool calls with their arguments and results, the final answer, and, on the `full` view, the system prompt and the model messages. Nothing is redacted for you.

That is why this is a separate package. Kitaru does not install it with the server, the server does not register it at startup, and no Kitaru deployment starts talking to TypeSafe until you submit an evaluation task and a worker executes it. The built-in `kitaru-evaluator` package stays offline and free; this one does not.

## Register the evaluator

You need a configured Kitaru CLI connected to your workspace, a TypeSafe API key, and a completed recorded or imported session. The [quickstart](https://docs.zenml.io/kitaru/getting-started/quickstart) provides returns-agent sessions matching the example below. Choose a session with `kitaru session list` and set `SESSION_ID` to its ID. Adapt the questions when evaluating another agent.

Save the connection schema that ships in the wheel at `kitaru_typesafe_evaluator/connection-schema.json` as `connection-schema.json` in your working directory:

```json
{
  "description": "TypeSafe connection.",
  "properties": {
    "TYPESAFE_API_KEY": {
      "format": "password",
      "title": "Typesafe Api Key",
      "type": "string",
      "writeOnly": true
    }
  },
  "required": ["TYPESAFE_API_KEY"],
  "title": "TypeSafeConnection",
  "type": "object"
}
```

This file holds no secret. It declares a shape, and the shape is "this plugin needs one secret string called `TYPESAFE_API_KEY`". Registering the evaluator with it tells Kitaru what to ask you for later, nothing more. Never put your key in this file.

You type the key itself at a hidden prompt when you run `kitaru connection create`, and the server keeps it as an encrypted secret, the same way Kitaru's importers and analyzers hold their provider credentials.

Then register the evaluator under a name you choose:

```bash
kitaru evaluator register typesafe-judge \
  --package "kitaru-typesafe-evaluator==0.1.0" \
  --entrypoint kitaru_typesafe_evaluator.judge:judge \
  --provider typesafe \
  --connection-schema connection-schema.json
```

The worker installs the package itself when it claims the first evaluation task.

## Give the evaluator an API key

The evaluator creates a TypeSafe client, which reads `TYPESAFE_API_KEY` from the task's environment. Because you registered the evaluator with a provider and a connection schema, the key has to arrive one of two ways. If you set up neither, the evaluation job stays `pending` and no worker ever claims it.

Store the key on the server, and an evaluator worker can run the task:

```bash
kitaru connection create typesafe-prod --evaluator typesafe-judge --default
```

Ensure an evaluator worker is running. If needed, run `kitaru worker start --claim evaluator` in another terminal.

Or keep the key on one worker, and tell that worker to claim tasks that need it:

```bash
export TYPESAFE_API_KEY=...
kitaru worker start --claim evaluator --selector kitaru/requires-credentials=typesafe
```

Connections are resolved when a job is created. To recover an existing job pending for lack of credentials, start a worker with the key and selector above. Creating a default connection afterward only helps new jobs; submit a new evaluation after creating it.

## Ask a question

Save the following as `questions.json` in your working directory and keep that file in version control. The question text is the logic:

```json
{
  "questions": {
    "invented_timeline": {
      "type": "noul",
      "pass_when": "no",
      "instructions": "Does `final_answer` promise the customer a specific number of days or a date, where that number or date does not appear in any `tool_calls` result?"
    },
    "action_executed": {
      "type": "noul",
      "pass_when": "yes",
      "instructions": "Does `tool_calls` contain a successful call that performs the action named in `final_answer.action` (issue_refund for refund, create_replacement for replacement, escalate_to_human for escalate, decline_request for reject)?"
    }
  }
}
```

```bash
kitaru session evaluate "$SESSION_ID" \
  --evaluator typesafe-judge@latest \
  --evaluator-params "typesafe-judge@latest=$(cat questions.json)" \
  --wait
```

Each question becomes its own evaluation result, named after its key, carrying the probability of yes as the score and the full params that produced it. This is an illustrative summary, not literal CLI output:

```text
invented_timeline  passed=False  score=0.94  jev-1.13.0 · p(yes)=0.94 · fail: p(no)=0.06 is at or below 0.20
action_executed    passed=True   score=0.99  jev-1.13.0 · p(yes)=0.99 · pass: p(yes)=0.99 is at or above 0.80
```

Read stored results with `kitaru evaluation list` and `kitaru evaluation get EVALUATION_ID`. The explanations above are generated threshold calculations, not model reasoning. Task completion does not mean every question passed. Missing session evidence does not automatically produce a held result, and model probabilities are not measured accuracy.

Use these questions for exploration first. Before using them as a gate, validate each failure check against human labels on sessions kept separate from question development, and report incorrect verdicts, held results, operational failures, and coverage.

See the [judge evaluations guide](https://docs.zenml.io/kitaru/guides/judge-evaluations) for the full params reference, the two state views, how to write questions jev answers steadily, what not to ask it, and how to calibrate it against human verdicts.

## Links

- [Kitaru documentation](https://docs.zenml.io/kitaru)
- [Source code](https://github.com/zenml-io/kitaru)
- [Issue tracker](https://github.com/zenml-io/kitaru/issues)

Licensed under Apache-2.0.
