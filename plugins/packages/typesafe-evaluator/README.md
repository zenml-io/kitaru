# Kitaru TypeSafe evaluator

Judge recorded and imported Kitaru sessions with jev, TypeSafe's hosted model that answers typed questions about a piece of state. You write the questions as evaluator params, the evaluator sends one request per session, and Kitaru stores one evaluation result per question.

## This package sends session content to TypeSafe

Every run of this evaluator sends part of a recorded session to TypeSafe's hosted API: what the user asked, the tool calls with their arguments and results, the final answer, and, on the `full` view, the system prompt and the model messages. Nothing is redacted for you.

That is why this is a separate package. Kitaru does not install it with the server, the server does not register it at startup, and no Kitaru deployment starts talking to TypeSafe until you run the two commands below. The built-in `kitaru-evaluator` package stays offline and free; this one does not.

## Register the evaluator

Save the connection schema that ships in the wheel at `kitaru_typesafe_evaluator/connection-schema.json` to a local file:

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

Store the key on the server, and any worker can run the task:

```bash
kitaru connection create typesafe-prod --evaluator typesafe-judge --default
```

Or keep the key on one worker, and tell that worker to claim tasks that need it:

```bash
export TYPESAFE_API_KEY=...
kitaru worker start --claim evaluator --selector kitaru/requires-credentials=typesafe
```

## Ask a question

Write your questions to a file and keep that file in version control. The question text is the logic:

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
      "instructions": "Does `tool_calls` contain a successful call that performs the action named in `final_answer.action` (issue_refund for refund, create_replacement for replacement, escalate_to_human for escalate)?"
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

Each question becomes its own evaluation result, named after its key, carrying jev's probability as the score and the full params that produced it:

```text
invented_timeline  passed=False  score=0.94  jev-1.13.0 · p(yes)=0.94 · fail: p(no)=0.06 is at or below 0.20
action_executed    passed=True   score=0.99  jev-1.13.0 · p(yes)=0.99 · pass: p(yes)=0.99 is at or above 0.80
```

See the [judge evaluations guide](https://docs.zenml.io/kitaru/guides/judge-evaluations) for the full params reference, the two state views, how to write questions jev answers steadily, what not to ask it, and how to calibrate it against human verdicts.

## Links

- [Kitaru documentation](https://docs.zenml.io/kitaru)
- [Source code](https://github.com/zenml-io/kitaru)
- [Issue tracker](https://github.com/zenml-io/kitaru/issues)

Licensed under Apache-2.0.
