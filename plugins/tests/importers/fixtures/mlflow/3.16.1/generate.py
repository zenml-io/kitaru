# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "mlflow==3.16.1",
#     "openai==3.19.2",
#     "anthropic==1.8.0",
#     "httpx==0.28.1",
#     "httpx2==2.13.1",
#     "langchain-openai==1.6.6",
#     "langchain-core==1.6.5",
# ]
# ///
"""Record MLflow 3.16.1 autolog traces against stubbed provider HTTP responses.

Writes ``traces.json`` with ``mlflow traces search --output json``. No model
service is called; the OpenAI and Anthropic clients use in-process transports.
"""

import contextlib
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

# MLflow records the OS user as mlflow.user; keep fixture identities synthetic.
os.environ["USER"] = os.environ["LOGNAME"] = "fixture-user"
os.environ["MLFLOW_ENABLE_ASYNC_TRACE_LOGGING"] = "false"
os.environ["MLFLOW_DISABLE_AGENT_HINT"] = "1"

import anthropic
import httpx
import httpx2
import mlflow
import openai
from langchain_openai import ChatOpenAI
from mlflow.entities import AssessmentSource, SpanType

OUT = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent
TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI") or (
    f"sqlite:///{Path(tempfile.mkdtemp()) / 'mlflow.db'}"
)
mlflow.set_tracking_uri(TRACKING_URI)
EXPERIMENT_ID = mlflow.create_experiment(
    "kitaru-fixtures", artifact_location="mlflow-artifacts:/kitaru-fixtures"
)
mlflow.set_experiment(experiment_id=EXPERIMENT_ID)

_openai_calls = {"n": 0}


def openai_handler(request: httpx.Request) -> httpx.Response:
    _openai_calls["n"] += 1
    body = json.loads(request.content)
    has_tool_result = any(m.get("role") == "tool" for m in body["messages"])
    if body.get("tools") and not has_tool_result:
        message = {
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {
                    "id": "call_weather_1",
                    "type": "function",
                    "function": {
                        "name": "get_weather",
                        "arguments": '{"city": "Delft"}',
                    },
                }
            ],
        }
        finish = "tool_calls"
    else:
        message = {"role": "assistant", "content": "It is 14C and cloudy in Delft."}
        finish = "stop"
    return httpx.Response(
        200,
        json={
            "id": f"chatcmpl-{_openai_calls['n']}",
            "object": "chat.completion",
            "created": 1790000000,
            "model": "gpt-4o-mini-2024-07-18",
            "choices": [{"index": 0, "message": message, "finish_reason": finish}],
            "usage": {
                "prompt_tokens": 42,
                "completion_tokens": 11,
                "total_tokens": 53,
                "prompt_tokens_details": {"cached_tokens": 8},
                "completion_tokens_details": {"reasoning_tokens": 0},
            },
        },
    )


def anthropic_handler(request: httpx2.Request) -> httpx2.Response:
    return httpx2.Response(
        200,
        json={
            "id": "msg_01",
            "type": "message",
            "role": "assistant",
            "model": "claude-sonnet-5-5",
            "content": [{"type": "text", "text": "Refunds take 5 business days."}],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 25, "output_tokens": 9},
        },
    )


oai = openai.OpenAI(
    api_key="test",
    http_client=httpx.Client(transport=httpx.MockTransport(openai_handler)),
)
ant = anthropic.Anthropic(
    api_key="test",
    http_client=httpx2.Client(transport=httpx2.MockTransport(anthropic_handler)),
)

mlflow.openai.autolog()
mlflow.anthropic.autolog()

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_weather",
            "parameters": {
                "type": "object",
                "properties": {"city": {"type": "string"}},
            },
        },
    }
]


@mlflow.trace(span_type=SpanType.TOOL)
def get_weather(city: str) -> dict:
    return {"city": city, "temp_c": 14, "sky": "cloudy"}


@mlflow.trace(span_type=SpanType.TOOL)
def lookup_order(order_id: str) -> dict:
    raise RuntimeError(f"Order {order_id} not found")


@mlflow.trace(name="weather_agent", span_type=SpanType.AGENT)
def weather_agent(question: str, session: str) -> str:
    mlflow.update_current_trace(
        session_id=session, user="user-42", tags={"env": "test"}
    )
    messages = [
        {"role": "system", "content": "You are a weather assistant."},
        {"role": "user", "content": question},
    ]
    first = oai.chat.completions.create(
        model="gpt-4o-mini", messages=messages, tools=TOOLS
    )
    call = first.choices[0].message.tool_calls[0]
    result = get_weather(**json.loads(call.function.arguments))
    messages += [
        first.choices[0].message.model_dump(exclude_none=True),
        {"role": "tool", "tool_call_id": call.id, "content": json.dumps(result)},
    ]
    second = oai.chat.completions.create(
        model="gpt-4o-mini", messages=messages, tools=TOOLS
    )
    return second.choices[0].message.content


@mlflow.trace(name="refund_agent", span_type=SpanType.AGENT)
def refund_agent(question: str) -> str:
    reply = ant.messages.create(
        model="claude-sonnet-5-5",
        max_tokens=256,
        system="You answer refund questions.",
        messages=[{"role": "user", "content": question}],
    )
    return reply.content[0].text


@mlflow.trace(name="order_agent", span_type=SpanType.AGENT)
def order_agent(order_id: str) -> str:
    mlflow.update_current_trace(session_id="session-orders")
    return lookup_order(order_id)


def langchain_chain(question: str) -> str:
    mlflow.langchain.autolog()
    llm = ChatOpenAI(
        model="gpt-4o-mini",
        api_key="test",
        http_client=httpx.Client(transport=httpx.MockTransport(openai_handler)),
    )
    return llm.invoke(question).content


weather_agent("What's the weather in Delft?", "session-weather")
weather_agent("And tomorrow?", "session-weather")
t2 = mlflow.get_last_active_trace_id()
mlflow.log_feedback(
    trace_id=t2,
    name="helpfulness",
    value=True,
    rationale="Answered the question",
    source=AssessmentSource(source_type="HUMAN", source_id="reviewer@example.com"),
)
refund_agent("How long do refunds take?")
with contextlib.suppress(RuntimeError):
    order_agent("A-1001")
langchain_chain("Say hi")

export = subprocess.run(
    [
        sys.executable,
        *("-m", "mlflow", "traces", "search"),
        *("--experiment-id", EXPERIMENT_ID, "--output", "json"),
    ],
    env={**os.environ, "MLFLOW_TRACKING_URI": TRACKING_URI},
    capture_output=True,
    text=True,
    check=True,
)
# Exception stack traces name local paths; replace them with placeholders.
text = json.dumps(json.loads(export.stdout), indent=2)
for local, placeholder in (
    (str(Path(__file__).resolve().parent), "<fixture-dir>"),
    (sys.prefix, "<python-env>"),
    (str(Path.home()), "<home>"),
):
    text = text.replace(local.replace("\\", "\\\\"), placeholder)
(OUT / "traces.json").write_text(text + "\n")
