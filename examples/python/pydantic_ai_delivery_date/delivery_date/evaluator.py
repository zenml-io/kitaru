"""Independent, versioned script evaluation of recorded delivery conversations."""

from pathlib import Path

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.api_models.v1.session import SessionStatus
from kitaru.task.evaluator import SessionView

from .dates import extract_explicit_dates

CHECK_NAMES = {"simulation_complete", "no_unsupported_date", "uses_supported_date"}


def evaluate_delivery(view: SessionView) -> list[EvaluationResult]:
    """Calculate date checks from new native assistant messages and frozen evidence."""
    inputs, outputs = view.session.inputs, view.session.outputs
    if not isinstance(inputs, dict) or not isinstance(outputs, dict):
        raise ValueError("Delivery evaluation requires recorded inputs and outputs")
    snapshot = inputs.get("scenario_snapshot")
    if not isinstance(snapshot, dict) or outputs.get("scenario") != snapshot:
        raise ValueError("Result does not match its frozen shipping snapshot")
    shipping = snapshot.get("shipping")
    if not isinstance(shipping, dict):
        raise ValueError("Missing shipping evidence")
    messages = outputs.get("messages")
    seed = inputs.get("input_seed")
    count = outputs.get("seed_message_count")
    if (
        not isinstance(messages, list)
        or not isinstance(seed, list)
        or type(count) is not int
        or count != len(seed)
        or [
            {"kind": message.get("kind"), "parts": message.get("parts")}
            for message in messages[:count]
        ]
        != [
            {"kind": message.get("kind"), "parts": message.get("parts")}
            for message in seed
        ]
    ):
        raise ValueError("Recorded new-message boundary differs from the input seed")
    answers = []
    for message in messages[count:]:
        if not isinstance(message, dict):
            raise ValueError("Malformed recorded native message")
        if message.get("kind") == "response":
            for part in message.get("parts", []):
                if part.get("part_kind") == "text":
                    content = part.get("content")
                    if not isinstance(content, str):
                        raise ValueError("Malformed assistant text")
                    answers.append(content)
    complete = (
        view.session.status == SessionStatus.COMPLETED
        and outputs.get("status") in {"completed", "boundary-completed"}
        and bool(answers)
    )
    dates = extract_explicit_dates(" ".join(answers))
    supported = shipping.get("estimated_delivery")
    checks = {
        "simulation_complete": complete,
        "no_unsupported_date": complete
        and not (dates - ({supported} if supported else set())),
        "uses_supported_date": complete and (supported is None or supported in dates),
    }
    return [
        EvaluationResult(
            name=name,
            score=value,
            passed=value,
            explanation=(
                "Checks new assistant messages against the frozen shipping estimate. "
                "Recognizes ISO and English month-name dates with explicit years."
                if complete
                else "The recorded simulation is incomplete or has no new assistant reply."
            ),
        )
        for name, value in checks.items()
    ]


def get_evaluator_source() -> bytes:
    """Bundle the date parser and evaluator as one portable script plugin."""
    source = (
        Path(__file__)
        .read_text()
        .replace("from .dates import extract_explicit_dates\n", "")
    )
    parser = Path(__file__).with_name("dates.py").read_text()
    return (parser + "\n" + source).encode()
